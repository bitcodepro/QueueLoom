using System.ComponentModel;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.VisualTree;
using Microsoft.Extensions.Logging;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.App;

public sealed partial class MainWindow : Window
{
    private readonly MainWindowViewModel? _viewModel;
    private readonly JsonAppSettingsStore? _settingsStore;
    private readonly WindowDialogService? _dialogService;
    private readonly GitHubUpdateChecker? _updateChecker;
    private readonly AppUpdater? _updater;
    private readonly IAppLauncher? _launcher;
    private readonly IThemeService? _theme;
    private readonly ILogger<MainWindow>? _logger;
    private bool _initialized;
    private bool _shutdownInProgress;
    private bool _shutdownComplete;
    private Task? _initializationTask;

    /// <summary>Design-time constructor used by the XAML previewer.</summary>
    public MainWindow()
    {
        InitializeComponent();
    }

    public MainWindow(
        MainWindowViewModel viewModel,
        JsonAppSettingsStore settingsStore,
        TopLevelAccessor topLevel,
        WindowDialogService dialogService,
        GitHubUpdateChecker updateChecker,
        IAppLauncher launcher,
        IThemeService theme,
        ILogger<MainWindow> logger,
        AppUpdater? updater = null)
        : this()
    {
        _updater = updater;
        _viewModel = viewModel;
        _settingsStore = settingsStore;
        _dialogService = dialogService;
        _updateChecker = updateChecker;
        _launcher = launcher;
        _theme = theme;
        _logger = logger;
        topLevel.Current = this;

        DataContext = _viewModel;
        Opened += OnOpened;
        Closing += OnClosing;
    }

    /// <summary>Invoked once after the view model has shut down, to release application services.</summary>
    public Func<ValueTask>? ShutdownCompleted { get; set; }

    private async void OnOpened(object? sender, EventArgs args)
    {
        if (_initialized || _viewModel is null || _settingsStore is null)
        {
            return;
        }

        _initialized = true;
        try
        {
            // Assigned before the first await so a close during startup waits for it.
            _initializationTask = InitializeAsync(_viewModel, _settingsStore);
            await _initializationTask;
            _viewModel.PropertyChanged += OnTrayRelevantPropertyChanged;
            UpdateTray();
        }
        catch (Exception exception)
        {
            _logger?.LogError(exception, "Startup initialization failed");
            if (!_shutdownInProgress)
            {
                await ShowStartupErrorAsync(exception);
            }
            return;
        }

        if (!UpdateRestart.AcknowledgeStartup()) await CheckForUpdatesAsync();
    }

    private async Task InitializeAsync(MainWindowViewModel viewModel, JsonAppSettingsStore settingsStore)
    {
        var settings = await settingsStore.LoadAsync();
        _theme?.Apply(settings.Theme);
        viewModel.ApplyPreferences(settings);
        // From here on every preference change is saved when it is made, including one made while environments are
        // still loading: closing no longer writes a final value, so a change that is not saved now would be lost.
        viewModel.PropertyChanged += OnViewModelPropertyChanged;
        await viewModel.InitializeAsync();
        viewModel.StartScheduledResends();
    }

    private async Task ShowStartupErrorAsync(Exception exception)
    {
        try
        {
            if (_dialogService is not null)
            {
                await _dialogService.ShowMessageAsync(
                    "QueueLoom could not load local data",
                    $"{SensitiveDataRedactor.SummarizeException(exception)}\n\nDetails were written to the log folder.",
                    isError: true);
            }
        }
        catch (Exception dialogException)
        {
            _logger?.LogError(dialogException, "Startup error dialog could not be shown");
        }
    }

    private void OnViewModelPropertyChanged(object? sender, PropertyChangedEventArgs args)
    {
        if (_viewModel is null || _settingsStore is null)
        {
            return;
        }

        switch (args.PropertyName)
        {
            case nameof(MainWindowViewModel.MonitorIntervalSeconds):
                var interval = _viewModel.MonitorIntervalSeconds;
                TrackPreferenceSave(() => _settingsStore.SaveMonitorIntervalSecondsAsync(interval));
                break;
            case nameof(MainWindowViewModel.ThemePreference):
                var theme = _viewModel.ThemePreference;
                TrackPreferenceSave(() => _settingsStore.SaveThemeAsync(theme));
                break;
            case nameof(MainWindowViewModel.KeepInTray):
                var keepInTray = _viewModel.KeepInTray;
                TrackPreferenceSave(() =>
                    _settingsStore.UpdateAsync(settings => settings with { KeepInTray = keepInTray }));
                break;
            case nameof(MainWindowViewModel.SystemNotifications):
                var system = _viewModel.SystemNotifications;
                TrackPreferenceSave(() =>
                    _settingsStore.UpdateAsync(settings => settings with { SystemNotifications = system }));
                break;
            case nameof(MainWindowViewModel.AlertWebhookUrl) when !_viewModel.HasAlertWebhookError:
                var webhook = string.IsNullOrWhiteSpace(_viewModel.AlertWebhookUrl) ? null : _viewModel.AlertWebhookUrl.Trim();
                TrackPreferenceSave(() =>
                    _settingsStore.UpdateAsync(settings => settings with { AlertWebhookUrl = webhook }));
                break;
            case nameof(MainWindowViewModel.ProtobufSchemaPath):
                var protobuf = string.IsNullOrWhiteSpace(_viewModel.ProtobufSchemaPath) ? null : _viewModel.ProtobufSchemaPath;
                TrackPreferenceSave(() =>
                    _settingsStore.UpdateAsync(settings => settings with { ProtobufSchemaPath = protobuf }));
                break;
            case nameof(MainWindowViewModel.BackupRetentionDays):
                var retention = _viewModel.BackupRetentionDays;
                TrackPreferenceSave(() =>
                    _settingsStore.UpdateAsync(settings => settings with { BackupRetentionDays = retention }));
                break;
            case nameof(MainWindowViewModel.SavedSearches):
                var searches = _viewModel.CaptureSavedSearchChanges();
                TrackPreferenceSave(() => _viewModel.PersistSavedSearchesAsync(searches, merge =>
                    _settingsStore.UpdateAsync(settings => settings with { SavedSearches = merge(settings.SavedSearches) })));
                break;
        }
    }

    private readonly List<Task> _preferenceSaves = [];

    /// <summary>Saves a preference in the background, remembered so closing can wait for it.</summary>
    private void TrackPreferenceSave(Func<Task> save)
    {
        var task = SavePreferenceBestEffortAsync(save);
        lock (_preferenceSaves)
        {
            _preferenceSaves.Add(task);
        }
        _ = task.ContinueWith(done =>
        {
            lock (_preferenceSaves)
            {
                _preferenceSaves.Remove(done);
            }
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private Task PendingPreferenceSaves()
    {
        lock (_preferenceSaves)
        {
            return Task.WhenAll(_preferenceSaves.ToArray());
        }
    }

    private async Task SavePreferenceBestEffortAsync(Func<Task> save)
    {
        try
        {
            await save();
        }
        catch (Exception exception)
        {
            // Local preference persistence must not interrupt Service Bus operations.
            _logger?.LogWarning(exception, "Saving a local preference failed");
        }
    }

    private async Task CheckForUpdatesAsync()
    {
        if (_updateChecker is null || _dialogService is null)
        {
            return;
        }

        try
        {
            _updater?.CleanUpPreviousUpdate(AppUpdater.CurrentTarget());
            var update = await _updateChecker.CheckAsync();
            if (update is null || _shutdownInProgress)
            {
                return;
            }

            var target = AppUpdater.CurrentTarget();
            var reason = target is null
                ? "This is a development build, which is not updated in place."
                : AppUpdater.CanInstall(target)
                    ? null
                    : $"QueueLoom cannot write to its folder ({target.InstallDirectory}).";
            var updater = _updater;
            Func<IProgress<UpdateProgress>, CancellationToken, Task>? install = updater is not null && target is not null && reason is null
                ? async (progress, token) =>
                {
                    var staging = await updater.DownloadAsync(update, target, progress, token);
                    await AppUpdater.InstallWithProgressAsync(target, staging, progress, token);
                    _logger?.LogInformation("Installed QueueLoom {Version}", update.Version);
                }
                : null;
            var dialog = new UpdateDialogViewModel(GitHubUpdateChecker.CurrentVersionText, update, install, reason,
                _viewModel?.ExportDiagnosticsCommand, _viewModel?.Diagnostics);
            if (await _dialogService.ShowUpdateAsync(dialog, _launcher) == UpdateDialogResult.Restart && target is not null)
            {
                try { await Task.Run(() => AppUpdater.StartInstalled(target)); }
                catch (Exception exception)
                {
                    _logger?.LogWarning(exception, "Update restart handoff failed");
                    await _dialogService.ShowMessageAsync("QueueLoom could not restart", SensitiveDataRedactor.SummarizeException(exception), isError: true);
                    return;
                }
                _quitRequested = true;
                Close();
            }
        }
        catch (Exception exception)
        {
            // Update checks must never prevent QueueLoom from starting or operating offline.
            _logger?.LogInformation(exception, "Update check skipped");
        }
    }

    protected override void OnKeyDown(KeyEventArgs args)
    {
        base.OnKeyDown(args);
        if (args.Handled || args.Key != Key.F || args.KeyModifiers != KeyModifiers.Control)
        {
            return;
        }

        // Ctrl+F focuses the current page's search or filter box.
        var search = Pages.Content is Control page
            ? page.GetVisualDescendants().OfType<TextBox>().FirstOrDefault(box => box.Classes.Contains("search") && box.IsEffectivelyVisible)
            : null;
        if (search is not null)
        {
            search.Focus();
            search.SelectAll();
            args.Handled = true;
        }
    }

    private async void OnClosing(object? sender, WindowClosingEventArgs args)
    {
        if (_shutdownComplete || _viewModel is null)
        {
            return;
        }

        args.Cancel = true;
        if (_shutdownInProgress)
        {
            return;
        }
        // Closing by the operator hides to the tray when that is on; the operating system shutting down still quits.
        if (args.CloseReason is WindowCloseReason.WindowClosing && HideToTrayInsteadOfClosing())
        {
            return;
        }
        _shutdownInProgress = true;

        try
        {
            if (_initializationTask is not null)
            {
                try
                {
                    await _initializationTask;
                }
                catch (Exception exception)
                {
                    // Already reported by OnOpened; shutdown still has to release resources.
                    _logger?.LogDebug(exception, "Startup had failed before shutdown");
                }
            }
            // Saved-search saves already admitted are written before the final settings write and before the settings
            // store is released, so a queued reinsert cannot be overtaken or abandoned (bounded like the rest of closing).
            await ShutdownWait.WithinAsync(() => new ValueTask(_viewModel.DrainSavedSearchSavesAsync()),
                _viewModel.ShutdownDrainTimeout, _viewModel.Clock, _logger);
            // Every change is saved when it is made; closing only waits for saves still under way. Writing this window's
            // value again here would overwrite a newer value another window saved since this one loaded its settings.
            await ShutdownWait.WithinAsync(() => new ValueTask(PendingPreferenceSaves()),
                _viewModel.ShutdownDrainTimeout, _viewModel.Clock, _logger);
            await _viewModel.DisposeAsync();
        }
        catch (Exception exception)
        {
            // The operating system is already closing the application. Avoid an
            // unhandled async-void exception if an SDK resource fails to dispose.
            _logger?.LogWarning(exception, "Shutdown did not complete cleanly");
        }
        finally
        {
            _viewModel.PropertyChanged -= OnViewModelPropertyChanged;
            _viewModel.PropertyChanged -= OnTrayRelevantPropertyChanged;
            RemoveTray();
            Opened -= OnOpened;
            if (ShutdownCompleted is { } shutdownCompleted)
            {
                // Disposing the services waits for the workspace's running operations; a stuck one must not keep
                // the window open, so this wait is bounded like the view model's.
                await ShutdownWait.WithinAsync(shutdownCompleted, _viewModel.ShutdownDrainTimeout, _viewModel.Clock, _logger);
            }
            _shutdownComplete = true;
            _shutdownInProgress = false;
            Close();
        }
    }
}
