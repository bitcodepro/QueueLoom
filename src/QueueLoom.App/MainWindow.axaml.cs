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
        ILogger<MainWindow> logger)
        : this()
    {
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
            var settings = await _settingsStore.LoadAsync();
            _theme?.Apply(settings.Theme);
            _viewModel.ApplyPreferences(settings);
            _initializationTask = _viewModel.InitializeAsync();
            await _initializationTask;
            _viewModel.PropertyChanged += OnViewModelPropertyChanged;
        }
        catch (Exception exception)
        {
            _logger?.LogError(exception, "Startup initialization failed");
            await ShowStartupErrorAsync(exception);
            return;
        }

        await CheckForUpdatesAsync();
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
                _ = SavePreferenceBestEffortAsync(() => _settingsStore.SaveMonitorIntervalSecondsAsync(interval));
                break;
            case nameof(MainWindowViewModel.ThemePreference):
                var theme = _viewModel.ThemePreference;
                _ = SavePreferenceBestEffortAsync(() => _settingsStore.SaveThemeAsync(theme));
                break;
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
            var update = await _updateChecker.CheckAsync();
            if (update is null || _shutdownInProgress)
            {
                return;
            }

            if (await _dialogService.PromptForUpdateAsync(update.Version.ToString(3)) && _launcher is not null)
            {
                await _launcher.OpenUriAsync(update.ReleasePage);
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
        _shutdownInProgress = true;

        try
        {
            if (_initializationTask is not null)
            {
                await _initializationTask;
            }
            if (_settingsStore is not null)
            {
                await _settingsStore.SaveMonitorIntervalSecondsAsync(_viewModel.MonitorIntervalSeconds);
            }
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
            Opened -= OnOpened;
            try
            {
                if (ShutdownCompleted is { } shutdownCompleted)
                {
                    await shutdownCompleted();
                }
            }
            catch (Exception exception)
            {
                _logger?.LogWarning(exception, "Releasing application services failed");
            }
            _shutdownComplete = true;
            _shutdownInProgress = false;
            Close();
        }
    }
}
