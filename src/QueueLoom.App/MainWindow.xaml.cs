using System.ComponentModel;
using System.Diagnostics;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Input.Platform;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;

namespace QueueLoom.App;

public sealed partial class MainWindow : Window
{
    private readonly JsonProfileRepository _profileRepository;
    private readonly EncryptedFileSecretVault _secretVault;
    private readonly MainWindowViewModel _viewModel;
    private readonly WindowDialogService _dialogService;
    private readonly JsonAppSettingsStore _settingsStore;
    private bool _initialized;
    private bool _shutdownInProgress;
    private bool _shutdownComplete;
    private Task? _initializationTask;

    public MainWindow()
    {
        InitializeComponent();

        var paths = QueueLoomPaths.CreateDefault();
        _profileRepository = new JsonProfileRepository(paths);
        _secretVault = new EncryptedFileSecretVault(paths);
        _settingsStore = new JsonAppSettingsStore(paths);
        var workspace = new AzureServiceBusWorkspace(
            _secretVault,
            backupStore: new DeadLetterJsonBackupStore(paths));
        _dialogService = new WindowDialogService(this);
        _viewModel = new MainWindowViewModel(
            _profileRepository,
            _secretVault,
            workspace,
            _dialogService);

        DataContext = _viewModel;
        Opened += OnOpened;
        Closing += OnClosing;
    }

    private async void OnOpened(object? sender, EventArgs args)
    {
        if (_initialized)
        {
            return;
        }

        _initialized = true;
        _initializationTask = _viewModel.InitializeAsync();
        await _initializationTask;
        _viewModel.MonitorIntervalSeconds = await _settingsStore.LoadMonitorIntervalSecondsAsync();
        _viewModel.PropertyChanged += OnViewModelPropertyChanged;
        await CheckForUpdatesAsync();
    }

    private void OnViewModelPropertyChanged(object? sender, PropertyChangedEventArgs args)
    {
        if (args.PropertyName == nameof(MainWindowViewModel.MonitorIntervalSeconds))
        {
            _ = SaveMonitorIntervalBestEffortAsync();
        }
    }

    private async Task SaveMonitorIntervalBestEffortAsync()
    {
        try
        {
            await _settingsStore.SaveMonitorIntervalSecondsAsync(_viewModel.MonitorIntervalSeconds);
        }
        catch
        {
            // Local preference persistence must not interrupt Service Bus operations.
        }
    }

    private async Task CheckForUpdatesAsync()
    {
        try
        {
            using var checker = new GitHubUpdateChecker();
            var update = await checker.CheckAsync();
            if (update is null || _shutdownInProgress)
            {
                return;
            }

            if (await _dialogService.PromptForUpdateAsync(update.Version.ToString(3)))
            {
                Process.Start(new ProcessStartInfo(update.ReleasePage.AbsoluteUri)
                {
                    UseShellExecute = true
                });
            }
        }
        catch
        {
            // Update checks must never prevent QueueLoom from starting or operating offline.
        }
    }

    private async void OnEntityNameDoubleTapped(object? sender, TappedEventArgs args)
    {
        args.Handled = true;
        if (sender is not Control { DataContext: EntityItemViewModel entity })
        {
            return;
        }

        await CopyEntityNameAsync(entity);
    }

    private async void OnCopyEntityNameClick(object? sender, Avalonia.Interactivity.RoutedEventArgs args)
    {
        args.Handled = true;
        if (sender is not Control { DataContext: EntityItemViewModel entity })
        {
            return;
        }

        await CopyEntityNameAsync(entity);
    }

    private async void OnCopyExplorerParentTopicNameClick(
        object? sender,
        Avalonia.Interactivity.RoutedEventArgs args)
    {
        args.Handled = true;
        if (sender is not Control { DataContext: EntityItemViewModel { IsSubscription: true } entity })
        {
            return;
        }

        await CopyExplorerTextAsync(entity.ParentPath, "parent topic");
    }

    private async Task CopyEntityNameAsync(EntityItemViewModel entity)
    {
        await CopyExplorerTextAsync(entity.Name, entity.KindLabel.ToLowerInvariant());
    }

    private async Task CopyExplorerTextAsync(string name, string kind)
    {
        try
        {
            var clipboard = TopLevel.GetTopLevel(this)?.Clipboard;
            if (clipboard is null)
            {
                ExplorerCopyStatus.Text = "Clipboard is unavailable; the entity name was not copied.";
                return;
            }

            await clipboard.SetTextAsync(name);
            ExplorerCopyStatus.Text = $"Copied {kind} name: {name}";
        }
        catch
        {
            ExplorerCopyStatus.Text = "Could not copy the entity name to the clipboard.";
        }
    }

    private async void OnCopyDlqEntityNameClick(object? sender, Avalonia.Interactivity.RoutedEventArgs args)
    {
        args.Handled = true;
        if (sender is not Control { DataContext: DlqSourceItemViewModel source })
        {
            return;
        }

        await CopyDlqNameAsync(
            source.EntityName,
            source.IsQueue ? "queue" : "subscription");
    }

    private async void OnCopyDlqTopicNameClick(object? sender, Avalonia.Interactivity.RoutedEventArgs args)
    {
        args.Handled = true;
        if (sender is not Control { DataContext: DlqSourceItemViewModel source } ||
            !source.IsSubscription)
        {
            return;
        }

        await CopyDlqNameAsync(source.ParentTopicName, "topic");
    }

    private async Task CopyDlqNameAsync(string name, string kind)
    {
        try
        {
            var clipboard = TopLevel.GetTopLevel(this)?.Clipboard;
            if (clipboard is null)
            {
                DeadLetterCopyStatus.Text = "Clipboard is unavailable; the name was not copied.";
                return;
            }

            await clipboard.SetTextAsync(name);
            DeadLetterCopyStatus.Text = $"Copied {kind} name: {name}";
        }
        catch
        {
            DeadLetterCopyStatus.Text = "Could not copy the source name to the clipboard.";
        }
    }

    private async void OnCopyMonitorEntityNameClick(object? sender, Avalonia.Interactivity.RoutedEventArgs args)
    {
        args.Handled = true;
        if (sender is not Control { DataContext: MonitorNotificationItemViewModel notification })
        {
            return;
        }

        await CopyMonitorNameAsync(
            notification.EntityName,
            notification.IsQueue ? "queue" : "subscription");
    }

    private async void OnCopyMonitorTopicNameClick(object? sender, Avalonia.Interactivity.RoutedEventArgs args)
    {
        args.Handled = true;
        if (sender is not Control { DataContext: MonitorNotificationItemViewModel { IsSubscription: true } notification })
        {
            return;
        }

        await CopyMonitorNameAsync(notification.ParentTopicName, "topic");
    }

    private async Task CopyMonitorNameAsync(string name, string kind)
    {
        try
        {
            var clipboard = TopLevel.GetTopLevel(this)?.Clipboard;
            if (clipboard is null)
            {
                MonitorCopyStatus.Text = "Clipboard is unavailable; the name was not copied.";
                return;
            }

            await clipboard.SetTextAsync(name);
            MonitorCopyStatus.Text = $"Copied {kind} name: {name}";
        }
        catch
        {
            MonitorCopyStatus.Text = "Could not copy the notification source name.";
        }
    }

    private async void OnCopyActivityDetailsClick(object? sender, Avalonia.Interactivity.RoutedEventArgs args)
    {
        args.Handled = true;
        if (sender is not Control { DataContext: ActivityItemViewModel activity })
        {
            return;
        }

        try
        {
            var clipboard = TopLevel.GetTopLevel(this)?.Clipboard;
            if (clipboard is null)
            {
                ActivityCopyStatus.Text = "Clipboard is unavailable; the details were not copied.";
                return;
            }

            await clipboard.SetTextAsync(activity.Details);
            ActivityCopyStatus.Text = "Activity details copied.";
        }
        catch
        {
            ActivityCopyStatus.Text = "Could not copy the activity details.";
        }
    }

    private async void OnCopyActivityEntityNameClick(object? sender, Avalonia.Interactivity.RoutedEventArgs args)
    {
        args.Handled = true;
        if (sender is not Control { DataContext: ActivityItemViewModel { HasSource: true } activity })
        {
            return;
        }

        var kind = activity.IsQueue ? "queue" : activity.IsTopic ? "topic" : "subscription";
        await CopyActivityNameAsync(activity.EntityName, kind);
    }

    private async void OnCopyActivityTopicNameClick(object? sender, Avalonia.Interactivity.RoutedEventArgs args)
    {
        args.Handled = true;
        if (sender is not Control { DataContext: ActivityItemViewModel { IsSubscription: true } activity })
        {
            return;
        }

        await CopyActivityNameAsync(activity.ParentTopicName, "parent topic");
    }

    private async Task CopyActivityNameAsync(string name, string kind)
    {
        try
        {
            var clipboard = TopLevel.GetTopLevel(this)?.Clipboard;
            if (clipboard is null)
            {
                ActivityCopyStatus.Text = "Clipboard is unavailable; the name was not copied.";
                return;
            }

            await clipboard.SetTextAsync(name);
            ActivityCopyStatus.Text = $"Copied {kind} name: {name}";
        }
        catch
        {
            ActivityCopyStatus.Text = "Could not copy the activity source name.";
        }
    }

    private async void OnClosing(object? sender, WindowClosingEventArgs args)
    {
        if (_shutdownComplete)
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
            await _settingsStore.SaveMonitorIntervalSecondsAsync(_viewModel.MonitorIntervalSeconds);
            await _viewModel.DisposeAsync();
        }
        catch
        {
            // The operating system is already closing the application. Avoid an
            // unhandled async-void exception if an SDK resource fails to dispose.
        }
        finally
        {
            _secretVault.Dispose();
            _profileRepository.Dispose();
            _settingsStore.Dispose();
            _viewModel.PropertyChanged -= OnViewModelPropertyChanged;
            Opened -= OnOpened;
            _shutdownComplete = true;
            _shutdownInProgress = false;
            Close();
        }
    }
}
