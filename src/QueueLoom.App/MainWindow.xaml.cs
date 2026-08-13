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
        await CheckForUpdatesAsync();
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

    private async Task CopyEntityNameAsync(EntityItemViewModel entity)
    {
        var name = entity.Name;
        try
        {
            var clipboard = TopLevel.GetTopLevel(this)?.Clipboard;
            if (clipboard is null)
            {
                ExplorerCopyStatus.Text = "Clipboard is unavailable; the entity name was not copied.";
                return;
            }

            await clipboard.SetTextAsync(name);
            ExplorerCopyStatus.Text = $"Copied {entity.KindLabel.ToLowerInvariant()} name: {name}";
        }
        catch
        {
            ExplorerCopyStatus.Text = "Could not copy the entity name to the clipboard.";
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
            Opened -= OnOpened;
            _shutdownComplete = true;
            _shutdownInProgress = false;
            Close();
        }
    }
}
