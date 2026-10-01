using Avalonia.Controls;
using Avalonia.Interactivity;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views;

public sealed partial class UpdateDialogWindow : Window
{
    private readonly UpdateDialogViewModel? _viewModel;
    private readonly IAppLauncher? _launcher;

    public UpdateDialogWindow()
    {
        InitializeComponent();
    }

    public UpdateDialogWindow(UpdateDialogViewModel viewModel, IAppLauncher? launcher)
    {
        _viewModel = viewModel;
        _launcher = launcher;
        DataContext = viewModel;
        InitializeComponent();
        Closing += (_, _) => _viewModel.CancelDownload();
    }

    private async void InstallClick(object? sender, RoutedEventArgs args)
    {
        if (_viewModel is not null)
        {
            await _viewModel.InstallAsync();
            if (_viewModel.IsReady) Close(UpdateDialogResult.Restart);
        }
    }

    private void CancelClick(object? sender, RoutedEventArgs args) => _viewModel?.CancelDownload();

    private async void ReleaseNotesClick(object? sender, RoutedEventArgs args)
    {
        if (_viewModel is not null && _launcher is not null)
        {
            await _launcher.OpenUriAsync(_viewModel.Update.ReleasePage);
        }
    }

    private void LaterClick(object? sender, RoutedEventArgs args) => Close(UpdateDialogResult.Later);

    private void RestartClick(object? sender, RoutedEventArgs args) => Close(UpdateDialogResult.Restart);
}
