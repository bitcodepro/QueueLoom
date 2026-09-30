using Avalonia.Controls;
using Avalonia.Interactivity;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views;

public sealed partial class ResendDialogWindow : Window
{
    private readonly ResendDialogViewModel? _viewModel;

    public ResendDialogWindow()
    {
        InitializeComponent();
    }

    public ResendDialogWindow(ResendDialogViewModel viewModel)
    {
        _viewModel = viewModel;
        DataContext = viewModel;
        InitializeComponent();
    }

    private void ConfirmClick(object? sender, RoutedEventArgs args) => Close(_viewModel?.ToOptions());

    private void CancelClick(object? sender, RoutedEventArgs args) => Close(null);
}
