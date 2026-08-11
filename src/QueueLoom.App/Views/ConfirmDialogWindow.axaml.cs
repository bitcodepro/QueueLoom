using Avalonia.Controls;
using Avalonia.Interactivity;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views;

public sealed partial class ConfirmDialogWindow : Window
{
    public ConfirmDialogWindow()
        : this(new ConfirmDialogViewModel("QueueLoom", string.Empty, false, null))
    {
    }

    public ConfirmDialogWindow(object viewModel)
    {
        DataContext = viewModel;
        InitializeComponent();
    }

    private void ConfirmClick(object? sender, RoutedEventArgs args) => Close(true);

    private void CancelClick(object? sender, RoutedEventArgs args) => Close(false);
}
