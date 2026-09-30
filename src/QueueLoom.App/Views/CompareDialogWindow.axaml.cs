using Avalonia.Controls;
using Avalonia.Interactivity;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views;

public sealed partial class CompareDialogWindow : Window
{
    public CompareDialogWindow()
    {
        InitializeComponent();
    }

    public CompareDialogWindow(CompareDialogViewModel viewModel)
    {
        DataContext = viewModel;
        InitializeComponent();
    }

    private void CloseClick(object? sender, RoutedEventArgs args) => Close(null);
}
