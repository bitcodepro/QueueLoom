using Avalonia.Controls;
using Avalonia.Input;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views.Pages;

public sealed partial class ExplorerPage : UserControl
{
    public ExplorerPage()
    {
        InitializeComponent();
    }

    private void OnEntityNameDoubleTapped(object? sender, TappedEventArgs args)
    {
        args.Handled = true;
        if (sender is Control { DataContext: EntityItemViewModel entity } &&
            DataContext is MainWindowViewModel viewModel)
        {
            viewModel.CopyTextCommand.Execute(entity.Name);
        }
    }
}
