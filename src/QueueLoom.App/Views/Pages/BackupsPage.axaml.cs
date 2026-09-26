using Avalonia.Controls;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views.Pages;

public sealed partial class BackupsPage : UserControl
{
    public BackupsPage()
    {
        InitializeComponent();
    }

    private void OnBackupSelectionChanged(object? sender, SelectionChangedEventArgs args)
    {
        if (DataContext is MainWindowViewModel { LoadSelectedBackupCommand: var load } && load.CanExecute(null))
        {
            load.Execute(null);
        }
    }
}
