using Avalonia.Controls;
using Avalonia.Interactivity;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views;

public sealed partial class QueueDialogWindow : Window
{
    private readonly QueueDialogViewModel? _viewModel;

    public QueueDialogWindow()
    {
        InitializeComponent();
    }

    public QueueDialogWindow(QueueDialogViewModel viewModel)
    {
        _viewModel = viewModel;
        DataContext = viewModel;
        InitializeComponent();
    }

    /// <summary>Closes with a <see cref="Core.ServiceBus.QueueDefinition"/> for a new queue or <see cref="Core.ServiceBus.QueueSettings"/> for an existing one.</summary>
    private void ConfirmClick(object? sender, RoutedEventArgs args)
    {
        object? result = _viewModel?.IsNew == true ? _viewModel.TryBuildDefinition() : _viewModel?.TryBuildSettings();
        if (result is not null)
        {
            Close(result);
        }
    }

    private void CancelClick(object? sender, RoutedEventArgs args) => Close(null);
}
