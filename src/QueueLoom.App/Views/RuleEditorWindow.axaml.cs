using Avalonia.Controls;
using Avalonia.Interactivity;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views;

public sealed partial class RuleEditorWindow : Window
{
    private readonly RuleEditorViewModel? _viewModel;

    public RuleEditorWindow()
    {
        InitializeComponent();
    }

    public RuleEditorWindow(RuleEditorViewModel viewModel)
    {
        _viewModel = viewModel;
        DataContext = viewModel;
        InitializeComponent();
    }

    /// <summary>Closes with the <see cref="Core.Routing.SubscriptionRule"/> to save, or stays open to show what is wrong.</summary>
    private void ConfirmClick(object? sender, RoutedEventArgs args)
    {
        if (_viewModel?.TryBuild() is { } rule)
        {
            Close(rule);
        }
    }

    private void CancelClick(object? sender, RoutedEventArgs args) => Close(null);
}
