using Avalonia.Controls;
using Avalonia.Interactivity;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views;

public sealed partial class TopicRoutingWindow : Window
{
    public TopicRoutingWindow()
    {
        InitializeComponent();
    }

    public TopicRoutingWindow(TopicRoutingViewModel viewModel)
    {
        DataContext = viewModel;
        InitializeComponent();
        Opened += async (_, _) =>
        {
            try { await viewModel.LoadAsync(); }
            catch (OperationCanceledException) when (!IsVisible) { }
        };
        Closed += async (_, _) =>
        {
            try { await viewModel.DisposeAsync(); }
            catch (OperationCanceledException) { }
        };
    }

    private void CloseClick(object? sender, RoutedEventArgs args) => Close(null);
}
