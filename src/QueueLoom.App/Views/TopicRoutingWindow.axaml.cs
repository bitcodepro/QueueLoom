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
        Opened += async (_, _) => await viewModel.LoadAsync();
    }

    private void CloseClick(object? sender, RoutedEventArgs args) => Close(null);
}
