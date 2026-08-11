using Avalonia.Controls;
using Avalonia.Interactivity;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views;

public sealed partial class ProfileEditorWindow : Window
{
    private readonly ProfileEditorViewModel _viewModel;

    public ProfileEditorWindow()
        : this(new ProfileEditorViewModel(null))
    {
    }

    public ProfileEditorWindow(ProfileEditorViewModel viewModel)
    {
        _viewModel = viewModel;
        DataContext = viewModel;
        InitializeComponent();
    }

    private void SaveClick(object? sender, RoutedEventArgs args)
    {
        if (_viewModel.TryBuild(out var result))
        {
            Close(result);
        }
    }

    private void CancelClick(object? sender, RoutedEventArgs args) => Close(null);
}
