using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
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

    private async void LoadKeyFileClick(object? sender, RoutedEventArgs args)
    {
        try
        {
            var files = await StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
            {
                Title = "Choose the service account key",
                AllowMultiple = false,
                FileTypeFilter = [new FilePickerFileType("JSON key") { Patterns = ["*.json"] }]
            });
            if (files.Count == 0)
            {
                return;
            }

            await using var stream = await files[0].OpenReadAsync();
            if (stream.Length > 64 * 1024)
            {
                return;
            }
            using var reader = new StreamReader(stream);
            _viewModel.GoogleServiceAccountKey = await reader.ReadToEndAsync();
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // The summary under the box keeps explaining what is expected.
        }
    }
}
