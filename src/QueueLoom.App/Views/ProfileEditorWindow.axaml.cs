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

    private async void BrowseTlsFileClick(object? sender, RoutedEventArgs args)
    {
        // async void: nothing may escape, or the application ends.
        try
        {
            var target = (sender as Control)?.Tag as string;
            var files = await StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
            {
                Title = target switch
                {
                    "ca" => "Choose the certificate authority",
                    "key" => "Choose the client key",
                    _ => "Choose the client certificate"
                },
                AllowMultiple = false,
                FileTypeFilter = target == "certificate"
                    ? [new FilePickerFileType("Certificate") { Patterns = ["*.pfx", "*.p12", "*.pem", "*.crt", "*.cer"] }, FilePickerFileTypes.All]
                    : [new FilePickerFileType("PEM") { Patterns = ["*.pem", "*.crt", "*.cer", "*.key"] }, FilePickerFileTypes.All]
            });
            if (files.Count == 0 || files[0].TryGetLocalPath() is not { } path)
            {
                return;
            }
            switch (target)
            {
                case "ca": _viewModel.TlsCaCertificatePath = path; break;
                case "key": _viewModel.TlsClientKeyPath = path; break;
                default: _viewModel.TlsClientCertificatePath = path; break;
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            _viewModel.ReportKeyFileProblem(exception);
        }
    }

    private async void LoadKeyFileClick(object? sender, RoutedEventArgs args)
    {
        // async void: nothing may escape, or the application ends. The picker itself can fail too.
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
            await _viewModel.LoadGoogleServiceAccountKeyAsync(() => files[0].OpenReadAsync());
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            _viewModel.ReportKeyFileProblem(exception);
        }
    }
}
