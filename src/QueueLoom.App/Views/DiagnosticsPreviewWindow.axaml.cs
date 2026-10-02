using Avalonia.Controls;
using Avalonia.Interactivity;
using QueueLoom.App.Services;

namespace QueueLoom.App.Views;

public sealed partial class DiagnosticsPreviewWindow : Window
{
    public DiagnosticsPreviewWindow() { InitializeComponent(); }
    public DiagnosticsPreviewWindow(DiagnosticsPreview preview) : this() { DataContext = preview; }
    private void CancelClick(object? sender, RoutedEventArgs args) => Close(false);
    private void SaveClick(object? sender, RoutedEventArgs args) => Close(true);
}
