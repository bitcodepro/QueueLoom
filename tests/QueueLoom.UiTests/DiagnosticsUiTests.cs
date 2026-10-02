using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Threading;
using Avalonia.VisualTree;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;

namespace QueueLoom.UiTests;

public sealed class DiagnosticsUiTests
{
    [Fact]
    public Task SettingsAndOperationError_OpenTheSameFrozenPreview_AndDismissSafely() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync(connect: false);
        var window = fixture.Window;
        var settings = window.GetVisualDescendants().OfType<Button>().Single(b => b.Content as string == "Settings");
        var flyout = Assert.IsType<MenuFlyout>(settings.Flyout);
        flyout.ShowAt(settings); Dispatcher.UIThread.RunJobs();
        var action = Assert.IsType<MenuItem>(Assert.Single(flyout.Items));
        Assert.Equal("Export diagnostics", action.Header);
        Assert.Same(fixture.ViewModel.ExportDiagnosticsCommand, action.Command);
        action.Command!.Execute(null); flyout.Hide(); Dispatcher.UIThread.RunJobs();
        var preview = Assert.Single(window.OwnedWindows.OfType<DiagnosticsPreviewWindow>());
        var capture = Assert.IsType<DiagnosticsPreview>(preview.DataContext);
        Assert.Equal(capture.Report, preview.GetVisualDescendants().OfType<SelectableTextBlock>().First().Text);
        preview.GetVisualDescendants().OfType<Button>().Single(b => b.Content as string == "Cancel")
            .RaiseEvent(new RoutedEventArgs(Button.ClickEvent));
        await fixture.ViewModel.ExportDiagnosticsCommand.Completion;
        fixture.ViewModel.DraftBody = "{";
        fixture.ViewModel.FormatJsonCommand.Execute(null); Dispatcher.UIThread.RunJobs();
        Assert.True(fixture.ViewModel.HasError);
        var error = window.GetVisualDescendants().OfType<Button>().Single(b => b.IsEffectivelyVisible && b.Content as string == "Export diagnostics");
        error.Command!.Execute(null); Dispatcher.UIThread.RunJobs();
        preview = Assert.Single(window.OwnedWindows.OfType<DiagnosticsPreviewWindow>());
        preview.Close(); await fixture.ViewModel.ExportDiagnosticsCommand.Completion;
        Assert.Empty(window.OwnedWindows);
        Assert.Empty(BindingErrors.Instance.Messages);
    });

    [Fact]
    public Task Preview_ShowsExactBothFiles_AndChooseDestinationReturnsTrue() => UiSession.RunAsync(async () =>
    {
        var journal = new DiagnosticsJournal(); journal.Begin("Connecting");
        var capture = journal.Capture();
        var owner = new Window(); owner.Show();
        var window = new DiagnosticsPreviewWindow(capture);
        var result = window.ShowDialog<bool>(owner); Dispatcher.UIThread.RunJobs();
        var tabs = window.GetVisualDescendants().OfType<TabControl>().Single(); tabs.SelectedIndex = 1; Dispatcher.UIThread.RunJobs();
        Assert.Contains(window.GetVisualDescendants().OfType<SelectableTextBlock>(), t => t.Text == capture.Json);
        window.GetVisualDescendants().OfType<Button>().Single(b => b.Content as string == "Choose ZIP destination")
            .RaiseEvent(new RoutedEventArgs(Button.ClickEvent));
        Assert.True(await result); owner.Close();
    });

    [Fact]
    public Task UpdaterFailure_OffersDiagnosticsWithRecordedStage() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync(connect: false);
        var vm = new UpdateDialogViewModel("1.5.7", new(new Version(9, 0, 0), "v9.0.0", new Uri("https://example.test")),
            (_, _) => Task.FromException(new UpdateStageException(UpdatePhase.Verification, "PRIVATE_TOKEN", true, new IOException("PRIVATE_PATH"))),
            exportDiagnostics: fixture.ViewModel.ExportDiagnosticsCommand, diagnostics: fixture.ViewModel.Diagnostics);
        await vm.InstallAsync();
        var window = new UpdateDialogWindow(vm, null); window.Show(); Dispatcher.UIThread.RunJobs();
        var button = window.GetVisualDescendants().OfType<Button>().Single(b => b.IsEffectivelyVisible && b.Content as string == "Export diagnostics");
        Assert.Same(fixture.ViewModel.ExportDiagnosticsCommand, button.Command);
        Assert.Contains("Verification", fixture.ViewModel.Diagnostics.Capture().Json, StringComparison.Ordinal);
        Assert.DoesNotContain("PRIVATE_TOKEN", fixture.ViewModel.Diagnostics.Capture().Json, StringComparison.Ordinal);
        window.Close();
    });
}
