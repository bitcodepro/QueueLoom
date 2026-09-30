using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.VisualTree;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;

namespace QueueLoom.UiTests;

public sealed class UpdateDialogUiTests
{
    [Fact]
    public Task UpdateDialog_ShowsTheRightButtonsInEachStage() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var update = new UpdateCheckResult(new Version(1, 5, 0), "v1.5.0", new Uri("https://github.com/bitcodepro/QueueLoom/releases/tag/v1.5.0"));
        var release = new TaskCompletionSource();
        var dialog = new UpdateDialogViewModel("1.4.0", update, async (progress, token) =>
        {
            progress.Report(new UpdateProgress(21 * 1024 * 1024, 52 * 1024 * 1024));
            await release.Task.WaitAsync(token);
        });
        var window = new UpdateDialogWindow(dialog, null);
        window.Show();

        Assert.Equal(["Release notes", "Not now", "Update now"], VisibleButtons(window));
        await Save(window, "update-available");

        var installing = dialog.InstallAsync();
        Assert.Equal(["Release notes", "Cancel"], VisibleButtons(window));
        await Save(window, "update-downloading");

        release.SetResult();
        await installing;
        Assert.Equal(["Release notes", "Restart later", "Restart now"], VisibleButtons(window));
        window.Close();

        var readOnly = new UpdateDialogWindow(new UpdateDialogViewModel("1.4.0", update, null,
            "QueueLoom cannot write to its folder (C:\\Program Files\\QueueLoom)."), null);
        readOnly.Show();
        Assert.Equal(["Release notes", "Not now", "Open releases page"], VisibleButtons(readOnly));
        readOnly.Close();
        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
    });

    private static string[] VisibleButtons(Window window)
    {
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        return window.GetVisualDescendants().OfType<Button>().Where(button => button.IsEffectivelyVisible)
            .Select(button => button.Content as string ?? string.Empty).ToArray();
    }

    private static async Task Save(Window window, string name)
    {
        var directory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        using var frame = window.CaptureRenderedFrame();
        if (!string.IsNullOrWhiteSpace(directory) && frame is not null)
        {
            Directory.CreateDirectory(directory);
            await using var file = File.Create(Path.Combine(directory, $"{name}.png"));
            frame.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
        }
    }
}
