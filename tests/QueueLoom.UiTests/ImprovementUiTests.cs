using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.Interactivity;
using Avalonia.VisualTree;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

public sealed class ImprovementUiTests
{
    [Fact]
    public Task HistoryContinueAndClearAreSeparateAndNewerActivitySurvivesClear() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        var profile = DemoData.Development;
        var plan = await fixture.OperationStore.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("orders"),
            [(new MessageDraft(new EditableMessageBody("test", MessageBodyFormat.Text)), "local backup")], false, 50, default,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile));
        await fixture.ViewModel.RefreshOperationHistoryCommand.ExecuteAsync();
        await fixture.NavigateAsync("Activity");
        Assert.Single(fixture.ViewModel.OperationItems);
        var check = fixture.Window.GetVisualDescendants().OfType<CheckBox>().Single(c => c.DataContext is OperationItemViewModel);
        check.IsChecked = true;
        var continuing = fixture.ViewModel.ContinueOperationCommand.ExecuteAsync();
        await fixture.SettleAsync();
        var review = fixture.Window.OwnedWindows.OfType<ConfirmDialogWindow>().Single();
        Assert.Contains("Saved bodies", ((ConfirmDialogViewModel)review.DataContext!).Message, StringComparison.Ordinal);
        Confirm(review); await continuing; await fixture.SettleAsync();
        Assert.Equal("Sent", fixture.OperationStore.ReadHistory(plan).Items[0].State);
        Assert.False(fixture.ViewModel.OperationItems[0].CanSelect);
        await Save(fixture.Window, "operation-history.png");

        var clearing = fixture.ViewModel.ClearActivityViewCommand.ExecuteAsync();
        await fixture.SettleAsync();
        var clearDialog = fixture.Window.OwnedWindows.OfType<ConfirmDialogWindow>().Single();
        Assert.Contains("operation/retry history", ((ConfirmDialogViewModel)clearDialog.DataContext!).Message, StringComparison.Ordinal);
        var newer = new ActivityItemViewModel(DateTimeOffset.UtcNow, "Info", "Concurrent arrival", "kept");
        fixture.ViewModel.Activity.Insert(0, newer);
        Confirm(clearDialog); await clearing;
        Assert.Equal(newer, Assert.Single(fixture.ViewModel.Activity));
        Assert.Single(fixture.OperationStore.List());
        fixture.ViewModel.RestoreActivityViewCommand.Execute(null);
        Assert.True(fixture.ViewModel.Activity.Count > 1);
        Assert.Single(fixture.OperationStore.List());
        Assert.Empty(BindingErrors.Instance.Messages);
    });

    [Fact]
    public Task BackupBrowsingDoesNotLoadBodyUntilOpenMessage() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.NavigateAsync("Backups");
        await fixture.ViewModel.RefreshBackupsCommand.Completion;
        Assert.NotNull(fixture.ViewModel.SelectedBackup);
        Assert.Null(fixture.ViewModel.SelectedBackupMessage);
        var open = fixture.Window.GetVisualDescendants().OfType<Button>().Single(b => Equals(b.Content, "Open message"));
        Assert.True(open.IsEffectivelyVisible);
        await fixture.ViewModel.LoadSelectedBackupCommand.ExecuteAsync();
        Assert.NotNull(fixture.ViewModel.SelectedBackupMessage);
        await Save(fixture.Window, "backup-lazy-body.png");
    });

    [Fact]
    public Task UpdateRetryRepeatedClicksAndInterruptedInstallationRemainSingleAttempt() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var calls = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var model = new UpdateDialogViewModel("1.5.5", new UpdateCheckResult(new Version(1, 6, 0), "v1.6.0", new Uri("https://example.test")),
            async (progress, _) =>
            {
                if (++calls == 1) throw new UpdateStageException(UpdatePhase.Verification, "Checksum mismatch", true, new IOException());
                progress.Report(new UpdateProgress(0, null) { Phase = UpdatePhase.Installation });
                await release.Task;
            });
        var owner = new Window(); owner.Show();
        var window = new UpdateDialogWindow(model, null);
        var result = window.ShowDialog<UpdateDialogResult>(owner);
        Click(window, "Update now");
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        Assert.True(model.CanRetry);
        await Save(window, "update-verification-retry.png");
        Click(window, "Retry update"); Click(window, "Retry update");
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        Assert.Equal(2, calls);
        Assert.False(model.CanCancel);
        window.Close(); // Closing during installation must leave the dialog and handoff alive.
        Assert.False(result.IsCompleted);
        await Save(window, "update-installing.png");
        release.SetResult();
        for (var i = 0; i < 20 && !result.IsCompleted; i++) { await Task.Delay(10); Avalonia.Threading.Dispatcher.UIThread.RunJobs(); }
        Assert.True(result.IsCompleted);
        Assert.Equal(UpdateDialogResult.Restart, await result);
        owner.Close();
        Assert.Empty(BindingErrors.Instance.Messages);
    });

    private static void Confirm(Window window) => window.GetVisualDescendants().OfType<Button>()
        .Single(b => b.IsEffectivelyVisible && Equals(b.Content, ((ConfirmDialogViewModel)window.DataContext!).ConfirmLabel)).RaiseEvent(new RoutedEventArgs(Button.ClickEvent));

    private static void Click(Window window, string label) => window.GetVisualDescendants().OfType<Button>()
        .Single(b => Equals(b.Content, label)).RaiseEvent(new RoutedEventArgs(Button.ClickEvent));

    private static async Task Save(Window window, string name)
    {
        var root = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
        if (string.IsNullOrWhiteSpace(root)) return;
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        using var frame = window.CaptureRenderedFrame();
        if (frame is null) return;
        Directory.CreateDirectory(root);
        await using var file = File.Create(Path.Combine(root, name));
        frame.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
    }
}
