using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.Interactivity;
using Avalonia.Threading;
using Avalonia.VisualTree;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

public sealed class StabilityUiTests
{
    [Fact]
    public Task IncompleteScheduledActivationIsUnselectableAfterRefreshAndRepeatedContinueIsBlocked() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        var profile = DemoData.Development;
        var original = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 7,
            ReadOnlyMemory<byte>.Empty, new EditableMessageProperties(MessageId: "original"));
        var item = new ResendItem(original, ServiceBusEntityReference.Queue("orders"),
            new MessageDraft(new EditableMessageBody("isolated", MessageBodyFormat.Text), new EditableMessageProperties(MessageId: "scheduled-copy")));
        var plan = await fixture.OperationStore.CreateResendAsync(profile.Id, [item, item], ResendMode.Copy, 50,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Scheduled resend", default, deferActivation: true);
        var state = Path.Combine(fixture.OperationStore.RootDirectory, plan.Id.ToString("N"), "000000.state");
        File.WriteAllText(state, "Pending"); // Persisted midpoint of the old activation loop.
        await fixture.ViewModel.RefreshOperationHistoryCommand.ExecuteAsync();
        await fixture.NavigateAsync("Activity");
        var checks = fixture.Window.GetVisualDescendants().OfType<CheckBox>().Where(control => control.DataContext is OperationItemViewModel).ToArray();
        Assert.Equal(2, checks.Length);
        Assert.All(checks, control => Assert.False(control.IsEnabled));
        Assert.All(fixture.ViewModel.OperationItems, row => Assert.Contains("Blocked scheduled snapshot", row.Outcome, StringComparison.Ordinal));
        fixture.ViewModel.OperationItems[0].IsMarked = true; // The command must independently guard stale/bypassed selection.
        await fixture.ViewModel.ContinueOperationCommand.ExecuteAsync();
        await fixture.ViewModel.ContinueOperationCommand.ExecuteAsync();
        Assert.Contains("cannot be retried", fixture.ViewModel.ErrorText, StringComparison.Ordinal);
        Assert.Empty(fixture.Window.OwnedWindows);
        Assert.Equal("Pending", File.ReadAllText(state));
        await Save(fixture.Window, "stability-blocked-scheduled-history.png");
        Assert.Empty(BindingErrors.Instance.Messages);
    });

    [Fact]
    public Task CancellingActivityClearThenRepeatedClearAndRestorePreserveOperationHistory() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        var profile = DemoData.Development;
        var plan = await fixture.OperationStore.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("orders"),
            [(new MessageDraft(new EditableMessageBody("test", MessageBodyFormat.Text)), "local backup")], false, 50, default,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile));
        await fixture.ViewModel.RefreshOperationHistoryCommand.ExecuteAsync();
        await fixture.NavigateAsync("Activity");
        var before = fixture.ViewModel.Activity.ToArray();
        var cancelled = fixture.ViewModel.ClearActivityViewCommand.ExecuteAsync();
        await fixture.SettleAsync();
        fixture.Window.OwnedWindows.OfType<ConfirmDialogWindow>().Single().Close(false);
        await cancelled;
        Assert.Equal(before, fixture.ViewModel.Activity);
        for (var attempt = 0; attempt < 2; attempt++)
        {
            var clearing = fixture.ViewModel.ClearActivityViewCommand.ExecuteAsync();
            await fixture.ViewModel.ClearActivityViewCommand.ExecuteAsync();
            await fixture.SettleAsync();
            var review = fixture.Window.OwnedWindows.OfType<ConfirmDialogWindow>().Single();
            review.GetVisualDescendants().OfType<Button>().Single(button => button.IsEffectivelyVisible &&
                Equals(button.Content, ((ConfirmDialogViewModel)review.DataContext!).ConfirmLabel)).RaiseEvent(new RoutedEventArgs(Button.ClickEvent));
            await clearing;
            Assert.Empty(fixture.ViewModel.Activity);
            fixture.ViewModel.RestoreActivityViewCommand.Execute(null);
            Assert.Equal(before.Length, fixture.ViewModel.Activity.Count);
            Assert.Equal(plan, Assert.Single(fixture.OperationStore.List()));
            Assert.Equal("Pending", Assert.Single(fixture.OperationStore.ReadHistory(plan).Items).State);
        }
        await Save(fixture.Window, "stability-restored-activity.png");
        Assert.Empty(BindingErrors.Instance.Messages);
    });

    private static async Task Save(Window window, string name)
    {
        var root = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
        if (string.IsNullOrWhiteSpace(root)) return;
        Dispatcher.UIThread.RunJobs();
        using var frame = window.CaptureRenderedFrame();
        if (frame is null) return;
        Directory.CreateDirectory(root);
        await using var output = File.Create(Path.Combine(root, name));
        frame.Save(output, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
    }
}
