using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task FailedAttemptStillThrottlesNextAttemptIncludingSelectiveRetry(bool retry, bool timeout)
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        var plan = await store.CreateResendAsync(profile.Id, [OperationItem(0), OperationItem(1)], ResendMode.Copy, 1,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Throttle regression", default);
        if (retry)
        {
            store.DelayAsync = (_, _) => Task.CompletedTask;
            workspace.OnSend = () => throw new DeliveryRejectedException("Proven rejection before routing repair");
            await store.RunItemsAsync(plan, [0, 1], false, workspace, () => true, null, default);
            workspace.SentMessages.Clear();
        }
        var delayed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.DelayAsync = (interval, _) =>
        {
            Assert.Equal(TimeSpan.FromSeconds(1), interval);
            delayed.TrySetResult();
            return release.Task;
        };
        workspace.OnSend = () =>
        {
            if (workspace.SentMessages.Count != 1) return;
            if (timeout) throw new TimeoutException("Lost send acknowledgement");
            throw new DeliveryRejectedException("Provider proved non-delivery");
        };
        var running = store.RunItemsAsync(plan, [0, 1], retry, workspace, () => true, null, default);
        try
        {
            await Task.WhenAny(delayed.Task, running).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Single(workspace.SentMessages); // Second transport call cannot precede the rate barrier.
            Assert.Equal(retry ? "Rejected" : "Pending", store.ReadHistory(plan).Items[1].State);
        }
        finally { release.TrySetResult(); await running; }
        Assert.Equal(2, workspace.SentMessages.Count);
    }

    private static ResendItem OperationItem(int index) => new(
        new BrowsedMessage(ServiceBusEntityReference.Queue("source"), ServiceBusSubQueue.DeadLetter, index,
            ReadOnlyMemory<byte>.Empty, new EditableMessageProperties(MessageId: $"original-{index}")),
        ServiceBusEntityReference.Queue("target"),
        new MessageDraft(new EditableMessageBody("hello", MessageBodyFormat.Text), new EditableMessageProperties(MessageId: $"copy-{index}")));

    private static async Task<ReplayPlan> PrepareOperation(BatchReplayStore store, ServiceBusProfile profile, ResendMode mode = ResendMode.Copy, int count = 2) =>
        await store.CreateResendAsync(profile.Id, Enumerable.Range(0, count).Select(OperationItem).ToArray(), mode, 50,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Immediate resend", default);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LostAcknowledgementOrCancellationNeverAuthorizesResend(bool cancellation)
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        var plan = await PrepareOperation(store, profile);
        workspace.OnSend = () => { if (cancellation) throw new OperationCanceledException(); throw new TimeoutException("Acknowledgement lost"); };
        await store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        Assert.Equal("Uncertain", store.ReadHistory(plan).Items[0].State);
        var reopened = new BatchReplayStore(directory.Path);
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunItemsAsync(plan, [0], true, workspace, () => true, null, default));
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunItemsAsync(plan, [0], false, workspace, () => true, null, default));
        workspace.OnSend = null;
        await reopened.RunItemsAsync(plan, [1], false, workspace, () => true, null, default);
        Assert.Equal(2, workspace.SentMessages.Count); // Pending can continue without retrying the uncertain item.
    }

    [Fact]
    public async Task ProvenRejectionRetriesOnlySelectedItemAndKeepsStableId()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        var plan = await PrepareOperation(store, profile);
        workspace.OnSend = () => throw new DeliveryRejectedException("Provider explicitly rejected delivery");
        await store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        Assert.Equal("Rejected", store.ReadHistory(plan).Items[0].State);
        workspace.OnSend = null;
        await store.RunItemsAsync(plan, [0], true, workspace, () => true, null, default);
        Assert.Equal("copy-0", workspace.SentMessages[0].Message.Properties.MessageId);
        Assert.Equal("copy-0", workspace.SentMessages[1].Message.Properties.MessageId);
        Assert.Equal("Pending", store.ReadHistory(plan).Items[1].State);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0], true, workspace, () => true, null, default));
        Assert.Equal(2, workspace.SentMessages.Count);
    }

    [Theory]
    [InlineData("Sending", 0, "Pending")]
    [InlineData("Sent", 1, "Sending")]
    [InlineData("Deleting", 1, "Sent")]
    [InlineData("Moved", 1, "DeleteUncertain")]
    public async Task PersistenceFailuresBlockEffectsOrLeaveNonRetriableIntent(string failingState, int sends, string expectedState)
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        var plan = await PrepareOperation(store, profile, ResendMode.Move, 1);
        store.BeforeStateWrite = (_, state) => { if (state == failingState) throw new IOException("Injected durable write failure"); };
        if (failingState == "Moved") await store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        else await Assert.ThrowsAsync<IOException>(() => store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default));
        Assert.Equal(sends, workspace.SentMessages.Count);
        Assert.Equal(expectedState, store.ReadHistory(plan).Items[0].State);
        Assert.Equal(failingState == "Moved" ? 1 : 0, workspace.DeleteRequests.Count);
        store.BeforeStateWrite = null;
        if (sends != 0) await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default));
    }

    // A Failed settlement may have been accepted before its response was lost: the item stays DeleteUncertain and the
    // three-day cleanup keeps its evidence, through the real executor rather than a hand-written state.
    [Fact]
    public async Task FailedSettlementInAMoveIsUncertainAndSurvivesRetention()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        workspace.FailedSequenceNumbers.Add(0);
        var plan = await PrepareOperation(store, profile, ResendMode.Move, 1);

        await store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        Assert.Equal("DeleteUncertain", store.ReadHistory(plan).Items[0].State);

        var folder = Directory.GetDirectories(directory.Path).Single();
        foreach (var file in Directory.GetFiles(folder)) File.SetLastWriteTimeUtc(file, DateTime.UtcNow.AddDays(-10));
        Assert.Equal(0, store.DeleteExpired(DateTimeOffset.UtcNow.AddDays(-3)));
        Assert.True(File.Exists(Path.Combine(folder, "plan.json")));
    }

    // SentOriginalKept was written by earlier versions for a failed settlement too, so it is not treated as finished.
    [Fact]
    public void LegacySentOriginalKeptIsNotAFinishedMove()
    {
        Assert.False(BatchReplayStore.IsFinishedState(ResendMode.Move, "SentOriginalKept"));
        Assert.True(BatchReplayStore.IsFinishedState(ResendMode.Move, "OriginalKept"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConfirmedMoveNeverResendsAfterDeletionFailureOrLostDeleteAcknowledgement(bool lostAck)
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        if (lostAck) workspace.OnDelete = () => throw new TimeoutException("Delete acknowledgement lost");
        else workspace.MissingSequenceNumbers.Add(0);
        var plan = await PrepareOperation(store, profile, ResendMode.Move, 1);
        var result = await store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        Assert.Equal(1, result.SentCount); Assert.Equal(1, result.OriginalsKeptCount);
        Assert.Equal(lostAck ? "DeleteUncertain" : "OriginalKept", store.ReadHistory(plan).Items[0].State);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0], true, workspace, () => true, null, default));
        Assert.Single(workspace.SentMessages); Assert.Single(workspace.DeleteRequests);
    }

    [Fact]
    public async Task ConcurrentOwnersAndRepeatedClicksCannotSendAnItemTwice()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        var plan = await PrepareOperation(store, profile, count: 1);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        workspace.SendGate = () => { entered.TrySetResult(); return release.Task; };
        var first = store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        await Task.WhenAny(entered.Task, first);
        if (first.IsCompleted) await first;
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var second = new BatchReplayStore(directory.Path).RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        release.SetResult(); await first;
        await Assert.ThrowsAsync<InvalidOperationException>(() => second);
        Assert.Single(workspace.SentMessages);
    }

    [Fact]
    public async Task CancellationAfterAcknowledgementRetainsSentAndUnattemptedStates()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        var plan = await PrepareOperation(store, profile);
        using var cancel = new CancellationTokenSource();
        workspace.OnSend = cancel.Cancel;
        await store.RunItemsAsync(plan, [0, 1], false, workspace, () => true, null, cancel.Token);
        Assert.Equal(["Sent", "Pending"], store.ReadHistory(plan).Items.Select(i => i.State));
        workspace.OnSend = null;
        await store.RunItemsAsync(plan, [1], false, workspace, () => true, null, default);
        Assert.Equal(2, workspace.SentMessages.Count);
    }

    [Fact]
    public async Task ConfigurationAndWriteAccessAreRevalidatedBeforeEveryEffect()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        var plan = await PrepareOperation(store, profile, ResendMode.Move, 1);
        var canWrite = true;
        workspace.OnSend = () => canWrite = false;
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0], false, workspace, () => canWrite, null, default));
        Assert.Single(workspace.SentMessages); Assert.Empty(workspace.DeleteRequests);
        Assert.Equal("Sent", store.ReadHistory(plan).Items[0].State);
        await workspace.ConnectAsync(profile with { FullyQualifiedNamespace = "changed.servicebus.windows.net" });
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default));
        Assert.Single(workspace.SentMessages);
    }
}
