using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
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
        Assert.Equal(lostAck ? "DeleteUncertain" : "SentOriginalKept", store.ReadHistory(plan).Items[0].State);
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
