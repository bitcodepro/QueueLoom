using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task LegacyScheduledSnapshotWithoutActivationProofPreservesConfirmedStatesAndBlocksPendingRecovery()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var plan = await store.CreateResendAsync(profile.Id, [OperationItem(0), OperationItem(1)], ResendMode.Copy, 50,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Scheduled resend", default, deferActivation: true);
        var folder = Path.Combine(directory.Path, plan.Id.ToString("N"));
        File.WriteAllText(Path.Combine(folder, "000000.state"), "Sent");
        File.WriteAllText(Path.Combine(folder, "000001.state"), "Pending");
        var planPath = Path.Combine(folder, "plan.json");
        var legacy = System.Text.Json.Nodes.JsonNode.Parse(File.ReadAllText(planPath))!.AsObject();
        legacy.Remove("RequiresScheduleActivation");
        File.WriteAllText(planPath, legacy.ToJsonString());
        plan = Assert.Single(new BatchReplayStore(directory.Path).List());
        Assert.Equal(["Sent", "AwaitingScheduleClaim"], store.ReadHistory(plan).Items.Select(item => item.State));
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [1], false, workspace, () => true, null, default));
        Assert.Empty(workspace.SentMessages);
        Assert.Equal("Sent", File.ReadAllText(Path.Combine(folder, "000000.state")));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    public async Task InterruptedScheduledActivationKeepsEveryItemBlockedAfterReopen(int boundary)
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var plan = await store.CreateResendAsync(profile.Id, [OperationItem(0), OperationItem(1)], ResendMode.Copy, 50,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Scheduled resend", default, deferActivation: true);
        store.BeforeStateWrite = (path, state) =>
        {
            if (boundary == -1 ? Path.GetFileName(path) == ".schedule-activated" :
                state == "Pending" && Path.GetFileName(path) == $"{boundary:D6}.state")
                throw new IOException("Interrupted activation before completion publication");
        };
        await Assert.ThrowsAsync<IOException>(() => store.ActivateScheduledAsync(plan, default));
        var reopened = new BatchReplayStore(directory.Path);
        Assert.All(reopened.ReadHistory(plan).Items, item => Assert.False(item.CanContinue));
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunItemsAsync(plan, [0], false, workspace, () => true, null, default));
        Assert.Empty(workspace.SentMessages);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not an activation")]
    [InlineData("foreign transaction")]
    public async Task DamagedActivationProofCannotAuthorizeAnyScheduledItem(string proof)
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var plan = await store.CreateResendAsync(profile.Id, [OperationItem(0)], ResendMode.Copy, 50,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Scheduled resend", default, deferActivation: true);
        await store.ActivateScheduledAsync(plan, default);
        File.WriteAllText(Path.Combine(directory.Path, plan.Id.ToString("N"), ".schedule-activated"), proof);
        var reopened = new BatchReplayStore(directory.Path);
        Assert.False(Assert.Single(reopened.ReadHistory(plan).Items).CanContinue);
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunItemsAsync(plan, [0], false, workspace, () => true, null, default));
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunAsync(plan, workspace, () => true, null, default));
        Assert.Empty(workspace.SentMessages);
    }

    [Fact]
    public async Task ActivatedScheduledSnapshotRetainsStableIdsAcrossSelectiveRecoveryAndCompetingOwners()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var plan = await store.CreateResendAsync(profile.Id, [OperationItem(0), OperationItem(1)], ResendMode.Copy, 50,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Scheduled resend", default, deferActivation: true);
        var firstActivation = store.ActivateScheduledAsync(plan, default);
        var other = new BatchReplayStore(directory.Path);
        var secondActivation = other.ActivateScheduledAsync(plan, default);
        await firstActivation;
        await Assert.ThrowsAsync<InvalidOperationException>(() => secondActivation);
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        workspace.OnSend = () => throw new DeliveryRejectedException("Proven rejection");
        await store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        Assert.Equal(["Rejected", "Pending"], other.ReadHistory(plan).Items.Select(item => item.State));
        workspace.OnSend = null;
        await other.RunItemsAsync(plan, [0], true, workspace, () => true, null, default);
        await other.RunItemsAsync(plan, [1], false, workspace, () => true, null, default);
        Assert.Equal(["copy-0", "copy-0", "copy-1"], workspace.SentMessages.Select(send => send.Message.Properties.MessageId));
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0, 1], false, workspace, () => true, null, default));
        Assert.Equal(3, workspace.SentMessages.Count);
    }

    [Fact]
    public async Task InterruptedActivationCannotSendThroughTheReplayExecutor()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var plan = await store.CreateResendAsync(profile.Id, [OperationItem(0)], ResendMode.Copy, 50,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Scheduled resend", default, deferActivation: true);
        // The old per-item activation had no durable completion boundary. This is its crash state.
        File.WriteAllText(Path.Combine(directory.Path, plan.Id.ToString("N"), "000000.state"), "Pending");
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        await Assert.ThrowsAsync<InvalidOperationException>(() => new BatchReplayStore(directory.Path).RunAsync(plan, workspace, () => true, null, default));
        Assert.Empty(workspace.SentMessages);
    }
}
