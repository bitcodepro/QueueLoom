using System.Text.Json.Nodes;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client.Exceptions;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(null, false)]
    [InlineData(null, true)]
    [InlineData("", false)]
    [InlineData(" ", false)]
    public async Task ReplayRecoveryRejectsMissingIdentityBeforeEitherExecutorCallsProvider(string? identity, bool retainNamespace)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(directory.Path);
        var plan = await PrepareReplayRegression(store, profile);
        var path = Path.Combine(directory.Path, plan.Id.ToString("N"), "plan.json");
        var legacy = JsonNode.Parse(await File.ReadAllTextAsync(path))!.AsObject();
        // Emulate an actual pre-identity JSON snapshot, rather than only a nullable constructor.
        if (identity is null) legacy.Remove("ConfigurationIdentity");
        else legacy["ConfigurationIdentity"] = identity;
        if (!retainNamespace) legacy.Remove("Namespace");
        await File.WriteAllTextAsync(path, legacy.ToJsonString());
        var reopened = new BatchReplayStore(directory.Path);
        plan = Assert.Single(reopened.List());
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile with { ConfigurationRevision = Guid.NewGuid() });
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunAsync(plan, workspace, () => true, null, default));
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunItemsAsync(plan, [0], false, workspace, () => true, null, default));
        Assert.Empty(workspace.SentMessages);
        Assert.All(reopened.ReadHistory(plan).Items, item => Assert.Equal("Pending", item.State));
    }

    [Fact]
    public async Task ReplayRecoveryRetainsLegacyHistoryAndAcceptsEstablishedIdentityWithoutNamespace()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(directory.Path);
        var plan = await PrepareReplayRegression(store, profile);
        var folder = Path.Combine(directory.Path, plan.Id.ToString("N"));
        var path = Path.Combine(folder, "plan.json");
        var legacy = JsonNode.Parse(await File.ReadAllTextAsync(path))!.AsObject();
        legacy.Remove("Namespace");
        await File.WriteAllTextAsync(path, legacy.ToJsonString());
        File.Delete(Path.Combine(folder, "000000.metadata.json"));
        var reopened = new BatchReplayStore(directory.Path);
        plan = Assert.Single(reopened.List());
        Assert.Equal("Legacy replay snapshot", reopened.ReadHistory(plan).Items[0].Origin);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        await reopened.RunAsync(plan, workspace, () => true, null, default);
        await reopened.RunAsync(plan, workspace, () => true, null, default);
        Assert.Equal(["saved-0", "saved-1"], workspace.SentMessages.Select(send => send.Message.Properties.MessageId));
    }

    [Fact]
    public async Task ReplayRecoveryResumeCallerConfirmsButBlocksLegacySnapshotAfterSameProfileChanges()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(directory.Path);
        var plan = await store.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("target"),
            [(MessageDraft.Empty, "legacy")], false, 50, default);
        var changed = profile with { ConfigurationRevision = Guid.NewGuid() };
        var dialogs = new FakeDialogService { ConfirmResult = true };
        var workspace = new FakeWorkspace();
        await using var vm = CreateViewModel(new FakeProfileRepository([changed], changed.Id), workspace, dialogs, replayStore: store);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        await vm.ResumeReplayCommand.ExecuteAsync();
        Assert.Equal("Review batch replay", Assert.Single(dialogs.Confirmations).Title);
        Assert.Empty(workspace.SentMessages);
        Assert.Contains("configuration identity", vm.ReplayStatus, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("Pending", Assert.Single(store.ReadHistory(plan).Items).State);
    }

    [Theory]
    [InlineData("replay")]
    [InlineData("restore")]
    [InlineData("resume")]
    public async Task ReplayRecoveryCallerPersistsRejectionAndSelectivelyRetriesSameSavedId(string caller)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var source = ServiceBusEntityReference.Queue("source");
        var message = SearchMessage(source, 42, "2026-08-12T10:00:00Z");
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("target", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))]) };
        var store = new BatchReplayStore(directory.Path);
        var backups = new FakeBackupRepository(CreateBackupSummary(profile, source, message), message);
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs, backups, replayStore: store);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);
        vm.Messages.Add(new MessageItemViewModel(message, profile.Id, profile.Name));
        await vm.RefreshBackupsCommand.ExecuteAsync();
        if (caller == "resume") await PrepareReplayRegression(store, profile);
        workspace.OnSend = () => throw new DeliveryRejectedException("Structured mandatory return");
        var command = caller == "replay" ? vm.ReplayLoadedMessagesCommand :
            caller == "restore" ? vm.RestoreFilteredBackupsCommand : vm.ResumeReplayCommand;
        Assert.True(command.CanExecute(null));
        await command.ExecuteAsync();
        // Each command refreshes the operation history in its finally without awaiting it. These tests run without the
        // window's UI thread, so a refresh still running would overlap the next one on another thread and could replace
        // the items ticked below; it ends first.
        await vm.OperationHistoryRefresh;
        Assert.Equal("Review batch replay", Assert.Single(dialogs.Confirmations).Title);
        var reopened = new BatchReplayStore(directory.Path);
        var plan = Assert.Single(reopened.List());
        Assert.Equal("Rejected", reopened.ReadHistory(plan).Items[0].State);
        var id = Assert.Single(workspace.SentMessages).Message.Properties.MessageId;
        workspace.OnSend = null;
        // Resume must not automatically retry the proven rejection or any later Pending item.
        await vm.ResumeReplayCommand.ExecuteAsync();
        await vm.OperationHistoryRefresh;
        Assert.Single(workspace.SentMessages);
        await vm.RefreshOperationHistoryCommand.ExecuteAsync();
        vm.OperationItems[0].IsMarked = true;
        await vm.RetryRejectedOperationCommand.ExecuteAsync();
        Assert.Equal([id, id], workspace.SentMessages.Select(send => send.Message.Properties.MessageId));
        Assert.Equal("Sent", reopened.ReadHistory(plan).Items[0].State);
        if (plan.Count == 2) Assert.Equal("Pending", reopened.ReadHistory(plan).Items[1].State);
    }

    [Theory]
    [InlineData("timeout")]
    [InlineData("cancel")]
    [InlineData("nack")]
    [InlineData("lost-ack")]
    public async Task ReplayRecoveryUnknownSendNeverResendsAfterReopen(string failure)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(directory.Path);
        var plan = await PrepareReplayRegression(store, profile);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        Exception error = failure switch
        {
            "timeout" => new TimeoutException("Publish confirmation timed out"),
            "cancel" => new OperationCanceledException("Cancelled waiting for confirmation"),
            "nack" => new PublishException(1, false),
            _ => new IOException("Connection lost after write")
        };
        workspace.OnSend = () => throw error;
        Assert.Same(error, await Record.ExceptionAsync(() => store.RunAsync(plan, workspace, () => true, null, default)));
        var reopened = new BatchReplayStore(directory.Path);
        Assert.Equal(["Uncertain", "Pending"], reopened.ReadHistory(plan).Items.Select(item => item.State));
        workspace.OnSend = null;
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunAsync(plan, workspace, () => true, null, default));
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunItemsAsync(plan, [0], true, workspace, () => true, null, default));
        Assert.Single(workspace.SentMessages);
    }

    [Theory]
    [InlineData("Sending", 0, "Pending")]
    [InlineData("Sent", 1, "Sending")]
    public async Task ReplayRecoveryStateWriteFailureNeverBecomesTransportRejection(string state, int sends, string expected)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(directory.Path);
        var plan = await PrepareReplayRegression(store, profile);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        // Even an exception of the transport's rejection type here is a persistence failure.
        store.BeforeStateWrite = (_, value) => { if (value == state) throw new DeliveryRejectedException("Injected state persistence failure"); };
        await Assert.ThrowsAsync<DeliveryRejectedException>(() => store.RunAsync(plan, workspace, () => true, null, default));
        var reopened = new BatchReplayStore(directory.Path);
        Assert.Equal(sends, workspace.SentMessages.Count);
        Assert.Equal(expected, reopened.ReadHistory(plan).Items[0].State);
        if (sends > 0)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunAsync(plan, workspace, () => true, null, default));
            await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunItemsAsync(plan, [0], true, workspace, () => true, null, default));
            Assert.Single(workspace.SentMessages);
        }
    }

    private static Task<ReplayPlan> PrepareReplayRegression(BatchReplayStore store, ServiceBusProfile profile) =>
        store.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("target"), Enumerable.Range(0, 2).Select(index =>
            (new MessageDraft(new EditableMessageBody("copy", MessageBodyFormat.Text), new EditableMessageProperties(MessageId: $"saved-{index}")), $"source {index}")),
            true, 50, default, profile.EndpointDisplay, ScheduledResend.IdentityFor(profile));
}
