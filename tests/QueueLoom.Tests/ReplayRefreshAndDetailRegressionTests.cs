using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task BugCycleOne_DesktopDeletionDisplaysWarningAndRemovesConfirmedDeletedMessage()
    {
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var source = ServiceBusEntityReference.Queue("orders");
        var message = SearchMessage(source, 42, "2026-08-12T10:00:00Z");
        var workspace = new FakeWorkspace { ResultWarnings = ["delete-result.report could not be saved"],
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)))]) };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            new FakeDialogService { ConfirmResult = true });
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.Messages.Add(new MessageItemViewModel(message, profile.Id, profile.Name) { IsMarked = true });
        Assert.True(vm.DeleteMarkedMessagesCommand.CanExecute(null));
        await vm.DeleteMarkedMessagesCommand.ExecuteAsync();
        Assert.Empty(vm.Messages);
        Assert.Contains("1 of 1 deleted", vm.StatusText, StringComparison.Ordinal);
        Assert.Contains("Warning", vm.StatusText, StringComparison.Ordinal);
        Assert.Contains("delete-result.report", vm.StatusText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BugCycleOne_BackupReplayDestinationSurvivesSameEnvironmentTopologyRefresh()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var source = ServiceBusEntityReference.Queue("source");
        var message = SearchMessage(source, 42, "2026-08-12T10:00:00Z");
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("target", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))]) };
        var backups = new FakeBackupRepository(CreateBackupSummary(profile, source, message), message);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            new FakeDialogService { ConfirmResult = true }, backups, replayStore: new BatchReplayStore(directory.Path));
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.RefreshBackupsCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);
        var selected = vm.ReplayDestination;
        await vm.RefreshTopologyCommand.ExecuteAsync();
        Assert.Equal(selected.Reference, vm.ReplayDestination?.Reference);
        Assert.Same(Assert.Single(vm.Destinations), vm.ReplayDestination);
        Assert.True(vm.RestoreFilteredBackupsCommand.CanExecute(null));
        await vm.RestoreFilteredBackupsCommand.ExecuteAsync();
        Assert.Single(workspace.SentMessages);
    }

    [Theory]
    [InlineData(ResendMode.Copy)]
    [InlineData(ResendMode.Move)]
    public async Task BugCycleOne_SuccessfulReplayRetryDoesNotShowPreviousRejectionAsCurrentDetail(ResendMode mode)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(directory.Path);
        var original = SearchMessage(ServiceBusEntityReference.Queue("source"), 42, "2026-08-12T10:00:00Z");
        var item = new ResendItem(original, ServiceBusEntityReference.Queue("target"), original.CreateDraft()).WithNewMessageId();
        var plan = await store.CreateResendAsync(profile.Id, [item], mode, 50, profile.EndpointDisplay,
            ScheduledResend.IdentityFor(profile), "resend", default);
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        workspace.OnSend = () => throw new DeliveryRejectedException("old rejection");
        await store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        var rejected = Assert.Single(new BatchReplayStore(directory.Path).ReadHistory(plan).Items);
        Assert.Equal("Rejected", rejected.State);
        Assert.Contains("old rejection", rejected.Detail, StringComparison.Ordinal);
        workspace.OnSend = null;
        await store.RunItemsAsync(plan, [0], true, workspace, () => true, null, default);
        var recovered = Assert.Single(new BatchReplayStore(directory.Path).ReadHistory(plan).Items);
        Assert.Equal(mode == ResendMode.Move ? "Moved" : "Sent", recovered.State);
        Assert.True(string.IsNullOrEmpty(recovered.Detail), $"Current successful outcome retained: {recovered.Detail}");
        Assert.DoesNotContain("old rejection", new OperationItemViewModel(recovered).Outcome, StringComparison.Ordinal);
        Assert.Contains("old rejection", await File.ReadAllTextAsync(Path.Combine(directory.Path, plan.Id.ToString("N"), "000000.rejections.jsonl")), StringComparison.Ordinal);
        Assert.Equal([item.Message.Properties.MessageId, item.Message.Properties.MessageId], workspace.SentMessages.Select(send => send.Message.Properties.MessageId));
    }

    [Fact]
    public async Task BugCycleOne_ReplayDestinationClearsOnRealEnvironmentSwitchEvenWithSameQueueName()
    {
        var first = CreateProfile("First", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var second = CreateProfile("Second", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("target", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))]) };
        await using var vm = CreateViewModel(new FakeProfileRepository([first, second], first.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);
        vm.SelectedProfile = vm.Profiles.Single(item => item.Id == second.Id);
        await vm.ConnectCommand.ExecuteAsync();
        Assert.Equal(second.Id, vm.ConnectedProfileId);
        Assert.Null(vm.ReplayDestination);
    }
}
