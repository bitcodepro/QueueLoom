using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Monitoring;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("0")]
    [InlineData("1.5")]
    [InlineData("10001")]
    public async Task InvalidPurgeLimitCannotDeleteMessages(string? input)
    {
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var source = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace { Snapshots = { [profile.Id] = Snapshot(profile.Id, new DeadLetterEntitySnapshot(source, 5)) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync(); await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();
        vm.SelectedDlqSource = Assert.Single(vm.FilteredDeadLetterSources);
        vm.PurgeLimitPerSource = input is null ? null : decimal.Parse(input, System.Globalization.CultureInfo.InvariantCulture);
        Assert.False(vm.HasValidPurgeLimit);
        await vm.PurgeSelectedDeadLettersCommand.ExecuteAsync();
        Assert.Empty(workspace.PurgeRequests);
        vm.PurgeLimitPerSource = 100;
        Assert.True(vm.HasValidPurgeLimit);
    }

    [Fact]
    public async Task PurgeDeniedConfirmationDoesNotChangeMessages()
    {
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var source = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace { Snapshots = { [profile.Id] = Snapshot(profile.Id, new DeadLetterEntitySnapshot(source, 5)) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            new FakeDialogService { ConfirmResult = false });
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync(); await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();
        vm.SelectedDlqSource = Assert.Single(vm.FilteredDeadLetterSources);
        await vm.PurgeSelectedDeadLettersCommand.ExecuteAsync();
        Assert.Empty(workspace.PurgeRequests);
    }

    [Fact]
    public async Task PagingContinuesWithoutDuplicatesAndStopsAtDisplayLimit()
    {
        var profile = CreateProfile("Test", EnvironmentKind.Test);
        var source = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace
        {
            Snapshots = { [profile.Id] = Snapshot(profile.Id, new DeadLetterEntitySnapshot(source, 1100)) },
            BrowseMessages = Enumerable.Range(1, 1100).Select(i => SearchMessage(source, i, "2026-08-12T10:00:00Z")).ToArray()
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync(); await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();
        vm.SelectedDlqSource = Assert.Single(vm.FilteredDeadLetterSources);
        await vm.BrowseDlqSourceCommand.ExecuteAsync();
        Assert.Equal(100, vm.Messages.Count);
        for (var i = 0; i < 9; i++) await vm.LoadMoreMessagesCommand.ExecuteAsync();
        Assert.Equal(1000, vm.Messages.Count);
        Assert.Equal(1000, vm.Messages.Select(m => m.SequenceNumber).Distinct().Count());
        Assert.False(vm.CanLoadMoreMessages);
        Assert.Equal(901, workspace.BrowseRequests.Last().FromSequenceNumber);
    }

    [Fact]
    public async Task ReplayResumeSkipsAcknowledgedMessagesAfterCancellationAndReopen()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        var draft = new MessageDraft(new EditableMessageBody("{}", MessageBodyFormat.Json));
        var plan = await store.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("target"),
            new[] { (draft, "first"), (draft, "second") }, false, 50, default);
        using var cancellation = new CancellationTokenSource();
        workspace.OnSend = () => cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.RunAsync(plan, workspace, () => true, null, cancellation.Token));
        Assert.Single(workspace.SentMessages);
        workspace.OnSend = null;
        var reopened = new BatchReplayStore(directory.Path);
        var result = await reopened.RunAsync(reopened.Latest(profile.Id)!, workspace, () => true, null, default);
        Assert.Equal(2, result.Sent);
        Assert.Equal(2, workspace.SentMessages.Count);
        Assert.Equal(2, workspace.SentMessages.Select(m => m.Message.Properties.MessageId).Distinct().Count());
    }

    [Fact]
    public async Task UncertainDeliveryBlocksResumeAndWrongEnvironmentCannotSend()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace(); await workspace.ConnectAsync(profile);
        var draft = new MessageDraft(new EditableMessageBody("hello", MessageBodyFormat.Text));
        var plan = await store.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("target"), new[] { (draft, "test") }, false, 50, default);
        workspace.OnSend = () => throw new IOException("Connection lost after write");
        await Assert.ThrowsAsync<IOException>(() => store.RunAsync(plan, workspace, () => true, null, default));
        workspace.OnSend = null;
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunAsync(plan, workspace, () => true, null, default));
        Assert.Single(workspace.SentMessages);
        await workspace.ConnectAsync(CreateProfile("Other", EnvironmentKind.Test));
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunAsync(plan, workspace, () => true, null, default));
        Assert.Single(workspace.SentMessages);
    }
}
