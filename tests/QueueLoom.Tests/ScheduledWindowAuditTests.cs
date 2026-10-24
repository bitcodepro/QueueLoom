using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ScheduledResendConsumedOrCancelledInOneWindowCannotSendFromAnother(bool cancel)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = CreateProfile("Two windows", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var storeA = new JsonScheduledResendStore(paths);
        var storeB = new JsonScheduledResendStore(paths);
        storeA.Save([DueJob(profile)]);
        var a = new FakeWorkspace(); var b = new FakeWorkspace();
        await using var vmA = await Window(storeA, a); await using var vmB = await Window(storeB, b);
        if (cancel) await vmA.CancelScheduledAsync(Assert.Single(vmA.ScheduledResends));
        else await vmA.RunDueScheduledResendsAsync();
        await vmB.RunDueScheduledResendsAsync();
        Assert.Empty(b.SentMessages);
        Assert.Equal(cancel ? 0 : 1, a.SentMessages.Count);
        Assert.Empty(storeB.Load());
        Assert.Empty(vmB.ScheduledResends);
        async Task<MainWindowViewModel> Window(IScheduledResendStore store, FakeWorkspace broker)
        {
            var vm = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(), broker,
                new FakeDialogService(), scheduledResends: store);
            await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync(); return vm;
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StaleScheduledWindowDoesNotEraseANewJobAddedElsewhere(bool cancel)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = CreateProfile("Two windows", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var old = DueJob(profile); var added = DueJob(profile) with { DueAt = DateTimeOffset.UtcNow.AddHours(1) };
        var store = new JsonScheduledResendStore(paths); store.Save([old]);
        var broker = new FakeWorkspace();
        await using var vm = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(), broker,
            new FakeDialogService(), scheduledResends: store);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        new JsonScheduledResendStore(paths).Save([old, added]);
        if(cancel) await vm.CancelScheduledAsync(Assert.Single(vm.ScheduledResends));
        else await vm.RunDueScheduledResendsAsync();
        Assert.Equal(added.Id, Assert.Single(new JsonScheduledResendStore(paths).Load()).Id);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ScheduledJobWithAnAnnotationThatCannotBeStored_IsCancelledOrRunFromTheList(bool cancel)
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        var (vm, broker, dialogs) = await AnnotatedWindowAsync(store);
        await using var _ = vm;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddHours(1) };
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        var row = Assert.Single(vm.ScheduledResends);

        if (cancel) await vm.CancelScheduledAsync(row);
        else await vm.RunScheduledNowAsync(row);

        Assert.Empty(store.Load());
        Assert.Empty(vm.ScheduledResends);
        Assert.Equal(cancel ? 0 : 1, broker.SentMessages.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ScheduledJobChangedOnDisk_StaysListedWhenItCannotBeClaimed(bool cancel)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var store = new JsonScheduledResendStore(paths);
        var (vm, broker, dialogs) = await AnnotatedWindowAsync(store);
        await using var _ = vm;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddHours(1) };
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        var row = Assert.Single(vm.ScheduledResends);
        var changed = Assert.Single(store.Load()) with { DestinationDisplay = "changed elsewhere" };
        new JsonScheduledResendStore(paths).Save([changed]);

        if (cancel) await vm.CancelScheduledAsync(row);
        else await vm.RunScheduledNowAsync(row);

        Assert.Equal("changed elsewhere", Assert.Single(store.Load()).DestinationDisplay);
        Assert.Equal("changed elsewhere", Assert.Single(vm.ScheduledResends).Resend.DestinationDisplay);
        Assert.Empty(broker.SentMessages);
        Assert.Contains("changed in another window", vm.StatusText, StringComparison.Ordinal);
    }

    private static async Task<(MainWindowViewModel ViewModel, FakeWorkspace Workspace, FakeDialogService Dialogs)> AnnotatedWindowAsync(
        IScheduledResendStore store)
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var queue = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)));
        var message = SearchMessage(queue.Reference, 2, "2026-08-12T10:00:00Z") with
        {
            AzureMessageAnnotations = new Dictionary<string, object?> { ["kept"] = "yes", ["odd"] = new object() }
        };
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [queue]),
            SearchMatches = { [profile.Id] = [message] }
        };
        workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow, [new DeadLetterEntitySnapshot(queue.Reference, 1)]);
        var dialogs = new FakeDialogService { ConfirmResult = true };
        var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs, scheduledResends: store);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();
        vm.DeadLetterSearchQuery = "correlation-42";
        await vm.SearchDeadLettersCommand.ExecuteAsync();
        vm.AreAllMessagesMarked = true;
        return (vm, workspace, dialogs);
    }

    private static ScheduledResend DueJob(ServiceBusProfile profile)
    {
        var source = ServiceBusEntityReference.Queue("orders");
        return new ScheduledResend(Guid.NewGuid(), profile.Id, profile.Name, DateTimeOffset.UtcNow, DateTimeOffset.UnixEpoch,
            ResendMode.Copy, 0, "orders", [new ScheduledResendItem(source, ServiceBusSubQueue.DeadLetter, 1, "original",
                source, new MessageDraft(new EditableMessageBody("scheduled", MessageBodyFormat.Text), new EditableMessageProperties(MessageId: Guid.NewGuid().ToString("N"))))])
            { ConfigurationIdentity = ScheduledResend.IdentityFor(profile) };
    }
}
