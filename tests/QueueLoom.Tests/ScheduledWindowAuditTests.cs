using QueueLoom.App.ViewModels;
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

    private static ScheduledResend DueJob(ServiceBusProfile profile)
    {
        var source = ServiceBusEntityReference.Queue("orders");
        return new ScheduledResend(Guid.NewGuid(), profile.Id, profile.Name, DateTimeOffset.UtcNow, DateTimeOffset.UnixEpoch,
            ResendMode.Copy, 0, "orders", [new ScheduledResendItem(source, ServiceBusSubQueue.DeadLetter, 1, "original",
                source, new MessageDraft(new EditableMessageBody("scheduled", MessageBodyFormat.Text), new EditableMessageProperties(MessageId: Guid.NewGuid().ToString("N"))))])
            { ConfigurationIdentity = ScheduledResend.IdentityFor(profile) };
    }
}
