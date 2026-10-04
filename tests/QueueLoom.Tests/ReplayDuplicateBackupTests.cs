using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // (c) "Backed up, but the deletion failed" followed by a successful retry leaves two backups of one message.
    // Restoring the filtered backups must send that message once, not twice.
    [Fact]
    public async Task RestoreFilteredBackups_SendsAMessageBackedUpTwiceOnlyOnce()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var source = ServiceBusEntityReference.Queue("orders");
        DeadLetterBackupSummary Backup(string file, int minute) =>
            new(Path.Combine(Path.GetTempPath(), "backups", file), profile.Id, profile.Name, profile.Environment.ToString(), null,
                source, ServiceBusSubQueue.DeadLetter, 7, "m-7", null, null, null, DateTimeOffset.UnixEpoch.AddMinutes(minute), 10);
        var backups = new ListBackupRepository(Backup("first-attempt.json", 1), Backup("retry.json", 2));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("target", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))])
        };
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs, backups,
            replayStore: new BatchReplayStore(directory.Path));
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);
        await vm.RefreshBackupsCommand.ExecuteAsync();
        Assert.Equal(2, vm.FilteredBackupMessages.Count);

        await vm.RestoreFilteredBackupsCommand.ExecuteAsync();

        Assert.Single(workspace.SentMessages);
    }

    // RabbitMQ derives the sequence number from the publisher's Message ID, which independent deliveries can share;
    // a backed-up purge saves both, and restoring must send both.
    [Fact]
    public async Task RestoreFilteredBackups_KeepsRabbitMqDeliveriesThatShareAMessageId()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Rabbit", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.RabbitMq,
            RabbitMq = new RabbitMqSettings("broker.invalid", "guest")
        };
        var source = ServiceBusEntityReference.Queue("orders");
        DeadLetterBackupSummary Backup(string file, int minute) =>
            new(Path.Combine(Path.GetTempPath(), "backups", file), profile.Id, profile.Name, profile.Environment.ToString(), null,
                source, ServiceBusSubQueue.DeadLetter, 7, "shared-id", null, null, null, DateTimeOffset.UnixEpoch.AddMinutes(minute), 10);
        var backups = new ListBackupRepository(Backup("first-delivery.json", 1), Backup("second-delivery.json", 2));
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("target", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))])
        };
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs, backups,
            replayStore: new BatchReplayStore(directory.Path));
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);
        await vm.RefreshBackupsCommand.ExecuteAsync();
        Assert.Equal(2, vm.FilteredBackupMessages.Count);

        await vm.RestoreFilteredBackupsCommand.ExecuteAsync();

        Assert.Equal(2, workspace.SentMessages.Count);
    }
}
