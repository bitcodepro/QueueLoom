using QueueLoom.Core.Profiles;
using System.Reflection;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

// After a purge the monitor's previous reading for a source was set to an exact zero even when the counter after the
// purge still showed messages or could not be read. The next check then reported those as new dead letters, against a
// zero that was never measured. Only a source proven empty starts from zero now.
public sealed class PurgeBaselineTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    [Theory]
    [InlineData(false, 0L)]
    [InlineData(true, null)]
    public async Task OnlyASourceProvenEmptyStartsTheNextCheckFromZero(bool verificationPending, long? expectedPrevious)
    {
        await using var azure = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        // A reading from before the purge.
        Snapshot(azure, 12);

        azure.RecordPurgeBaselines([new DeadLetterPurgeSourceResult(Orders, ServiceBusSubQueue.DeadLetter, 12,
            VerificationPending: verificationPending)]);
        var next = Snapshot(azure, 3);

        Assert.Equal(expectedPrevious, next.PreviousCount);
    }

    [Fact]
    public async Task AFailedPurgeKeepsTheLastReading()
    {
        await using var azure = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        Snapshot(azure, 12);

        azure.RecordPurgeBaselines([new DeadLetterPurgeSourceResult(Orders, ServiceBusSubQueue.DeadLetter, 0, Error: "denied")]);

        Assert.Equal(12, Snapshot(azure, 12).PreviousCount);
    }

    private static DeadLetterEntitySnapshot Snapshot(AzureServiceBusWorkspace azure, long count) =>
        (DeadLetterEntitySnapshot)typeof(AzureServiceBusWorkspace).GetMethod("CreateSnapshot", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(azure, [Orders, ServiceBusSubQueue.DeadLetter, count, null, DeadLetterCountQuality.Exact])!;
}

public sealed partial class ViewModelStateTests
{
    // The window keeps its own previous counts for the dead-letter list. After a purge whose counter still showed
    // messages (or could not be read) it recorded an exact zero too, so the next scan showed growth (+3) from a zero
    // that was never measured. Such a source now compares with "at least none", which shows the change as unknown.
    [Theory]
    [InlineData("verified", "+3")]
    [InlineData("pending", "unknown")]
    [InlineData("failed", "−9")] // the display uses the minus sign U+2212
    public async Task PurgeBaseline_TheNextScanComparesOnlyWithWhatThePurgeProved(string purge, string expectedDelta)
    {
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var source = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace
        {
            Snapshots = { [profile.Id] = Snapshot(profile.Id, new DeadLetterEntitySnapshot(source, 12)) },
            PurgeResultFor = target => purge switch
            {
                "pending" => new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, 12, VerificationPending: true),
                "failed" => new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, 0, Error: "denied"),
                _ => new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, 12)
            }
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            new FakeDialogService { ConfirmResult = true });
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();
        vm.SelectedDlqSource = Assert.Single(vm.FilteredDeadLetterSources);

        await vm.PurgeSelectedDeadLettersCommand.ExecuteAsync();
        Assert.Single(workspace.PurgeRequests);
        workspace.Snapshots[profile.Id] = Snapshot(profile.Id, new DeadLetterEntitySnapshot(source, 3));
        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();

        var row = Assert.Single(vm.DeadLetterSources, item => item.Entity == source);
        Assert.Equal(expectedDelta, row.Delta);
    }
}
