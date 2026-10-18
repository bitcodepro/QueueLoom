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
