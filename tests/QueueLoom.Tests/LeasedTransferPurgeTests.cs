using System.Reflection;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class LeasedTransferPurgeTests
{
    // (d) A purge of only a transfer dead-letter queue on SQS is dropped silently: no source, no error, "0 deleted".
    [Fact]
    public async Task PurgeOfOnlyATransferDlq_IsNotReportedAsAnEmptySuccess()
    {
        using var directory = new TemporaryDirectory();
        var workspace = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault(),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        var queue = new AwsQueueInfo("q", "http://localhost/q", "arn:aws:sqs:us-east-1:123:q", false, 0, 0, 0, null, null, null);
        var index = new AwsTopologyIndex([queue], []);
        var profile = ViewModelStateTests.CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.AmazonSqsSns
        };
        Set(typeof(AwsSqsSnsWorkspace), "_index", index);
        Set(typeof(LeasedMessagingWorkspace), "_profile", profile);
        Set(typeof(LeasedMessagingWorkspace), "_connectionState", WorkspaceConnectionState.Connected);
        Set(typeof(LeasedMessagingWorkspace), "_cachedTopology", index.ToTopology(DateTimeOffset.UtcNow));

        // Exactly what the MCP purge_dead_letters tool sends for subQueue 'transfer-dlq'.
        var request = new DeadLetterPurgeRequest(
            [new DeadLetterPurgeTarget(ServiceBusEntityReference.Queue("q"), ServiceBusSubQueue.TransferDeadLetter)],
            batchSize: 20, maximumMessagesPerSubQueue: 100);

        DeadLetterPurgeResult? result = null;
        var error = await Record.ExceptionAsync(async () => result = await workspace.PurgeDeadLettersAsync(request));

        Assert.True(error is not null || result!.HasFailures,
            $"Sources: {result?.Sources.Count}, deleted: {result?.DeletedCount}, failures: {result?.HasFailures}");
        await workspace.DisposeAsync();

        void Set(Type type, string field, object value) =>
            type.GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, value);
    }
}
