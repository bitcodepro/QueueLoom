using System.Reflection;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

/// <summary>
/// Bug 6: ChangeMessageVisibilityBatch answers per entry. Entries it reports failed are retried (only those), later
/// batches are still released, and what stays unreleased is reported instead of silently dropped.
/// </summary>
public sealed class SqsReleaseTests
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    [Fact]
    public async Task ARetryableFailedEntryIsRetriedAloneAndLaterBatchesAreReleased()
    {
        var client = new ReleasingClient(failFirstTimes: 1, code: "InternalError");
        var channel = Channel(client);

        await channel.ReleaseAsync(Messages(12), CancellationToken.None);

        Assert.Equal([10, 1, 2], client.Batches.Select(batch => batch.Count));
        Assert.Equal(["m-1"], client.Batches[1]);
        Assert.Equal(12, client.Visible.Count);
    }

    [Fact]
    public async Task AnEntryThatKeepsFailingIsReportedAfterEveryBatchWasTried()
    {
        var client = new ReleasingClient(failFirstTimes: int.MaxValue, code: "InternalError");
        var channel = Channel(client);

        var error = await Assert.ThrowsAsync<IOException>(() => channel.ReleaseAsync(Messages(12), CancellationToken.None));

        Assert.Contains("1 message(s)", error.Message, StringComparison.Ordinal);
        Assert.Contains("reappear when their visibility timeout ends", error.Message, StringComparison.Ordinal);
        Assert.Equal(["m-10", "m-11"], client.Batches[^1]); // the next batch was still released
        Assert.Equal(11, client.Visible.Count);
    }

    // An expired receipt handle (or a message no longer in flight) does not mean the message is still hidden.
    [Theory]
    [InlineData("ReceiptHandleIsInvalid")]
    [InlineData("AWS.SimpleQueueService.MessageNotInflight")]
    public async Task AnExpiredHandleNeedsNoRetryAndIsNotAnError(string code)
    {
        var client = new ReleasingClient(failFirstTimes: int.MaxValue, code: code, senderFault: true);
        var channel = Channel(client);

        await channel.ReleaseAsync(Messages(3), CancellationToken.None);

        Assert.Single(client.Batches);
    }

    private static List<LeasedMessage> Messages(int count) => Enumerable.Range(0, count).Select(index => new LeasedMessage(
        new BrowsedMessage(ServiceBusEntityReference.Queue("q"), ServiceBusSubQueue.DeadLetter, index, "x"u8.ToArray(),
            new EditableMessageProperties(MessageId: $"m-{index}")), $"m-{index}", true)).ToList();

    private static ILeasedMessageChannel Channel(ReleasingClient client)
    {
        var owner = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(AwsSqsSnsWorkspace).GetField("_sqs", Any)!.SetValue(owner, client);
        var queue = new AwsQueueInfo("q", "http://localhost/q", "arn:aws:sqs:us-east-1:123:q", false, 0, 0, 0, null, null, null);
        var type = typeof(AwsSqsSnsWorkspace).GetNestedType("SqsChannel", BindingFlags.NonPublic)!;
        return (ILeasedMessageChannel)Activator.CreateInstance(type, Any, null,
            [owner, ServiceBusEntityReference.Queue("q"), ServiceBusSubQueue.DeadLetter, queue, null, false], null)!;
    }

    /// <summary>Fails the entry for m-1 the given number of times; everything else becomes visible.</summary>
    private sealed class ReleasingClient(int failFirstTimes, string code, bool senderFault = false)
        : AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost" })
    {
        private int _failures;
        public List<List<string>> Batches { get; } = [];
        public HashSet<string> Visible { get; } = [];

        public override Task<ChangeMessageVisibilityBatchResponse> ChangeMessageVisibilityBatchAsync(
            ChangeMessageVisibilityBatchRequest request, CancellationToken cancellationToken = default)
        {
            Batches.Add(request.Entries.Select(entry => entry.ReceiptHandle).ToList());
            var failed = new List<BatchResultErrorEntry>();
            foreach (var entry in request.Entries)
            {
                if (entry.ReceiptHandle == "m-1" && _failures < failFirstTimes)
                {
                    _failures++;
                    failed.Add(new BatchResultErrorEntry { Id = entry.Id, Code = code, SenderFault = senderFault, Message = "failed" });
                }
                else
                {
                    Visible.Add(entry.ReceiptHandle);
                }
            }
            return Task.FromResult(new ChangeMessageVisibilityBatchResponse { Failed = failed });
        }
    }
}
