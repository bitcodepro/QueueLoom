using System.Reflection;
using Confluent.Kafka;
using QueueLoom.App.ViewModels;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// When one partition of a topic had no leader, reading its watermarks stopped there: the partitions after it were not
// read, and a consumer group's lag counted every unread partition as caught up. The topic then showed a numeric lag
// (often 0) as if it were complete. Every readable partition is now read, and a group whose committed partitions
// include an unread one shows its lag as unknown.
public sealed class KafkaPartialWatermarkLagTests
{
    [Fact]
    public void APartitionWithoutLeader_LeavesTheOtherPartitionsRead_AndTheGroupLagUnknown()
    {
        var consumer = DispatchProxy.Create<IConsumer<byte[]?, byte[]?>, ConsumerProxy>();
        ((ConsumerProxy)(object)consumer).Leaderless = 1;

        var info = KafkaWorkspace.ReadTopicInfo(consumer, "orders", [0, 1, 2], CancellationToken.None);

        Assert.NotNull(info.CountError);
        Assert.Equal([0, 2], info.Ends.Keys.Order());
        var committed = new[] { 0, 1, 2 }.Select(partition =>
            new TopicPartitionOffsetError("orders", partition, 10, new Error(ErrorCode.NoError))).ToList();
        Assert.Null(KafkaWorkspace.LagOf(info, committed));
        // A group that committed only on readable partitions still has a known lag.
        Assert.Equal(180, KafkaWorkspace.LagOf(info, [committed[0], committed[2]]));
    }

    [Fact]
    public void AnUnknownLag_IsShownAsUnknown_NotAsCaughtUp()
    {
        var unknown = new Core.ServiceBus.ConsumerActivity(null, [new("billing", null, "Stable")]);
        Assert.Null(unknown.MaximumLag);
        Assert.Equal("lag unknown", unknown.Summary);
        Assert.Contains("billing: lag unknown", unknown.Details, StringComparison.Ordinal);

        var mixed = new Core.ServiceBus.ConsumerActivity(null, [new("billing", null), new("audit", 4)]);
        Assert.Equal(4, mixed.MaximumLag);
        Assert.Equal("2 groups, lag up to 4 (audit) · some lag unknown", mixed.Summary);

        var orders = new KafkaTopologyIndex([new KafkaTopicInfo("orders", [0], 3) { Groups = unknown.Groups }],
            Core.Profiles.KafkaSettings.DefaultDeadLetterSuffixes).ToTopology(DateTimeOffset.UnixEpoch).Queues.Single();
        var row = new EntityItemViewModel(orders.Reference, orders.Runtime, orders.Status, false, 0, consumers: orders.Consumers);
        Assert.True(row.HasConsumerLag);
    }

    public class ConsumerProxy : DispatchProxy
    {
        internal int Leaderless { get; set; } = -1;

        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == nameof(IConsumer<byte[], byte[]>.QueryWatermarkOffsets))
            {
                var partition = (TopicPartition)args![0]!;
                if (partition.Partition.Value == Leaderless)
                {
                    throw new KafkaException(ErrorCode.LeaderNotAvailable);
                }
                return new WatermarkOffsets(0, 100);
            }
            if (method.Name == nameof(IDisposable.Dispose)) return null;
            throw new NotSupportedException(method.Name);
        }
    }
}
