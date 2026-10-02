using System.Reflection;
using Confluent.Kafka;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

public sealed class CycleTwoKafkaPagingRegressionTests
{
    [Theory]
    [InlineData(true, 1)]
    [InlineData(false, 1)]
    [InlineData(true, 2)]
    [InlineData(false, 2)]
    public async Task CycleTwoKafka_NewestPagesCountSurvivingRecordsRatherThanOffsetSpan(bool sparse, int partitions)
    {
        var offsets = sparse ? Enumerable.Range(0, 100).Select(i => (long)i).Append(199).ToArray()
            : Enumerable.Range(0, 101).Select(i => (long)i).ToArray();
        var broker = new ConsumerFixture(offsets, partitions);
        var first = await Read(broker, new BrowseStart(BrowseStartKind.Newest), 100);
        Assert.Equal(100, first.Count);
        var positions = first.GroupBy(m => m.Message.Position!.Value.Partition).ToDictionary(g => g.Key, g => g.Min(m => m.Message.Position!.Value.Offset));
        var next = await Read(broker, new BrowseStart(BrowseStartKind.Newest) with { Positions = positions }, 100);
        var all = first.Concat(next).Select(m => m.LeaseHandle).ToArray();
        Assert.Equal(all.Length, all.Distinct().Count());
        if (partitions == 1)
        {
            Assert.Single(next);
            Assert.Equal(0, next[0].Message.Position!.Value.Offset);
            Assert.Equal(offsets.Length, all.Length);
        }
        Assert.Empty(broker.Commits);
    }

    [Fact]
    public async Task CycleTwoKafka_GenuineEndAndCallerCancellationRemainRecognized()
    {
        var broker = new ConsumerFixture([2, 9], 1);
        Assert.Equal(2, (await Read(broker, new BrowseStart(BrowseStartKind.Newest), 100)).Count);
        Assert.Empty(await Read(new ConsumerFixture([], 1), new BrowseStart(BrowseStartKind.Newest), 100));
        using var cancellation = new CancellationTokenSource(); cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Read(broker, new BrowseStart(BrowseStartKind.Newest), 100, cancellation.Token));
    }

    private static async Task<IReadOnlyList<LeasedMessage>> Read(ConsumerFixture fixture, BrowseStart start, int count, CancellationToken token = default)
    {
        await using var owner = new KafkaWorkspace(new EmptyVault());
        var type = typeof(KafkaWorkspace).GetNestedType("KafkaChannel", BindingFlags.NonPublic)!;
        var topic = new KafkaTopicInfo("isolated", Enumerable.Range(0, fixture.Partitions).ToArray(), fixture.High);
        var channel = (ILeasedMessageChannel)Activator.CreateInstance(type, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            null, [owner, topic, ServiceBusEntityReference.Queue("isolated"), ServiceBusSubQueue.Active, null], null)!;
        type.GetMethod("StartAt")!.Invoke(channel, [start, count]);
        var assignments = new List<TopicPartitionOffset>();
        var ends = (Dictionary<int, long>)type.GetField("_end", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(channel)!;
        // Inject only the SDK consumer. Use the production Range for each assignment, including continuation.
        for (var partition = 0; partition < fixture.Partitions; partition++)
        {
            var range = ((long From, long To))type.GetMethod("Range", BindingFlags.NonPublic | BindingFlags.Instance)!.Invoke(channel, [partition, 0L, fixture.High, null])!;
            ends[partition] = range.To;
            assignments.Add(new("isolated", partition, range.From));
        }
        var consumer = DispatchProxy.Create<IConsumer<byte[]?, byte[]?>, ConsumerProxy>();
        ((ConsumerProxy)(object)consumer).Fixture = fixture;
        consumer.Assign(assignments);
        type.GetField("_consumer", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(channel, consumer);
        try
        {
            var result = new List<LeasedMessage>();
            while (result.Count < count)
            {
                var page = await channel.ReceiveAsync(Math.Min(100, count - result.Count), token);
                if (page.Count == 0) break;
                result.AddRange(page);
            }
            return result;
        }
        finally { ((IDisposable)channel).Dispose(); }
    }

    public class ConsumerProxy : DispatchProxy
    {
        internal ConsumerFixture Fixture { get; set; } = null!;
        protected override object? Invoke(MethodInfo? method, object?[]? args) => Fixture.Invoke(method!.Name, args!);
    }
    internal sealed class ConsumerFixture(long[] offsets, int partitions)
    {
        public int Partitions => partitions;
        public long High => offsets.Length == 0 ? 0 : offsets.Max() + 1;
        private readonly Dictionary<int, long> _positions = [];
        public List<string> Commits { get; } = [];
        public object? Invoke(string method, object?[] args)
        {
            if (method == "Assign")
            {
                _positions.Clear();
                foreach (var assignment in (IEnumerable<TopicPartitionOffset>)args[0]!) _positions[assignment.Partition.Value] = assignment.Offset.Value;
            }
            else if (method == "Seek")
            {
                var position = (TopicPartitionOffset)args[0]!; _positions[position.Partition.Value] = position.Offset.Value;
            }
            else if (method == "QueryWatermarkOffsets") return new WatermarkOffsets(0, High);
            else if (method == "Consume")
            {
                foreach (var (partition, position) in _positions.ToArray())
                {
                    var remaining = offsets.Where(offset => offset >= position).ToArray();
                    if (remaining.Length == 0) { _positions.Remove(partition); return new ConsumeResult<byte[]?, byte[]?> { Topic = "isolated", Partition = partition, Offset = High, IsPartitionEOF = true }; }
                    var offset = remaining[0]; _positions[partition] = offset + 1;
                    return new ConsumeResult<byte[]?, byte[]?> { Topic = "isolated", Partition = partition, Offset = offset,
                        Message = new() { Value = "body"u8.ToArray(), Timestamp = new Timestamp(DateTime.UnixEpoch.AddSeconds(offset)), Headers = new Headers() } };
                }
                return null;
            }
            else if (method is "Commit" or "StoreOffset") Commits.Add(method);
            else if (method is not ("Close" or "Dispose")) throw new InvalidOperationException("Unexpected SDK call: " + method);
            return null;
        }
    }
    private sealed class EmptyVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string value, CancellationToken token = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
    }
}
