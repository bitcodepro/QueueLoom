using Confluent.Kafka;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// The ".DLT" topic created with a topic always had one partition. Spring's DeadLetterPublishingRecoverer writes a
// failed record to the same partition number of "<topic>.DLT", so a failure on partition 3 of a six-partition topic
// could not be dead-lettered. The dead-letter topic now gets the partitions of its topic.
public sealed class KafkaDeadLetterPartitionsTests
{
    [Theory]
    [InlineData(6, 6)]
    [InlineData(null, 1)]  // the topic itself is created with one partition when none is given
    public void TheDeadLetterTopic_HasThePartitionsOfItsTopic(int? partitions, int expected)
    {
        var (topic, deadLetter) = KafkaWorkspace.TopicSpecifications(new QueueDefinition("orders", new QueueSettings(Partitions: partitions)));

        Assert.Equal(expected, topic.NumPartitions);
        Assert.Equal("orders.DLT", deadLetter.Name);
        Assert.Equal(topic.NumPartitions, deadLetter.NumPartitions);
    }
}

// Growing a topic grows its dead-letter topics. A retry after a failed dead-letter growth must still repair it, an
// unreadable dead-letter topic must not pass as absent, and a dead-letter topic of another topic is never touched.
public sealed class KafkaDeadLetterGrowthTests
{
    private sealed class FakeAdmin : KafkaWorkspace.IPartitionAdmin
    {
        public Dictionary<string, int> Partitions { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, ErrorCode> Errors { get; } = new(StringComparer.Ordinal);
        public HashSet<string> Failing { get; } = new(StringComparer.Ordinal);
        public List<(string Topic, int IncreaseTo)> Calls { get; } = [];

        public Metadata GetMetadata(TimeSpan timeout) => new([],
            Partitions.Select(pair => new TopicMetadata(pair.Key,
                Enumerable.Range(0, pair.Value).Select(id => new PartitionMetadata(id, 1, [1], [1], new Error(ErrorCode.NoError))).ToList(),
                new Error(Errors.GetValueOrDefault(pair.Key, ErrorCode.NoError)))).ToList(), 1, "broker");

        public Task CreatePartitionsAsync(string topic, int increaseTo)
        {
            Calls.Add((topic, increaseTo));
            if (Failing.Contains(topic))
            {
                throw new KafkaException(new Error(ErrorCode.PolicyViolation, "not allowed"));
            }
            Partitions[topic] = increaseTo;
            return Task.CompletedTask;
        }
    }

    private static Task Grow(FakeAdmin admin, string queue, int partitions, params string[] suffixes) =>
        KafkaWorkspace.GrowPartitionsAsync(admin, queue, partitions, suffixes.Length == 0 ? [".DLT"] : suffixes, TimeSpan.FromSeconds(1),
            CancellationToken.None);

    [Fact]
    public async Task ARetryWithTheSameTarget_RepairsTheDeadLetterTopic_WithoutGrowingTheTopicAgain()
    {
        var admin = new FakeAdmin { Partitions = { ["orders"] = 3, ["orders.DLT"] = 3 }, Failing = { "orders.DLT" } };
        await Assert.ThrowsAsync<InvalidOperationException>(() => Grow(admin, "orders", 5));
        Assert.Equal(5, admin.Partitions["orders"]);
        Assert.Equal(3, admin.Partitions["orders.DLT"]);

        admin.Failing.Clear();
        admin.Calls.Clear();
        await Grow(admin, "orders", 5);

        Assert.Equal([("orders.DLT", 5)], admin.Calls);
        Assert.Equal(5, admin.Partitions["orders.DLT"]);
    }

    [Fact]
    public async Task AnUnreadableDeadLetterTopic_IsReported_AndLeftAlone()
    {
        var admin = new FakeAdmin { Partitions = { ["orders"] = 3, ["orders.DLT"] = 3 }, Errors = { ["orders.DLT"] = ErrorCode.LeaderNotAvailable } };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Grow(admin, "orders", 5));

        Assert.Contains("orders.DLT", error.Message);
        Assert.Equal([("orders", 5)], admin.Calls);
    }

    [Fact]
    public async Task ADeadLetterTopicOfAnotherTopic_IsNotGrown_WhenTheEndingsOverlap()
    {
        var admin = new FakeAdmin { Partitions = { ["orders"] = 3, ["orders-retry"] = 3, ["orders-retry-dlt"] = 3, ["orders-dlt"] = 3 } };

        await Grow(admin, "orders", 5, "-dlt", "-retry-dlt");

        Assert.Equal(3, admin.Partitions["orders-retry-dlt"]);
        Assert.Equal(5, admin.Partitions["orders-dlt"]);
    }

    [Fact]
    public async Task ADeadLetterTopicWithMorePartitions_IsNeverShrunk()
    {
        var admin = new FakeAdmin { Partitions = { ["orders"] = 3, ["orders.DLT"] = 8 } };

        await Grow(admin, "orders", 5);

        Assert.Equal([("orders", 5)], admin.Calls);
    }
}
