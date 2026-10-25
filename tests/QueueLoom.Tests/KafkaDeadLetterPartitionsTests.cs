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
