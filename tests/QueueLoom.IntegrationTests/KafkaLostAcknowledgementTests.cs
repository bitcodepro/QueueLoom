using Confluent.Kafka;
using Confluent.Kafka.Admin;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.IntegrationTests;

/// <summary>
/// A real broker writes a record whose acknowledgement never reaches the client (the proxy drops the connection
/// instead), so librdkafka sends the same request again. Only the idempotent producer's sequence number lets the broker
/// recognise that retry; the compatibility mode writes the record twice, which shows the acknowledgement really was lost.
/// </summary>
public sealed class KafkaLostAcknowledgementTests
{
    [EmulatorFact(Emulators.Kafka)]
    public Task TheIdempotentProducerWritesTheRecordOnce() => SendWithALostAcknowledgementAsync(compatibilityMode: false, expectedCopies: 1);

    [EmulatorFact(Emulators.Kafka)]
    public Task TheCompatibilityModeCanWriteItTwice() => SendWithALostAcknowledgementAsync(compatibilityMode: true, expectedCopies: 2);

    private static async Task SendWithALostAcknowledgementAsync(bool compatibilityMode, int expectedCopies)
    {
        var topic = Emulators.Unique("lost-ack");
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Emulators.KafkaServers }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
        try
        {
            await using var proxy = new KafkaAckDropProxy(Emulators.KafkaServers);
            await using (var workspace = new KafkaWorkspace(new InMemorySecretVault()))
            {
                var profile = ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development,
                        new AuthenticationSettings(AuthenticationKind.KafkaNone), accessMode: ProfileAccessMode.ReadWrite) with
                {
                    Provider = MessagingProvider.Kafka,
                    Kafka = new KafkaSettings(proxy.BootstrapServers, NonIdempotentProducer: compatibilityMode)
                };
                await workspace.ConnectAsync(profile);
                await SendAsync(workspace, topic, "warm-up"); // Metadata and, when idempotent, the producer ID are in place.

                proxy.DropNextProduceResponse();
                await SendAsync(workspace, topic, "order-1").WaitAsync(TimeSpan.FromSeconds(60));
                Assert.True(proxy.ProduceResponseDropped);
            }

            Assert.Equal(expectedCopies, ReadAll(topic).Count(value => value == "order-1"));
        }
        finally
        {
            await admin.DeleteTopicsAsync([topic]);
        }
    }

    private static Task SendAsync(KafkaWorkspace workspace, string topic, string body) =>
        workspace.SendMessageAsync(new SendMessageRequest(ServiceBusEntityReference.Queue(topic),
            new MessageDraft(new EditableMessageBody(body, MessageBodyFormat.Text), EditableMessageProperties.Empty)));

    private static List<string> ReadAll(string topic)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = Emulators.KafkaServers, GroupId = Guid.NewGuid().ToString("N"),
            AutoOffsetReset = AutoOffsetReset.Earliest, EnablePartitionEof = true, EnableAutoCommit = false
        }).Build();
        consumer.Assign(new TopicPartitionOffset(topic, 0, Offset.Beginning));
        var values = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result is null) continue;
            if (result.IsPartitionEOF) break;
            values.Add(result.Message.Value);
        }
        return values;
    }
}
