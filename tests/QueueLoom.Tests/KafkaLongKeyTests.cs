using System.Text;
using Confluent.Kafka;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class KafkaLongKeyTests
{
    // A Kafka Connect JSON key (schema + payload) is routinely longer than Azure's 128-character PartitionKey limit.
    private static readonly string LongKey = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"field\":\"order_id\"},{\"type\":\"string\",\"field\":\"tenant\"}],\"optional\":false,\"name\":\"com.example.orders.OrderKey\"},\"payload\":{\"order_id\":4200000017,\"tenant\":\"emea-west\"}}";

    private static BrowsedMessage BrowseKafka(byte[] key) => KafkaMessageMapper.FromKafka(
        new ConsumeResult<byte[]?, byte[]?> { Topic = "orders.DLT", Partition = new Partition(0), Offset = new Offset(7),
            Message = new Message<byte[]?, byte[]?> { Key = key, Value = "{\"id\":1}"u8.ToArray(), Headers = [] } },
        ServiceBusEntityReference.Queue("orders.DLT"), ServiceBusSubQueue.DeadLetter);

    [Fact]
    public void BrowsedKafkaMessageWithLongKeyIsAValidDraft()
    {
        Assert.True(LongKey.Length > MessageDraftValidator.MaxMessageIdentifierLength);
        var draft = BrowseKafka(Encoding.UTF8.GetBytes(LongKey)).CreateDraft();

        var validation = MessageDraftValidator.Validate(draft);

        Assert.True(validation.IsValid, string.Join(" ", validation.Errors.Select(e => e.Message)));
        // The key still travels unchanged to Kafka.
        Assert.Equal(LongKey, Encoding.UTF8.GetString(KafkaMessageMapper.ToKafka(draft).Key!));
    }

    [Fact]
    public async Task DurableKafkaCopyOfALongKeyMessageCanBePrepared()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var profile = ViewModelStateTests.CreateProfile("Kafka", EnvironmentKind.Development, ProfileAccessMode.ReadWrite)
            with { Provider = MessagingProvider.Kafka };
        var message = BrowseKafka(Encoding.UTF8.GetBytes(LongKey));
        var items = new[] { new ResendItem(message, ServiceBusEntityReference.Queue("orders"), message.CreateDraft()) };

        var error = await Record.ExceptionAsync(() => store.CreateResendAsync(profile.Id, items, ResendMode.Copy, 50,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Immediate resend", default));

        Assert.Null(error);
    }
}
