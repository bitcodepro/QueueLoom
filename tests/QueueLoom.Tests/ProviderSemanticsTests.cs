using System.Text;
using Amazon.SQS.Model;
using Confluent.Kafka;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Kafka;
using Encoding = System.Text.Encoding;

namespace QueueLoom.Tests;

/// <summary>Provider-specific semantics: SNS envelopes, Kafka partitioning, Pub/Sub reserved attributes, FIFO order.</summary>
public sealed class ProviderSemanticsTests
{
    private const string Envelope = """
        {
          "Type": "Notification",
          "MessageId": "dc1e94d9-56c5-5e96-808d-cc7f68faa162",
          "TopicArn": "arn:aws:sns:us-east-1:123:events",
          "Message": "{\"orderId\":7}",
          "Timestamp": "2026-10-01T10:00:00.000Z",
          "SignatureVersion": "1",
          "Signature": "c2lnbmF0dXJl",
          "SigningCertURL": "https://sns.us-east-1.amazonaws.com/cert.pem",
          "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe",
          "MessageAttributes": {
            "tenant": { "Type": "String", "Value": "contoso" },
            "CorrelationId": { "Type": "String", "Value": "c-1" },
            "count": { "Type": "Number", "Value": "5" },
            "blob": { "Type": "Binary", "Value": "AQID" }
          }
        }
        """;

    // SNS delivers to an SQS subscriber without raw message delivery as a JSON envelope that carries the
    // published body in "Message" and the attributes in "MessageAttributes". A copy sent "back to" the topic
    // must publish that body and those attributes, not the envelope (which SNS would wrap a second time,
    // with no attributes for the subscription filter policies to match).
    [Fact]
    public void SnsEnvelopeReadThroughASubscriptionResendsThePublishedMessageAndAttributes()
    {
        var message = new Message
        {
            MessageId = "sqs-1", ReceiptHandle = "r-1", Body = Envelope,
            Attributes = new() { ["SentTimestamp"] = "1790000000000" }
        };

        var browsed = AwsMessageMapper.FromSqs(message, ServiceBusEntityReference.Subscription("events", "billing"), ServiceBusSubQueue.Active);
        var draft = browsed.CreateDraft();

        Assert.Equal("{\"orderId\":7}", AwsMessageMapper.BodyText(draft));
        // Deleting and moving find the delivery by its SQS message ID.
        Assert.Equal("sqs-1", browsed.Properties.MessageId);
        Assert.Equal("c-1", browsed.Properties.CorrelationId);
        var published = AwsMessageMapper.ToSnsAttributes(draft);
        Assert.Equal("contoso", published["tenant"].StringValue);
        Assert.Equal("c-1", published["CorrelationId"].StringValue);
        Assert.Equal("5", published["count"].StringValue);
        Assert.StartsWith("Number", published["count"].DataType, StringComparison.Ordinal);
        Assert.Equal([1, 2, 3], published["blob"].BinaryValue!.ToArray());
    }

    [Fact]
    public void SnsEnvelopeReadThroughItsSqsQueueStaysTheEnvelopeItsConsumerExpects()
    {
        var message = new Message { MessageId = "sqs-1", ReceiptHandle = "r-1", Body = Envelope };

        var browsed = AwsMessageMapper.FromSqs(message, ServiceBusEntityReference.Queue("billing-queue"), ServiceBusSubQueue.DeadLetter);

        Assert.Equal(Envelope, Encoding.UTF8.GetString(browsed.Body.Span));
        Assert.Empty(browsed.ApplicationProperties);
    }

    [Fact]
    public void RawDeliveryThroughASubscriptionIsNotMistakenForAnEnvelope()
    {
        var body = """{"Type":"Notification","Message":"not from SNS"}""";
        var message = new Message
        {
            MessageId = "sqs-2", ReceiptHandle = "r-2", Body = body,
            MessageAttributes = new() { ["tenant"] = new MessageAttributeValue { DataType = "String", StringValue = "contoso" } }
        };

        var browsed = AwsMessageMapper.FromSqs(message, ServiceBusEntityReference.Subscription("events", "billing"), ServiceBusSubQueue.Active);

        Assert.Equal(body, Encoding.UTF8.GetString(browsed.Body.Span));
        Assert.Equal("tenant", Assert.Single(browsed.ApplicationProperties).Name);
    }

    // librdkafka's default partitioner is consistent_random (CRC32), Java's is murmur2. Records resent from
    // QueueLoom must land in the partition Java/Spring producers use for the same key, or per-key order breaks
    // and a compacted topic keeps both the stale and the replayed value (compaction is per partition).
    [Fact]
    public void KafkaResendsUseTheJavaCompatiblePartitionerForKeyedRecords()
    {
        var config = KafkaWorkspace.CreateProducerConfig(new ClientConfig { BootstrapServers = "localhost:9092" });

        Assert.Equal(Partitioner.Murmur2Random, config.Partitioner);
        Assert.Equal(Acks.All, config.Acks);
        Assert.Equal("localhost:9092", config.BootstrapServers);
    }

    // Pub/Sub adds googclient_* attributes itself (schema name, encoding and revision on topics with a
    // schema) and rejects a publish whose attribute keys begin with "goog" (case-insensitive).
    [Fact]
    public void PubSubReservedGoogAttributesDoNotTravelWithAResentCopy()
    {
        var received = new ReceivedMessage
        {
            AckId = "ack-1",
            Message = new PubsubMessage
            {
                MessageId = "1", Data = ByteString.CopyFromUtf8("{}"),
                Attributes =
                {
                    ["googclient_schemaname"] = "projects/p/schemas/orders",
                    ["googclient_schemaencoding"] = "JSON",
                    ["googclient_schemarevisionid"] = "a1b2c3d4",
                    ["GOOG_custom"] = "x",
                    ["tenant"] = "contoso"
                }
            }
        };

        var browsed = GooglePubSubWorkspace.ToBrowsedMessage(received, ServiceBusEntityReference.Subscription("orders", "billing"), ServiceBusSubQueue.DeadLetter);

        Assert.Equal(["tenant"], browsed.ApplicationProperties.Select(property => property.Name));
    }

    [Fact]
    public void SearchMatchesOfOneFifoGroupKeepTheirReceiveOrderWhenTimestampsTie()
    {
        var queue = ServiceBusEntityReference.Queue("orders.fifo");
        var sentAt = DateTimeOffset.Parse("2026-10-01T10:00:00.123Z", System.Globalization.CultureInfo.InvariantCulture);
        // Leased services derive the sequence number from a hash of the message ID: it says nothing about order.
        long[] hashed = [900, 100, 500];
        var matches = hashed.Select((sequence, index) => new BrowsedMessage(queue, ServiceBusSubQueue.DeadLetter, sequence,
            Encoding.UTF8.GetBytes($"step-{index + 1}"), new EditableMessageProperties(MessageId: $"id-{index}", SessionId: "order-7"),
            enqueuedAt: sentAt) { HasSequenceNumber = false }).ToArray();
        var result = new DeadLetterSearchResult(Guid.NewGuid(), sentAt, sentAt,
            [new DeadLetterSearchSourceResult(queue, ServiceBusSubQueue.DeadLetter, 3, matches)], false);

        Assert.Equal(["step-1", "step-2", "step-3"], result.Matches.Select(message => Encoding.UTF8.GetString(message.Body.Span)));
    }
}

public sealed partial class ViewModelStateTests
{
    // Messages of one SQS FIFO group sent in one SendMessageBatch share their millisecond SentTimestamp. Their
    // receive order is the group order; ordering the tie by the random SQS message ID scrambles it, and a
    // "resend all" then replays the group out of order.
    [Fact]
    public async Task FifoGroupKeepsItsReceiveOrderWhenTimestampsTieAndIsResentInThatOrder()
    {
        var profile = CreateProfile("aws", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.AmazonSqsSns
        };
        var queue = ServiceBusEntityReference.Queue("orders.fifo");
        var sentAt = DateTimeOffset.Parse("2026-10-01T10:00:00.123Z", System.Globalization.CultureInfo.InvariantCulture);
        string[] ids = ["f3c9", "a91d", "d07e"];
        var messages = ids.Select((id, index) => new BrowsedMessage(queue, ServiceBusSubQueue.DeadLetter, 1_000 + index,
            Encoding.UTF8.GetBytes($"step-{index + 1}"), new EditableMessageProperties(MessageId: id, SessionId: "order-7"),
            enqueuedAt: sentAt) { HasSequenceNumber = false }).ToArray();
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders.fifo", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 0, deadLetter: 3)))]),
            BrowseMessages = messages
        };
        await using var viewModel = CreateViewModel(
            new FakeProfileRepository([profile], profile.Id), workspace, new FakeDialogService());
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.BrowseSelectedDeadLettersCommand.ExecuteAsync();

        Assert.Equal(["step-1", "step-2", "step-3"], viewModel.Messages.Select(item => Encoding.UTF8.GetString(item.Message.Body.Span)));
        viewModel.AreAllMessagesMarked = true;
        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();

        Assert.Equal(["step-1", "step-2", "step-3"], workspace.SentMessages.Select(request => request.Message.Body.Content));
    }
}
