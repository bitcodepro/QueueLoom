using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

/// <summary>Bug 5: the JSON export ("all details") keeps property types, every standard field and the Kafka record.</summary>
public sealed class MessageExportDetailTests
{
    private static async Task<JsonElement> ExportAsync(BrowsedMessage message)
    {
        using var stream = new MemoryStream();
        await MessageExport.WriteJsonAsync(stream, [new ExportedMessage("Development", message)], CancellationToken.None);
        return JsonDocument.Parse(stream.ToArray()).RootElement[0].Clone();
    }

    [Fact]
    public async Task TypesStandardFieldsAndEmptyValuesAreKept()
    {
        var properties = new EditableMessageProperties(
            MessageId: "m-1", CorrelationId: string.Empty, ContentType: "application/json", Subject: "orders.created", To: "to",
            ReplyTo: "reply", SessionId: "session", ReplyToSessionId: "reply-session", PartitionKey: "partition",
            TransactionPartitionKey: "transaction", TimeToLive: TimeSpan.FromMinutes(90),
            ScheduledEnqueueTime: new DateTimeOffset(2026, 10, 5, 12, 0, 0, TimeSpan.Zero), AmqpType: "type", AmqpAppId: "app")
            { NativeSubject = "native" };
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 3, "{}"u8.ToArray(),
            properties,
            [
                new MessageApplicationProperty("amount", ApplicationPropertyType.Int64, "42"),
                new MessageApplicationProperty("label", ApplicationPropertyType.String, "42"),
                new MessageApplicationProperty("tags", ApplicationPropertyType.String, "[\"a\"]") { WireType = "String.Array" }
            ]);

        var exported = await ExportAsync(message);

        Assert.Equal(MessageExport.ExportVersion, exported.GetProperty("exportVersion").GetInt32());
        Assert.Equal("reply-session", exported.GetProperty("replyToSessionId").GetString());
        Assert.Equal("partition", exported.GetProperty("partitionKey").GetString());
        Assert.Equal("transaction", exported.GetProperty("transactionPartitionKey").GetString());
        Assert.Equal("native", exported.GetProperty("nativeSubject").GetString());
        Assert.Equal("01:30:00", exported.GetProperty("timeToLive").GetString());
        Assert.Equal(properties.ScheduledEnqueueTime, exported.GetProperty("scheduledEnqueueTimeUtc").GetDateTimeOffset());
        Assert.Equal(string.Empty, exported.GetProperty("correlationId").GetString()); // empty, not missing
        Assert.False(exported.TryGetProperty("sessionIdMissing", out _));
        var typed = exported.GetProperty("typedApplicationProperties").EnumerateArray().ToArray();
        Assert.Equal(("amount", "Int64", "42"), (typed[0].GetProperty("name").GetString(), typed[0].GetProperty("type").GetString(), typed[0].GetProperty("value").GetString()));
        Assert.Equal(("label", "String", "42"), (typed[1].GetProperty("name").GetString(), typed[1].GetProperty("type").GetString(), typed[1].GetProperty("value").GetString()));
        Assert.Equal("String.Array", typed[2].GetProperty("wireType").GetString());
        Assert.False(typed[0].TryGetProperty("wireType", out _));
    }

    [Fact]
    public async Task AMissingPropertyIsLeftOut()
    {
        var exported = await ExportAsync(new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 3,
            "{}"u8.ToArray(), new EditableMessageProperties(MessageId: "m-1")));

        Assert.False(exported.TryGetProperty("correlationId", out _));
        Assert.False(exported.TryGetProperty("timeToLive", out _));
        Assert.False(exported.TryGetProperty("kafka", out _));
    }

    [Fact]
    public async Task TheKafkaRecordIsKeptWithBinaryKeyTombstoneAndRepeatedHeaders()
    {
        var envelope = new KafkaEnvelope([0xff, 0x00], true,
            [new KafkaRawHeader("trace", [0x01]), new KafkaRawHeader("trace", [0x02, 0xfe]), new KafkaRawHeader("empty", null)],
            EditableMessageProperties.Empty, []);
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("events"), ServiceBusSubQueue.Active, 9, Array.Empty<byte>(),
            EditableMessageProperties.Empty) { KafkaEnvelope = envelope };

        var kafka = (await ExportAsync(message)).GetProperty("kafka");

        Assert.Equal(Convert.ToBase64String(new byte[] { 0xff, 0x00 }), kafka.GetProperty("keyBase64").GetString());
        Assert.True(kafka.GetProperty("tombstone").GetBoolean());
        var headers = kafka.GetProperty("headers").EnumerateArray().ToArray();
        Assert.Equal(["trace", "trace", "empty"], headers.Select(header => header.GetProperty("name").GetString()));
        Assert.Equal(Convert.ToBase64String(new byte[] { 0x02, 0xfe }), headers[1].GetProperty("valueBase64").GetString());
        Assert.Equal(JsonValueKind.Null, headers[2].GetProperty("valueBase64").ValueKind);
    }
}
