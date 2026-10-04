using System.Globalization;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.RabbitMq;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

/// <summary>
/// A broker value outside DateTimeOffset or TimeSpan used to abort the read, and a whole Decimal or UInt64
/// past the exact range of double used to compare equal to a different integer.
/// </summary>
public sealed class BrokerTimeAndExactIntegerTests
{
    private static readonly ServiceBusEntityReference Source = ServiceBusEntityReference.Queue("orders");

    [Fact]
    public void A_millisecond_timestamp_is_omitted_and_the_message_is_still_read()
    {
        // 1_700_000_000_000 is milliseconds for 2023-11-14, a value clients store in the AMQP seconds field.
        var message = RabbitMqMessageMapper.FromAmqp("hi"u8.ToArray(),
            new BasicProperties
            {
                MessageId = "m-1",
                Timestamp = new AmqpTimestamp(1_700_000_000_000),
                Expiration = "60000"
            },
            "orders", Source, ServiceBusSubQueue.Active);

        Assert.Equal("m-1", message.Properties.MessageId);
        Assert.Null(message.EnqueuedAt);
        Assert.Equal(TimeSpan.FromSeconds(60), message.Properties.TimeToLive);
    }

    [Fact]
    public void An_expiration_that_does_not_fit_in_a_TimeSpan_is_omitted()
    {
        var message = RabbitMqMessageMapper.FromAmqp("hi"u8.ToArray(),
            new BasicProperties { MessageId = "m-1", Expiration = long.MaxValue.ToString(CultureInfo.InvariantCulture) },
            "orders", Source, ServiceBusSubQueue.Active);

        Assert.Null(message.Properties.TimeToLive);
        Assert.Equal("m-1", message.Properties.MessageId);
    }

    [Fact]
    public void A_header_timestamp_outside_the_calendar_stays_the_raw_seconds()
    {
        var message = RabbitMqMessageMapper.FromAmqp("hi"u8.ToArray(),
            new BasicProperties
            {
                MessageId = "m-1",
                Headers = new Dictionary<string, object?> { ["when"] = new AmqpTimestamp(1_700_000_000_000) }
            },
            "orders", Source, ServiceBusSubQueue.Active);

        var property = Assert.Single(message.ApplicationProperties);
        Assert.Equal("when", property.Name);
        Assert.Equal(ApplicationPropertyType.Int64, property.Type);
        Assert.Equal("1700000000000", property.Value);
    }

    [Fact]
    public void A_dead_letter_reason_survives_an_unreadable_death_time()
    {
        var death = new Dictionary<string, object?>
        {
            ["queue"] = "orders"u8.ToArray(),
            ["reason"] = "rejected"u8.ToArray(),
            ["count"] = 2L,
            ["time"] = new AmqpTimestamp(1_700_000_000_000)
        };
        var message = RabbitMqMessageMapper.FromAmqp("hi"u8.ToArray(),
            new BasicProperties
            {
                MessageId = "m-1",
                Headers = new Dictionary<string, object?> { ["x-death"] = new List<object?> { death } }
            },
            "orders.dlq", Source, ServiceBusSubQueue.DeadLetter);

        Assert.Equal("Rejected by a consumer", message.DeadLetterReason);
        Assert.Equal("From orders, 2 times", message.DeadLetterErrorDescription);
    }

    [Fact]
    public void An_ordinary_timestamp_and_ttl_are_unchanged()
    {
        var message = RabbitMqMessageMapper.FromAmqp("hi"u8.ToArray(),
            new BasicProperties
            {
                MessageId = "m-1",
                Timestamp = new AmqpTimestamp(1_700_000_000),
                Expiration = "60000",
                Headers = new Dictionary<string, object?> { ["when"] = new AmqpTimestamp(1_700_000_000) }
            },
            "orders", Source, ServiceBusSubQueue.Active);

        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1_700_000_000), message.EnqueuedAt);
        Assert.Equal(TimeSpan.FromMinutes(1), message.Properties.TimeToLive);
        Assert.Equal(ApplicationPropertyType.DateTimeOffset, Assert.Single(message.ApplicationProperties).Type);
    }

    [Fact]
    public void A_queue_timestamp_outside_the_calendar_does_not_fail_the_queue()
    {
        var info = AwsQueueInfo.From("https://sqs.us-east-1.amazonaws.com/1/orders", new Dictionary<string, string>
        {
            ["QueueArn"] = "arn:aws:sqs:us-east-1:1:orders",
            ["CreatedTimestamp"] = "1700000000000",
            ["LastModifiedTimestamp"] = "1700000000"
        });

        Assert.Equal("orders", info.Name);
        Assert.Null(info.CreatedAt);
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1_700_000_000), info.UpdatedAt);
    }

    [Fact]
    public void A_decimal_integer_matches_only_that_integer()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            new Dictionary<string, object?> { ["id"] = 9007199254740993m });

        Assert.Equal(RoutingOutcome.Receives, Check("id = 9007199254740993", message));
        Assert.Equal(RoutingOutcome.Skips, Check("id = 9007199254740992", message));
        Assert.Equal(RoutingOutcome.Receives, Check("price = 0.1", new RoutingMessage(EditableMessageProperties.Empty,
            new Dictionary<string, object?> { ["price"] = 0.1m })));
    }

    [Fact]
    public void A_whole_number_double_cannot_hold_is_left_to_Service_Bus()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            new Dictionary<string, object?> { ["id"] = ulong.MaxValue });

        Assert.Equal(RoutingOutcome.Unknown, Check("id = 18446744073709551615", message));
        Assert.Equal(RoutingOutcome.Unknown, Check("id = 18446744073709551614", message));
    }

    private static RoutingOutcome Check(string filter, RoutingMessage message) =>
        TopicRouting.Check(new SubscriptionRule("r", RuleFilterKind.Sql, filter), message).Outcome;
}
