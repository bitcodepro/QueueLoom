using System.Globalization;
using Amazon.SQS.Model;
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
    public void An_integral_decimal_divides_as_a_decimal()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            new Dictionary<string, object?> { ["amount"] = 5m });

        Assert.Equal(RoutingOutcome.Receives, Check("amount / 2 > 2", message));
    }

    [Fact]
    public void Decimal_endpoints_do_not_abort_routing()
    {
        Assert.Equal(decimal.MaxValue, Assert.IsType<decimal>(RoutingValue.Normalize(decimal.MaxValue)));
        Assert.Equal(decimal.MinValue, Assert.IsType<decimal>(RoutingValue.Normalize(decimal.MinValue)));

        Assert.Equal(RoutingOutcome.Unknown, Check("amount > 0", new RoutingMessage(EditableMessageProperties.Empty,
            new Dictionary<string, object?> { ["amount"] = decimal.MaxValue })));
        Assert.Equal(RoutingOutcome.Unknown, Check("amount > 0", new RoutingMessage(EditableMessageProperties.Empty,
            new Dictionary<string, object?> { ["amount"] = decimal.MinValue })));
    }

    [Fact]
    public void A_whole_number_double_cannot_hold_is_left_to_Service_Bus()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            new Dictionary<string, object?> { ["id"] = ulong.MaxValue });

        Assert.Equal(RoutingOutcome.Unknown, Check("id = 18446744073709551615", message));
        Assert.Equal(RoutingOutcome.Unknown, Check("id = 18446744073709551614", message));
    }

    [Fact]
    public void Ulong_2pow63_is_not_long_max() =>
        Assert.NotEqual(RoutingOutcome.Receives, Check("id = 9223372036854775807", 9223372036854775808UL));

    [Theory]
    [InlineData("amount > fee")]
    [InlineData("amount + fee > 5")]
    public void Integral_and_fractional_decimals_compare_together(string filter)
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            new Dictionary<string, object?> { ["amount"] = 5m, ["fee"] = 0.5m });
        Assert.Equal(RoutingOutcome.Receives, Check(filter, message));
    }

    [Theory]
    [InlineData("amount > 2.5")]
    [InlineData("amount = 5.0")]
    [InlineData("amount IN (5.0, 6.0)")]
    [InlineData("amount * 1.5 > 7")]
    [InlineData("amount + 0.5 = 5.5")]
    public void Whole_decimal_matches_a_fractional_literal(string filter) =>
        Assert.Equal(RoutingOutcome.Receives, CheckAmount(filter, 5m));

    [Fact]
    public void A_long_is_not_widened_into_a_different_double()
    {
        // A long that double cannot hold is not cast to a neighbouring double. That pair is unknown,
        // not a skip and not a match. 2^53 itself is exact, and so is an ordinary small long.
        Assert.Equal(RoutingOutcome.Unknown, Check("id = 9007199254740992.0", 9007199254740993L));
        Assert.Equal(RoutingOutcome.Unknown, Check("id = 9007199254741000.0", 9007199254741001L));
        Assert.Equal(RoutingOutcome.Unknown, Check("id = 9223372036854775807.0", 9223372036854775806L));
        Assert.Equal(RoutingOutcome.Unknown, Check("id > 9007199254740992.0", 9007199254740993L));
        Assert.Equal(RoutingOutcome.Unknown, Check("id + 0.0 = 9007199254740992", 9007199254740993L));
        Assert.Equal(RoutingOutcome.Unknown, Check("id + 0.0 = 9007199254740993", 9007199254740993L));
        Assert.Equal(RoutingOutcome.Unknown, Check("id = 9007199254740993.0", 9007199254740993L));
        Assert.Equal(RoutingOutcome.Unknown, Check("id = 9007199254740993.0", 9007199254740993m));
        // 9007199254740993.0 is the double 2^53, the same value as this long.
        Assert.Equal(RoutingOutcome.Receives, Check("id = 9007199254740993.0", 9007199254740992L));
        Assert.Equal(RoutingOutcome.Receives, Check("id = 9007199254740992.0", 9007199254740992L));
        Assert.Equal(RoutingOutcome.Receives, Check("id = 9007199254740992.0", 9007199254740992d));
        Assert.Equal(RoutingOutcome.Receives, Check("id = 5.0", 5L));
        // long.MaxValue widens to 2^63, so it is not compared with the double +Infinity of 1e400.
        // Main reported Receives by that cast. Guessing the same match is the loss this guard refuses.
        Assert.Equal(RoutingOutcome.Unknown, Check("id < 1e400", long.MaxValue));
    }

    [Fact]
    public void An_exactly_representable_double_matches_its_literal()
    {
        // These spellings have 16 or 17 significant digits. Parsing them as decimal and casting
        // back misses the double, so the preview used to say unknown for the property's own value.
        Assert.Equal(RoutingOutcome.Receives, Check("id = 1234567.123456789", 1234567.123456789d));
        Assert.Equal(RoutingOutcome.Receives, Check("id = 0.30000000000000004", 0.30000000000000004d));
    }

    [Fact]
    public void Long_and_double_arithmetic_follows_double_promotion()
    {
        Assert.Equal(RoutingOutcome.Skips, Check("id * 0.1 = 0.3", 3L));
        Assert.Equal(RoutingOutcome.Receives, Check("id * 0.1 = 0.30000000000000004", 3L));
        Assert.Equal(RoutingOutcome.Receives, Check("id * 0.1 > 0.3", 3L));
        Assert.Equal(RoutingOutcome.Skips, Check("id % 0.1 = 0", 1L));
        Assert.Equal(RoutingOutcome.Receives, Check("id / 3.0 = 1.6666666666666667", 5L));
        Assert.Equal(RoutingOutcome.Skips, Check("id * 0.1 = 0.3 OR id = 99", 3L));
        Assert.Equal(RoutingOutcome.Skips, Check("id * 3 = 0.3", 0.1f));
    }

    [Fact]
    public void An_sqs_sent_timestamp_outside_the_calendar_does_not_fail_the_message()
    {
        var message = new Message
        {
            MessageId = "m-1",
            Body = "hello",
            Attributes = new Dictionary<string, string> { ["SentTimestamp"] = "253402300800000" }
        };

        var browsed = AwsMessageMapper.FromSqs(message, Source, ServiceBusSubQueue.Active);

        Assert.Equal("m-1", browsed.Properties.MessageId);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(browsed.Body.Span));
        Assert.Null(browsed.EnqueuedAt);
    }

    private static RoutingOutcome Check(string filter, object value) =>
        TopicRouting.Check(new SubscriptionRule("r", RuleFilterKind.Sql, filter),
            new RoutingMessage(EditableMessageProperties.Empty, new Dictionary<string, object?> { ["id"] = value })).Outcome;

    private static RoutingOutcome Check(string filter, RoutingMessage message) =>
        TopicRouting.Check(new SubscriptionRule("r", RuleFilterKind.Sql, filter), message).Outcome;

    private static RoutingOutcome CheckAmount(string filter, object value) =>
        Check(filter, new RoutingMessage(EditableMessageProperties.Empty,
            new Dictionary<string, object?> { ["amount"] = value }));
}
