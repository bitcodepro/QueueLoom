using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

/// <summary>
/// Bug 2: RabbitMQ headers with tables, arrays, void values, 'x' byte arrays and short or unsigned integers must reach
/// the broker again with the same AMQP types and bytes after an unedited copy, and after backup → purge → restore.
/// </summary>
public sealed class RabbitComplexHeaderTests
{
    private static Dictionary<string, object?> Headers() => new(StringComparer.Ordinal)
    {
        ["meta"] = new Dictionary<string, object?>
        {
            ["opaque"] = new byte[] { 0xff },
            ["nothing"] = null,
            ["list"] = new List<object?> { 1, "two"u8.ToArray(), null, new BinaryTableValue([0x00, 0xfe]) },
            ["small"] = (short)-7,
            ["tiny"] = (sbyte)-3,
            ["octet"] = (byte)200,
            ["word"] = (ushort)65000,
            ["unsigned"] = 4_000_000_000u,
            ["single"] = 1.5f,
            ["nested"] = new Dictionary<string, object?> { ["deep"] = new AmqpTimestamp(1_700_000_000) }
        },
        ["blob"] = new BinaryTableValue([0x01, 0xff, 0x80]),
        ["void"] = null,
        ["flags"] = new List<object?> { true, false },
        ["plain"] = "text"u8.ToArray()
    };

    private static BrowsedMessage Browse() => RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(),
        new BasicProperties { MessageId = "complex", Headers = Headers() }, "orders",
        ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter);

    [Fact]
    public void AnUneditedCopyWritesTheSameAmqpTypesAndBytes()
    {
        var draft = Browse().CreateDraft();

        Assert.True(MessageDraftValidator.Validate(draft, MessagingProvider.RabbitMq).IsValid);
        AssertSameAmqp(Headers(), RabbitMqMessageMapper.ToAmqp(draft).Headers!);
    }

    [Fact]
    public async Task ABackupBeforeAPurgeRestoresEveryHeaderExactly()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword))
            with { Provider = MessagingProvider.RabbitMq };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
        await session.BackupAsync(Browse(), default);

        var repository = new JsonDeadLetterBackupRepository(paths);
        var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
        var draft = restored.CreateDraft();

        Assert.True(MessageDraftValidator.Validate(draft, MessagingProvider.RabbitMq).IsValid);
        AssertSameAmqp(Headers(), RabbitMqMessageMapper.ToAmqp(draft).Headers!);
        Assert.Equal("body"u8.ToArray(), restored.Body.ToArray());
    }

    [Fact]
    public void ABrokenTypedValueIsRefusedBeforeSending()
    {
        var draft = new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text), EditableMessageProperties.Empty,
            [new MessageApplicationProperty("meta", ApplicationPropertyType.String, """{"t":"table","v":[["a",{"t":"bytes","v":"not base64!"}]]}""")
                { WireType = AmqpTypedValue.WireType }]);

        var result = MessageDraftValidator.Validate(draft, MessagingProvider.RabbitMq);

        Assert.Contains(result.Errors, error => error.Code == "message.application_property.wire_type_invalid");
    }

    // Deep nesting: 22 tables (66 JSON levels in the typed form) and 32 arrays survive an unchanged copy and a backup
    // restore; the default 64-level JSON reader limit would have refused the draft.
    [Fact]
    public async Task DeeplyNestedHeadersSurviveCopyAndBackup()
    {
        object? tables = "leaf"u8.ToArray();
        for (var level = 0; level < 22; level++) tables = new Dictionary<string, object?> { ["level"] = tables };
        object? arrays = 7;
        for (var level = 0; level < 32; level++) arrays = new List<object?> { arrays };
        var headers = new Dictionary<string, object?> { ["tables"] = tables, ["arrays"] = arrays };
        var browsed = RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(), new BasicProperties { MessageId = "deep", Headers = headers },
            "orders", ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter);

        var copy = browsed.CreateDraft();
        Assert.True(MessageDraftValidator.Validate(copy, MessagingProvider.RabbitMq).IsValid);
        AssertSameAmqp(headers, RabbitMqMessageMapper.ToAmqp(copy).Headers!);

        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword))
            with { Provider = MessagingProvider.RabbitMq };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
        await session.BackupAsync(browsed, default);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var restored = (await repository.LoadAsync(Assert.Single(await repository.ListAsync()))).CreateDraft();
        Assert.True(MessageDraftValidator.Validate(restored, MessagingProvider.RabbitMq).IsValid);
        AssertSameAmqp(headers, RabbitMqMessageMapper.ToAmqp(restored).Headers!);
    }

    // Values that cannot be sent as described are refused before the broker is called; the boundaries are accepted.
    [Theory]
    [InlineData("""{"t":"i16","v":"40000"}""", false)]
    [InlineData("""{"t":"i16","v":"-32768"}""", true)]
    [InlineData("""{"t":"i16","v":"32767"}""", true)]
    [InlineData("""{"t":"u32","v":"4294967295"}""", true)]
    [InlineData("""{"t":"u32","v":"-1"}""", false)]
    [InlineData("""{"t":"i32","v":"abc"}""", false)]
    [InlineData("""{"t":"ts","v":"99999999999999999999"}""", false)]
    [InlineData("""{"t":"dec","v":"12.5"}""", true)]
    [InlineData("""{"t":"dec","v":"99999999999.5"}""", false)]
    [InlineData("""{"t":"f32","v":"x"}""", false)]
    [InlineData("""{"t":"table","v":[["a",{"t":"void"}],["a",{"t":"void"}]]}""", false)]
    [InlineData("""{"t":"f32","v":"1e100"}""", false)]
    [InlineData("""{"t":"f32","v":"NaN"}""", false)]
    [InlineData("""{"t":"f32","v":"Infinity"}""", false)]
    [InlineData("""{"t":"f32","v":"3.4028235E+38"}""", true)]
    [InlineData("""{"t":"f64","v":"1e400"}""", false)]
    [InlineData("""{"t":"f64","v":"-Infinity"}""", false)]
    [InlineData("""{"t":"f64","v":"1.7976931348623157E+308"}""", true)]
    [InlineData("""{"t":"i32","v":"1","v":"2"}""", false)]
    [InlineData("""{"t":"i32","t":"i64","v":"1"}""", false)]
    [InlineData("""{"t":"array","v":[{"t":"bool","v":true,"v":false}]}""", false)]
    public void TypedValuesAreCheckedAgainstWhatTheWireCarries(string value, bool valid)
    {
        Assert.Equal(valid, AmqpTypedValue.Problem(value) is null);
        if (valid)
        {
            RabbitMqMessageMapper.FromTyped(System.Text.Json.Nodes.JsonNode.Parse(value)!.AsObject());
        }
    }

    [Theory]
    [InlineData(255, true)]
    [InlineData(256, false)]
    public void TableFieldNamesAreAtMost255Bytes(int length, bool valid)
    {
        var name = new string('n', length);
        var json = "{\"t\":\"table\",\"v\":[[\"" + name + "\",{\"t\":\"void\"}]]}";
        Assert.Equal(valid, AmqpTypedValue.Problem(json) is null);
    }

    // Routing preview: the broker's headers matcher compares values, not field types. A browsed float 1.5 and short 7
    // match a binding on the double 1.5 and the integer 7; a table header is left to RabbitMQ.
    [Fact]
    public void TypedHeadersArePredictedAsTheBrokerMatchesThem()
    {
        var browsed = RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(), new BasicProperties
        {
            MessageId = "routing",
            Headers = new Dictionary<string, object?> { ["ratio"] = 1.5f, ["count"] = (short)7, ["meta"] = new Dictionary<string, object?> { ["a"] = 1 } }
        }, "orders", ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter);
        var message = QueueLoom.Core.Routing.RoutingMessage.From(browsed.CreateDraft());

        Assert.Equal(QueueLoom.Core.Routing.RoutingOutcome.Receives, QueueLoom.Core.Routing.RabbitBindings.MatchHeaders(
            new Dictionary<string, object?> { ["x-match"] = "all", ["ratio"] = 1.5, ["count"] = 7L }, message).Outcome);
        Assert.Equal(QueueLoom.Core.Routing.RoutingOutcome.Skips, QueueLoom.Core.Routing.RabbitBindings.MatchHeaders(
            new Dictionary<string, object?> { ["x-match"] = "all", ["ratio"] = 2.5 }, message).Outcome);
        Assert.Equal(QueueLoom.Core.Routing.RoutingOutcome.Unknown, QueueLoom.Core.Routing.RabbitBindings.MatchHeaders(
            new Dictionary<string, object?> { ["x-match"] = "all", ["meta"] = "x" }, message).Outcome);
    }

    // Headers named like broker counters are left out of a message's stable identity only where its reader established
    // that the broker writes them (a quorum queue's x-delivery-count, RabbitMQ 4.3's x-acquired-count). Elsewhere they
    // are the producer's and keep messages apart, even when the chosen one is gone and only a counter twin remains.
    [Fact]
    public void OnlyBrokerOwnedCountersAreLeftOutOfTheIdentity()
    {
        var brokerOwned = new HashSet<string>(StringComparer.Ordinal) { "x-delivery-count", "x-acquired-count" };
        BrowsedMessage Read(long count, string tenant, bool owned) => RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(), new BasicProperties
        {
            MessageId = "same",
            Headers = new Dictionary<string, object?>
            {
                ["x-delivery-count"] = count, ["x-acquired-count"] = count, ["tenant"] = System.Text.Encoding.UTF8.GetBytes(tenant)
            }
        }, "orders", ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter) with
        {
            BrokerOwnedHeaders = owned ? brokerOwned : new HashSet<string>()
        };

        // Broker-owned (quorum): the broker raised the counter; the same message is still found.
        var later = Read(5, "acme", owned: true);
        Assert.Same(later, Assert.Single(MessageFingerprint.Find(MessageFingerprint.Of(Read(1, "acme", owned: true)), [later], out _)));
        // Producer-owned (RabbitMQ 4.2 classic): a different counter value is a different message, also when it is alone.
        Assert.Empty(MessageFingerprint.Find(MessageFingerprint.Of(Read(2, "acme", owned: false)), [Read(1, "acme", owned: false)], out var ambiguous));
        Assert.False(ambiguous);
        // A different user header is always a different message.
        Assert.Empty(MessageFingerprint.Find(MessageFingerprint.Of(Read(1, "acme", owned: true)), [Read(1, "other", owned: true)], out _));
        // Two broker-owned counter twins, neither matching exactly: which one was meant is unknown.
        var one = Read(1, "acme", owned: true);
        var two = Read(2, "acme", owned: true);
        Assert.Same(two, Assert.Single(MessageFingerprint.Find(MessageFingerprint.Of(Read(2, "acme", owned: true)), [one, two], out _)));
        Assert.Empty(MessageFingerprint.Find(MessageFingerprint.Of(Read(3, "acme", owned: true)), [one, two], out ambiguous));
        Assert.True(ambiguous);
    }

    /// <summary>Same keys, values, CLR types (which decide the AMQP field types RabbitMQ.Client writes) and bytes.</summary>
    private static void AssertSameAmqp(object? expected, object? actual)
    {
        switch (expected)
        {
            case null:
                Assert.Null(actual);
                break;
            case IDictionary<string, object?> table:
                var other = Assert.IsAssignableFrom<IDictionary<string, object?>>(actual);
                Assert.Equal(table.Keys.Order(StringComparer.Ordinal), other.Keys.Order(StringComparer.Ordinal));
                foreach (var (key, value) in table) AssertSameAmqp(value, other[key]);
                break;
            case IList<object?> list:
                var items = Assert.IsAssignableFrom<IList<object?>>(actual);
                Assert.Equal(list.Count, items.Count);
                for (var index = 0; index < list.Count; index++) AssertSameAmqp(list[index], items[index]);
                break;
            case BinaryTableValue binary:
                Assert.Equal(binary.Bytes, Assert.IsType<BinaryTableValue>(actual).Bytes);
                break;
            case byte[] bytes:
                Assert.Equal(bytes, Assert.IsType<byte[]>(actual));
                break;
            default:
                Assert.Equal(expected.GetType(), actual?.GetType());
                Assert.Equal(expected, actual);
                break;
        }
    }
}
