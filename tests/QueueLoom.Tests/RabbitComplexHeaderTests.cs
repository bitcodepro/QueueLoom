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
