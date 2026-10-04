using System.Text;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class BodyDecoderTests
{
    [Fact]
    public void RegistryAvro_SchemaThatNamesItself_IsReportedInsteadOfCrashing()
    {
        var schema = new MessageSchema(1, MessageSchemaType.Avro, """{"type":"X","name":"X"}""");

        var decoded = BodyDecoder.Decode(new byte[] { 0, 0, 0, 0, 1, 2 }, schema: schema)!;

        Assert.False(decoded.IsJson);
        Assert.Contains("refers to itself", decoded.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void AvroFile_SchemaThatNamesItself_IsReportedInsteadOfCrashing()
    {
        var decoded = BodyDecoder.Decode(AvroFile("""{"type":"X","name":"X"}""", "null", 1, [2]))!;

        Assert.False(decoded.IsJson);
        Assert.Contains("refers to itself", decoded.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void AvroFile_FieldWithoutAType_IsReportedInsteadOfThrowing()
    {
        var schema = """{"type":"record","name":"R","fields":[{"name":"a"}]}""";

        var decoded = BodyDecoder.Decode(AvroFile(schema, "null", 1, [2]))!;

        Assert.False(decoded.IsJson);
        Assert.Contains("could not be read", decoded.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void RegistryAvro_LinkedListKeepsReading()
    {
        var schema = new MessageSchema(1, MessageSchemaType.Avro,
            """{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","Node"]}]}""");
        var body = new List<byte> { 0, 0, 0, 0, 1 };
        for (var index = 0; index < 50; index++)
        {
            body.AddRange(Long(index));
            body.AddRange(Long(index == 49 ? 0 : 1));
        }

        var decoded = BodyDecoder.Decode(body.ToArray(), schema: schema)!;

        Assert.True(decoded.IsJson, decoded.Note);
    }
}

public sealed class SearchAndComparisonRobustnessTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    private static BrowsedMessage Message(byte[] body, long number = 1) =>
        new(Orders, ServiceBusSubQueue.DeadLetter, number, body, new EditableMessageProperties(MessageId: $"m-{number}"));

    private static BrowsedMessage Message(string body, long number = 1) => Message(Encoding.UTF8.GetBytes(body), number);

    [Fact]
    public void JsonSearch_ReadsBodiesThatStartWithAByteOrderMark()
    {
        byte[] body = [0xEF, 0xBB, 0xBF, .. """{"status":"failed"}"""u8.ToArray()];

        Assert.True(MessageSearchQuery.Parse("$.status == 'failed'").Matches(Message(body)));
    }

    [Theory]
    [InlineData("""{"id":250}""", "$.id == '250'")]
    [InlineData("""{"id":"250"}""", "$.id == 250")]
    [InlineData("""{"id":250}""", "$.id == 250")]
    public void JsonSearch_NumberEqualsTextHoldingIt_InBothDirections(string body, string query) =>
        Assert.True(MessageSearchQuery.Parse(query).Matches(Message(body)));

    [Theory]
    [InlineData("$.items[99999999999] == 1")]
    [InlineData("$.id == .5")]
    [InlineData("$.id == 05")]
    [InlineData("$.id == +5")]
    [InlineData("$.id == Infinity")]
    public void JsonSearch_OddNumbersAreQueryMistakes(string query) =>
        Assert.Throws<MessageSearchQueryException>(() => MessageSearchQuery.Parse(query));

    [Theory]
    [InlineData("Message is 300KB, limit is 256KB", "Message is {n}KB, limit is {n}KB")]
    [InlineData("Took 1500ms", "Took {n}ms")]
    [InlineData("Order ORD42 failed", "Order ORD42 failed")]
    public void CausePatterns_ReplaceWholeNumbersOnly(string description, string expected) =>
        Assert.Equal(expected, DeadLetterCauses.Pattern(description));

    [Theory]
    [InlineData("a\rb", "a\nb")]
    [InlineData("a\r\nb", "a\nb")]
    public void Comparison_LineEndingsCount(string left, string right) =>
        Assert.NotEqual(0, MessageComparison.Compare(Message(left), Message(right, 2)).ChangedLines);

    [Fact]
    public void Comparison_SameTextIsEqual() =>
        Assert.Equal(0, MessageComparison.Compare(Message("a\r\nb"), Message("a\r\nb", 2)).ChangedLines);
}
