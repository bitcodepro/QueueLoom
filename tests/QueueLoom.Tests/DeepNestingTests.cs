using System.Text;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// Message bodies, schemas and filters come from brokers and users. Nesting deep enough to exhaust the stack used to
// end the whole process (a stack overflow cannot be caught): on Windows, whose threads have 1 MiB stacks, a
// self-referencing Avro schema already did. Each case runs on a thread with a 1 MiB stack, so the outcome does not
// depend on the platform, and must be reported as an ordinary error. Ordinary nesting keeps working.
public sealed class DeepNestingTests
{
    // The stack of a Windows thread.
    private const int SmallStack = 1024 * 1024;

    [Fact]
    public void ASelfReferencingAvroSchemaIsReported_FileAndRegistry()
    {
        var (file, registry) = OnSmallStack(() =>
        {
            var decodedFile = BodyDecoder.Decode(BodyDecoderTests.AvroFile("""{"type":"X","name":"X"}""", "null", 1, [2]))!;
            var decodedRegistry = BodyDecoder.Decode(new byte[] { 0, 0, 0, 0, 1, 2 },
                schema: new MessageSchema(1, MessageSchemaType.Avro, """{"type":"X","name":"X"}"""))!;
            return (decodedFile, decodedRegistry);
        });

        foreach (var decoded in new[] { file, registry })
        {
            Assert.False(decoded.IsJson);
            Assert.Contains("refers to itself", decoded.Note, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ARecursiveAvroRecordStillDecodesItsData()
    {
        // A linked list three items long: recursion in the schema, shallow in the data.
        const string schema = """
            {"type":"record","name":"Node","fields":[
              {"name":"value","type":"int"},{"name":"next","type":["null","Node"]}]}
            """;
        byte[] payload = [0x02, 0x02, 0x04, 0x02, 0x06, 0x00];

        var decoded = OnSmallStack(() => BodyDecoder.Decode(new byte[] { 0, 0, 0, 0, 7 }.Concat(payload).ToArray(),
            schema: new MessageSchema(7, MessageSchemaType.Avro, schema)))!;

        Assert.True(decoded.IsJson, decoded.Note);
        Assert.Contains("\"value\":3", decoded.Text.Replace(" ", "", StringComparison.Ordinal), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("parentheses")]
    [InlineData("not")]
    [InlineData("minus")]
    public void ADeeplyNestedSqlFilterIsASyntaxError(string nesting)
    {
        const int depth = 100_000;
        var text = nesting switch
        {
            "parentheses" => new string('(', depth) + "a = '1'" + new string(')', depth),
            "not" => string.Concat(Enumerable.Repeat("NOT ", depth)) + "a = '1'",
            _ => "b = " + string.Concat(Enumerable.Repeat("- ", depth)) + "1"
        };

        var failure = OnSmallStack(() => Record.Exception(() => SqlFilter.Parse(text)));

        Assert.IsType<SqlFilterSyntaxException>(failure);
        Assert.Contains("nested too deeply", failure.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("OR", "a = '2'")]
    [InlineData("AND", "a = '1'")]
    public void AVeryLongSqlConditionChainIsReportedWhenEvaluated(string joiner, string condition)
    {
        // The chain is flat to read but builds a tree as deep as it is long. Every condition leaves the outcome open
        // (false for OR, true for AND), so the part that could not be evaluated decides it and is reported.
        var text = string.Join($" {joiner} ", Enumerable.Repeat(condition, 100_000));

        var failure = OnSmallStack(() => Record.Exception(() => SqlFilter.Parse(text).Evaluate(Message())));

        Assert.IsType<SqlFilterNotSupportedException>(failure);
        Assert.Contains("nested too deeply", failure.Message, StringComparison.Ordinal);
    }

    // A part that cannot be evaluated does not matter when the other side decides (TRUE OR x), as for any part
    // QueueLoom cannot check: the long chain still answers correctly.
    [Fact]
    public void AVeryLongSqlOrChainWithADecidingConditionStillAnswers()
    {
        var text = string.Join(" OR ", Enumerable.Repeat("a = '1'", 100_000));

        Assert.True(OnSmallStack(() => SqlFilter.Parse(text).Evaluate(Message())));
    }

    // Far deeper nesting than any real rule still parses and evaluates on a 1 MiB stack. (Service Bus accepts SQL filters of
    // up to 1,024 characters, about 510 levels of parentheses; a Release build manages about 529 before reporting.)
    [Fact]
    public void DeepButReasonableNestingStillWorks()
    {
        const int depth = 300;
        var text = new string('(', depth) + "a = '1'" + new string(')', depth);

        Assert.True(OnSmallStack(() => SqlFilter.Parse(text).Evaluate(Message())));
    }

    [Fact]
    public void OrdinaryNestingInASqlFilterStillWorks()
    {
        var text = new string('(', 50) + "NOT NOT a = '1' AND b = 'x' OR - -1 = 1" + new string(')', 50);

        var result = OnSmallStack(() => SqlFilter.Parse(text).Evaluate(Message()));

        Assert.True(result);
    }

    [Theory]
    [InlineData("parentheses")]
    [InlineData("not")]
    public void ADeeplyNestedPubSubFilterIsASyntaxError(string nesting)
    {
        const int depth = 100_000;
        var text = nesting == "parentheses"
            ? new string('(', depth) + "attributes.a = \"1\"" + new string(')', depth)
            : string.Concat(Enumerable.Repeat("NOT ", depth)) + "attributes.a = \"1\"";

        var failure = OnSmallStack(() => Record.Exception(() => PubSubFilter.Parse(text)));

        Assert.IsType<SqlFilterSyntaxException>(failure);
        Assert.Contains("nested too deeply", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AVeryLongPubSubConditionChainIsReportedWhenEvaluated()
    {
        var text = string.Join(" AND ", Enumerable.Repeat("attributes.a = \"1\"", 100_000));

        var failure = OnSmallStack(() => Record.Exception(() => PubSubFilter.Parse(text).Evaluate(Message())));

        Assert.IsType<SqlFilterNotSupportedException>(failure);
        Assert.Contains("nested too deeply", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OrdinaryNestingInAPubSubFilterStillWorks()
    {
        var text = new string('(', 50) + "NOT NOT attributes.a = \"1\" AND attributes.b = \"x\"" + new string(')', 50);

        var result = OnSmallStack(() => PubSubFilter.Parse(text).Evaluate(Message()));

        Assert.True(result);
    }

    private static RoutingMessage Message() => new(EditableMessageProperties.Empty,
        new KeyValuePair<string, object?>[] { new("a", "1"), new("b", "x") });

    /// <summary>Runs on a thread with a small stack and returns the result or rethrows its exception.</summary>
    private static T OnSmallStack<T>(Func<T> work)
    {
        T result = default!;
        Exception? failure = null;
        var thread = new Thread(() =>
        {
            try { result = work(); }
            catch (Exception exception) { failure = exception; }
        }, SmallStack);
        thread.Start();
        thread.Join();
        if (failure is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(failure);
        return result;
    }
}
