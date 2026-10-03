using System.IO.Compression;
using System.Text;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class MessageSearchQueryTests
{
    private const string Order =
        """{"order": {"id": "ORD-1042", "status": "failed", "total": 250.5, "paid": false, "coupon": null, "items": [{"sku": "A-1", "qty": 2}, {"sku": "B-7", "qty": 1}], "placed": "2026-09-30T14:00:00Z", "region": "EU"}}""";

    private static BrowsedMessage Message(string body, string? subject = "order.created", byte[]? raw = null, string? contentType = null) =>
        new(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1, raw ?? Encoding.UTF8.GetBytes(body),
            new EditableMessageProperties(MessageId: "m-1", Subject: subject, ContentType: contentType),
            [new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "Acme")],
            deadLetterReason: "ProcessingFailed", deadLetterErrorDescription: "Order ORD-1042 was not found");

    [Theory]
    [InlineData("$.order.status == 'failed'", true)]
    [InlineData("$.order.status = \"failed\"", true)]
    [InlineData("$.order.status == 'FAILED'", false)]
    [InlineData("$.order.status != 'shipped'", true)]
    [InlineData("$.order.missing != 'shipped'", false)]
    [InlineData("$.order.total > 250", true)]
    [InlineData("$.order.total <= 250", false)]
    [InlineData("$.order.items[*].sku == 'B-7'", true)]
    [InlineData("$.order.items[0].sku == 'B-7'", false)]
    [InlineData("$.order.items[1].qty >= 1", true)]
    [InlineData("$['order']['region'] == 'EU'", true)]
    [InlineData("$.order.paid == false", true)]
    [InlineData("$.order.coupon == null", true)]
    [InlineData("$.order.coupon", true)]
    [InlineData("$.order.discount", false)]
    [InlineData("$.order.id =~ /^ORD-\\d+$/", true)]
    [InlineData("$.order.id =~ /^ord-/", false)]
    [InlineData("$.order.id =~ /^ord-/i", true)]
    [InlineData("$.order.placed >= '2026-09-30'", true)]
    [InlineData("$.order.region == 'US' or $.order.total > 100", true)]
    [InlineData("$.order.region == 'EU' and $.order.paid == true", false)]
    [InlineData("$.order.region == 'EU' && $.order.items[*].qty == 2", true)]
    public void Json_conditions_look_at_fields_of_the_body(string query, bool expected) =>
        Assert.Equal(expected, MessageSearchQuery.Parse(query).Matches(Message(Order)));

    [Fact]
    public void Json_conditions_look_inside_packed_bodies_and_skip_other_bodies()
    {
        using var packed = new MemoryStream();
        using (var gzip = new GZipStream(packed, CompressionLevel.Fastest, leaveOpen: true))
        {
            gzip.Write(Encoding.UTF8.GetBytes(Order));
        }
        var query = MessageSearchQuery.Parse("$.order.status == 'failed'");
        Assert.True(query.Matches(Message(string.Empty, raw: packed.ToArray())));
        Assert.False(query.Matches(Message("status failed, not JSON")));
        Assert.False(query.Matches(Message(string.Empty)));
    }

    [Theory]
    [InlineData("/ORD-\\d{4}/", true)]
    [InlineData("/ord-\\d{4}/", false)]
    [InlineData("/ord-\\d{4}/i", true)]
    [InlineData("/^order\\.(created|updated)$/", true)]
    [InlineData("/^acme$/i", true)]
    [InlineData("/never-there/", false)]
    public void Regular_expressions_search_every_field(string query, bool expected) =>
        Assert.Equal(expected, MessageSearchQuery.Parse(query).Matches(Message("{}")));

    [Fact]
    public void Plain_text_is_found_anywhere_ignoring_case()
    {
        Assert.True(MessageSearchQuery.Parse("acme").Matches(Message("{}")));
        Assert.True(MessageSearchQuery.Parse("not FOUND").Matches(Message("{}")));
        Assert.True(MessageSearchQuery.Parse("/path/to/file").Matches(Message("see /path/to/file")));
        Assert.False(MessageSearchQuery.Parse("globex").Matches(Message("{}")));
    }

    [Theory]
    [InlineData("/[unclosed/", "regular expression cannot be read")]
    [InlineData("$.order.id =~ /^ORD/q", "not a flag")]
    [InlineData("$.order.status == failed", "not a value")]
    [InlineData("$.order.status ==", "value is missing")]
    [InlineData("$.order.items[x]", "inside [ ]")]
    [InlineData("$.order.total > true", "Only numbers and text")]
    [InlineData("$.order.id =~ ^ORD", "between slashes")]
    [InlineData("$.order.a == 1 xor $.b", "Expected and, or")]
    public void Broken_searches_say_what_is_wrong(string query, string reason)
    {
        var exception = Assert.Throws<MessageSearchQueryException>(() => MessageSearchQuery.Parse(query));
        Assert.Contains(reason, exception.Message, StringComparison.Ordinal);
    }


    [Fact]
    public void Json_regex_ends_at_a_slash_after_an_even_number_of_backslashes()
    {
        // Query text has two backslash characters before the closer, so the slash is not escaped.
        // The pattern source is C:\\orders\\, which matches the text C:\orders\ (trailing backslash).
        var query = MessageSearchQuery.Parse("$.path =~ /C:\\\\orders\\\\/");
        Assert.True(query.Matches(Message("""{"path":"C:\\orders\\"}""")));
        Assert.False(query.Matches(Message("""{"path":"C:\\orders"}""")));

        var trailing = MessageSearchQuery.Parse("$.path =~ /pre\\\\/");
        Assert.True(trailing.Matches(Message("""{"path":"pre\\"}""")));
        Assert.False(trailing.Matches(Message("""{"path":"pre/post"}""")));

        // An even run closes the pattern. "post" is then read as flags, not as more pattern text.
        var leftover = Assert.Throws<MessageSearchQueryException>(() =>
            MessageSearchQuery.Parse("$.path =~ /pre\\\\/post/"));
        Assert.Contains("not a flag", leftover.Message, StringComparison.Ordinal);

        var closed = MessageSearchQuery.Parse("$.path =~ /pre\\\\/ and $.other");
        Assert.True(closed.Matches(Message("""{"path":"pre\\","other":1}""")));
        Assert.False(closed.Matches(Message("""{"path":"pre/post","other":1}""")));
    }

    [Fact]
    public void Json_regex_still_lets_one_backslash_escape_a_slash()
    {
        var query = MessageSearchQuery.Parse("$.path =~ /pre\\/post/");
        Assert.True(query.Matches(Message("""{"path":"pre/post"}""")));
        Assert.False(query.Matches(Message("""{"path":"pre\\post"}""")));
    }

    [Fact]
    public void Top_level_regex_still_closes_at_the_last_slash()
    {
        // The last slash closes a top-level regex. /pre\\/post/ therefore matches a backslash
        // followed by /post, not the text pre/post (that would be a single escaped slash).
        var embedded = MessageSearchQuery.Parse("/pre\\\\/post/");
        Assert.True(embedded.IsRegex);
        Assert.True(embedded.Matches(Message("pre\\/post")));
        Assert.False(embedded.Matches(Message("pre/post")));

        var trailing = MessageSearchQuery.Parse("/C:\\\\orders\\\\/");
        Assert.True(trailing.Matches(Message("C:\\orders\\")));
    }

    [Fact]
    public void A_search_request_reads_its_query_once_and_rejects_a_broken_one()
    {
        var target = new DeadLetterSearchTarget(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1);
        Assert.True(new DeadLetterSearchRequest("$.order.status == 'failed'", [target]).Search.IsJsonCondition);
        Assert.Throws<MessageSearchQueryException>(() => new DeadLetterSearchRequest("/(/", [target]));
    }
}
