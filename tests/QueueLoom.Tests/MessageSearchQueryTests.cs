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

    // A JSON condition looked inside packed bodies while text and regular-expression search read only the raw bytes as
    // UTF-8: a gzip or base64 dead letter about order SHIP-77 was found by $.order.id but not by typing SHIP-77.
    [Theory]
    [InlineData("gzip", "ship-77", true)]
    [InlineData("gzip", "/SHIP-\\d+/", true)]
    [InlineData("gzip", "ORD-9999", false)]
    [InlineData("base64", "ship-77", true)]
    [InlineData("base64", "/SHIP-\\d+/", true)]
    [InlineData("base64", "ORD-9999", false)]
    public void Text_search_looks_inside_packed_bodies(string packing, string query, bool expected)
    {
        var json = Encoding.UTF8.GetBytes("""{"order": {"id": "SHIP-77", "status": "failed"}}""");
        byte[] raw;
        if (packing == "gzip")
        {
            using var packed = new MemoryStream();
            using (var gzip = new GZipStream(packed, CompressionLevel.Fastest, leaveOpen: true))
            {
                gzip.Write(json);
            }
            raw = packed.ToArray();
        }
        else
        {
            raw = Encoding.ASCII.GetBytes(Convert.ToBase64String(json));
        }
        // No other field mentions the order, so only the body can match.
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1, raw,
            new EditableMessageProperties(MessageId: "m-1"));

        Assert.Equal(expected, MessageSearchQuery.Parse(query).Matches(message));
    }

    // Decoding stops at 16 MiB. The cut text ends where the limit fell, so an end anchor matched it although the whole
    // body ends otherwise, and a term beyond the cut was reported as not matching. A literal text found in the part read
    // is in the body; anything else on a cut body is undecided, like a regular expression that ran out of time.
    [Theory]
    [InlineData("/!\\z/", null)]
    [InlineData("SHIP-77", null)]
    [InlineData("/SHIP-77/", null)]
    [InlineData("!!!!", null)]
    [InlineData("$.order == 'x'", null)]
    public void A_body_cut_while_decoding_decides_only_what_its_start_proves(string query, bool? expected)
    {
        using var packed = new MemoryStream();
        using (var gzip = new GZipStream(packed, CompressionLevel.Fastest, leaveOpen: true))
        {
            gzip.Write(Encoding.ASCII.GetBytes(new string('!', BodyDecoder.MaximumDecodedBytes) + "SHIP-77"));
        }
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1, packed.ToArray(),
            new EditableMessageProperties(MessageId: "m-1"));
        var search = MessageSearchQuery.Parse(query);

        if (expected is { } match)
        {
            Assert.Equal(match, search.Matches(message));
        }
        else
        {
            Assert.Throws<IncompleteBodySearchException>(() => search.Matches(message));
        }
    }

    // What a cut body's preview shows is not the body: the cut can split a character (the preview falls back to a hex
    // view), an Avro record can pass the output budget (decoding fails), and a partial Avro preview adds its own closing
    // brackets. Each of these left the search with a definite answer; each is now undecided.
    [Theory]
    [InlineData("split-character", "/SHIP-77\\z/")]
    [InlineData("split-character", "SHIP-77")]
    [InlineData("avro-budget", "$[0]")]
    [InlineData("avro-budget", "x")]
    [InlineData("avro-records", "0")]
    [InlineData("avro-records", "$[1000] == 1")]
    public void A_cut_body_is_undecided_whatever_its_preview_looks_like(string shape, string query)
    {
        static byte[] ZigZag(long value)
        {
            var zigzag = (ulong)((value << 1) ^ (value >> 63));
            var bytes = new List<byte>();
            do { var next = (byte)(zigzag & 0x7F); zigzag >>= 7; bytes.Add(zigzag == 0 ? next : (byte)(next | 0x80)); }
            while (zigzag != 0);
            return bytes.ToArray();
        }
        byte[] body;
        if (shape == "split-character")
        {
            using var packed = new MemoryStream();
            using (var gzip = new GZipStream(packed, CompressionLevel.Fastest, leaveOpen: true))
            {
                gzip.Write(Encoding.UTF8.GetBytes(new string('!', BodyDecoder.MaximumDecodedBytes - 1) + "€SHIP-77"));
            }
            body = packed.ToArray();
        }
        else if (shape == "avro-budget")
        {
            // 3 MiB of U+0000 is 18 MiB once escaped as JSON.
            var text = new byte[3 * 1024 * 1024];
            body = BodyDecoderTests.AvroFile("\"string\"", "deflate", 1, [.. ZigZag(text.Length), .. text]);
        }
        else
        {
            // 1,000 zeros and then a 1: the preview stops at the zeros and closes the array itself.
            body = BodyDecoderTests.AvroFile("\"long\"", "null", BodyDecoder.MaximumAvroRecords + 1,
                [.. Enumerable.Repeat((byte)0, BodyDecoder.MaximumAvroRecords), .. ZigZag(1)]);
        }
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1, body,
            new EditableMessageProperties(MessageId: "m-1"));

        Assert.False(BodyDecoder.Decode(body)!.IsComplete);
        Assert.Throws<IncompleteBodySearchException>(() => MessageSearchQuery.Parse(query).Matches(message));
    }

    // The records a partial Avro preview does contain are real: a JSON condition they satisfy is a real match.
    [Fact]
    public void A_json_condition_met_by_a_decoded_avro_record_matches_although_later_records_were_not_read()
    {
        var body = BodyDecoderTests.AvroFile("\"long\"", "null", BodyDecoder.MaximumAvroRecords + 1,
            [.. Enumerable.Repeat((byte)0, BodyDecoder.MaximumAvroRecords), 2]);
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1, body,
            new EditableMessageProperties(MessageId: "m-1"));

        Assert.True(MessageSearchQuery.Parse("$[0] == 0").Matches(message));
    }

    [Fact]
    public void A_term_in_another_field_still_matches_a_message_whose_body_was_cut()
    {
        using var packed = new MemoryStream();
        using (var gzip = new GZipStream(packed, CompressionLevel.Fastest, leaveOpen: true))
        {
            gzip.Write(new byte[BodyDecoder.MaximumDecodedBytes + 10]);
        }
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1, packed.ToArray(),
            new EditableMessageProperties(MessageId: "m-1", Subject: "SHIP-77"));

        Assert.True(MessageSearchQuery.Parse("/SHIP-\\d+/").Matches(message));
    }

    [Fact]
    public void Undecided_messages_are_counted_by_cause_in_the_source_error()
    {
        Assert.Null(DeadLetterSearchSourceResult.UndecidedError(0, 0));
        Assert.StartsWith("2 message(s) could not be checked: the regular expression", DeadLetterSearchSourceResult.UndecidedError(2, 0));
        var both = DeadLetterSearchSourceResult.UndecidedError(1, 3)!;
        Assert.Contains("3 message(s) could not be checked: their decoded body is longer than 16 MiB", both, StringComparison.Ordinal);
        Assert.Contains("regular expression took too long", both, StringComparison.Ordinal);
        Assert.Equal(DeadLetterSearchSourceResult.UndecidedError(4, 0), DeadLetterSearchSourceResult.RegexTimeoutError(4));
    }

    // A binary body is decoded into a hex dump for display; searching it would match bytes written as hex digits.
    [Fact]
    public void Text_search_does_not_match_the_hex_dump_of_a_binary_body()
    {
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1,
            Convert.FromBase64String(Convert.ToBase64String([0x1f, 0x8b, 0x00, 0xde, 0xad, 0xbe, 0xef, 0x01])),
            new EditableMessageProperties(MessageId: "m-1"));

        Assert.False(MessageSearchQuery.Parse("DEAD").Matches(message));
        Assert.False(MessageSearchQuery.Parse("/de ?ad/i").Matches(message));
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
