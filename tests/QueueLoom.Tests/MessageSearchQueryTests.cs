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
    public void A_search_request_reads_its_query_once_and_rejects_a_broken_one()
    {
        var target = new DeadLetterSearchTarget(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1);
        Assert.True(new DeadLetterSearchRequest("$.order.status == 'failed'", [target]).Search.IsJsonCondition);
        Assert.Throws<MessageSearchQueryException>(() => new DeadLetterSearchRequest("/(/", [target]));
    }
}
