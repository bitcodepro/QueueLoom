using System.IO.Compression;
using System.Text;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// GZipStream and ZLibStream often just stop at a cut instead of throwing, so the first half of a compressed body was
// shown as complete. And a plain-text token starting with "eJ" decodes from Base64 to bytes that pass the zlib header
// check; the text was replaced by a hex dump of those bytes.
public sealed class BodyDecoderTruncationTests
{
    private static readonly string Json = "{" + string.Join(",", Enumerable.Range(0, 400).Select(index => $"\"key{index}\":\"value number {index * 7919 % 1000}\"")) + "}";

    private static byte[] Compress(string text, bool gzip)
    {
        using var output = new MemoryStream();
        using (Stream stream = gzip
                   ? new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true)
                   : new ZLibStream(output, CompressionLevel.Optimal, leaveOpen: true))
        {
            stream.Write(Encoding.UTF8.GetBytes(text));
        }
        return output.ToArray();
    }

    [Theory]
    [InlineData(true, 2)]
    [InlineData(true, 3)]
    [InlineData(true, 10)]
    [InlineData(false, 2)]
    [InlineData(false, 3)]
    [InlineData(false, 10)]
    public void CutCompressedBodyIsNotShownAsComplete(bool gzip, int fraction)
    {
        var full = Compress(Json, gzip);
        var cut = full[..(full.Length - full.Length / fraction)];

        var decoded = BodyDecoder.Decode(cut);

        Assert.NotNull(decoded);
        Assert.Equal(gzip ? "gzip" : "zlib", decoded.Steps[0]);
        Assert.False(decoded.IsComplete);
        Assert.Contains("cut off", decoded.Note, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true, 1)]
    [InlineData(true, 4)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    public void BodyMissingOnlyItsTrailerIsNotShownAsComplete(bool gzip, int missing)
    {
        var full = Compress(Json, gzip);

        var decoded = BodyDecoder.Decode(full[..^missing]);

        Assert.NotNull(decoded);
        Assert.False(decoded.IsComplete);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IntactCompressedBodyIsComplete(bool gzip)
    {
        var decoded = BodyDecoder.Decode(Compress(Json, gzip));

        Assert.NotNull(decoded);
        Assert.Equal([gzip ? "gzip" : "zlib", "JSON"], decoded.Steps);
        Assert.True(decoded.IsComplete);
        Assert.Null(decoded.Note);
    }

    [Fact]
    public void ConcatenatedGzipMembersAreComplete()
    {
        var decoded = BodyDecoder.Decode(Compress("first part, ", true).Concat(Compress("second part", true)).ToArray());

        Assert.NotNull(decoded);
        Assert.True(decoded.IsComplete);
        Assert.Equal("first part, second part", decoded.Text);
    }

    // A second gzip header and the start of a stored block whose last eight bytes are zero: those zeros are the CRC-32
    // and length of empty output, so a check of the final eight bytes took the unfinished member for an empty one.
    private static readonly byte[] UnfinishedEmptyLookingMember = Convert.FromHexString("1f8b0800000000000000000000");

    [Fact]
    public void UnfinishedMemberWhoseLastBytesAreZeroIsNotComplete()
    {
        var decoded = BodyDecoder.Decode((byte[])[.. Compress("first part", true), .. UnfinishedEmptyLookingMember]);

        Assert.NotNull(decoded);
        Assert.False(decoded.IsComplete);
        Assert.Contains("cut off", decoded.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void SearchIsUndecidedOnAnUnfinishedMemberWhoseLastBytesAreZero()
    {
        var body = Compress("first part", true).Concat(UnfinishedEmptyLookingMember).ToArray();
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1, body,
            new EditableMessageProperties(MessageId: "m-1"));

        Assert.Throws<IncompleteBodySearchException>(() => MessageSearchQuery.Parse("first").Matches(message));
    }

    [Fact]
    public void GenuineEmptyGzipMemberAppendedIsComplete()
    {
        var decoded = BodyDecoder.Decode((byte[])[.. Compress("first part", true), .. Compress(string.Empty, true)]);

        Assert.NotNull(decoded);
        Assert.True(decoded.IsComplete);
        Assert.Equal("first part", decoded.Text);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TrailingBytesAfterTheLastMemberAreNotComplete(bool gzip)
    {
        var decoded = BodyDecoder.Decode((byte[])[.. Compress(Json, gzip), 0x00, 0x00]);

        Assert.NotNull(decoded);
        Assert.False(decoded.IsComplete);
    }

    [Theory]
    [InlineData("eJxOrderNumber42")]
    [InlineData("eJwAbCdEfGhIjKl")]
    public void TextThatLooksLikeBase64OfZlibStaysText(string body)
    {
        var decoded = BodyDecoder.Decode(Encoding.ASCII.GetBytes(body));

        Assert.True(decoded is null || decoded.Text == body, decoded?.Text);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Base64OfCompressedJsonStillDecodes(bool gzip)
    {
        var body = Encoding.ASCII.GetBytes(Convert.ToBase64String(Compress("""{"orderId":42}""", gzip)));

        var decoded = BodyDecoder.Decode(body);

        Assert.NotNull(decoded);
        Assert.Equal(["base64", gzip ? "gzip" : "zlib", "JSON"], decoded.Steps);
        Assert.True(decoded.IsComplete);
        Assert.Contains("\"orderId\": 42", decoded.Text, StringComparison.Ordinal);
    }

    private static BrowsedMessage Message(byte[] body) => new(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1,
        body, new EditableMessageProperties(MessageId: "m-1"));

    // The JSON of a complete first member is only a prefix of the body: what the unfinished rest holds can change the answer.
    [Fact]
    public void JsonSearchIsUndecidedOnACompleteMemberBeforeAnUnfinishedOne()
    {
        var body = Compress("""{"status":"failed"}""", true).Concat(UnfinishedEmptyLookingMember).ToArray();

        Assert.Throws<IncompleteBodySearchException>(() => MessageSearchQuery.Parse("$.status == 'failed'").Matches(Message(body)));
    }

    [Fact]
    public void JsonSearchIsUndecidedWhenTheGzipChecksumIsDamaged()
    {
        var body = Compress("""{"status":"failed"}""", true);
        body[^8] ^= 0xFF;

        Assert.Throws<IncompleteBodySearchException>(() => MessageSearchQuery.Parse("$.status == 'failed'").Matches(Message(body)));
    }

    [Fact]
    public void JsonSearchStillMatchesCompleteGzipJson()
    {
        var body = Compress("""{"status":"failed"}""", true);

        Assert.True(MessageSearchQuery.Parse("$.status == 'failed'").Matches(Message(body)));
        Assert.False(MessageSearchQuery.Parse("$.status == 'done'").Matches(Message(body)));
    }

    // FHCRC: the low 16 bits of the CRC-32 of the header, from ID1 up to the CRC itself.
    private static byte[] WithHeaderCrc(string text, int flip = 0)
    {
        var plain = Compress(text, true);
        var header = plain[..10];
        header[3] = 2;
        var crc = (ushort)(Crc32(header) ^ flip);
        return [.. header, (byte)crc, (byte)(crc >> 8), .. plain[10..]];
    }

    private static uint Crc32(byte[] data)
    {
        var crc = 0xFFFFFFFFu;
        foreach (var value in data)
        {
            crc ^= value;
            for (var bit = 0; bit < 8; bit++)
            {
                crc = (crc & 1) != 0 ? 0xEDB88320 ^ crc >> 1 : crc >> 1;
            }
        }
        return ~crc;
    }

    [Fact]
    public void GzipHeaderCrcIsChecked()
    {
        var valid = BodyDecoder.Decode(WithHeaderCrc("first part"));
        Assert.NotNull(valid);
        Assert.True(valid.IsComplete);
        Assert.Equal("first part", valid.Text);

        // A damaged header is no gzip at all, as with any other header damage.
        var damaged = BodyDecoder.Decode(WithHeaderCrc("first part", 1));
        Assert.True(damaged is null || damaged.Steps[0] != "gzip");

        // A second member whose header CRC is wrong is damage after the first.
        var second = BodyDecoder.Decode((byte[])[.. Compress("first part", true), .. WithHeaderCrc("second", 1)]);
        Assert.NotNull(second);
        Assert.False(second.IsComplete);

        // Cut inside the header CRC: cut off.
        var cut = BodyDecoder.Decode((byte[])[.. Compress("first part", true), .. WithHeaderCrc("second")[..11]]);
        Assert.NotNull(cut);
        Assert.False(cut.IsComplete);
        Assert.Contains("cut off", cut.Note, StringComparison.Ordinal);
    }
}
