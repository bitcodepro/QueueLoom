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
}
