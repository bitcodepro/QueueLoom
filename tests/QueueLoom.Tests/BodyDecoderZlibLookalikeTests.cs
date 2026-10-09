using System.Text;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// A few Base64 prefixes ("XG", "HK", "8O", "hC") are also a valid zlib header. Such a body was taken for zlib; inflating
// it failed, and decoding stopped there, so its Base64 was never unpacked. A failed decompression now lets the other
// layers be tried.
public sealed class BodyDecoderZlibLookalikeTests
{
    [Fact]
    public void Base64ThatLooksLikeAZlibHeaderIsStillDecoded()
    {
        // Base64 of "\documentclass{article} hello world": it starts with "XG", a valid zlib header.
        var body = Encoding.ASCII.GetBytes("XGRvY3VtZW50Y2xhc3N7YXJ0aWNsZX0gaGVsbG8gd29ybGQ=");

        var decoded = BodyDecoder.Decode(body);

        Assert.NotNull(decoded);
        Assert.Equal("base64", decoded.Steps[0]);
        Assert.Contains("documentclass{article} hello world", decoded.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void RealZlibIsStillUnpacked()
    {
        using var output = new MemoryStream();
        using (var zlib = new System.IO.Compression.ZLibStream(output, System.IO.Compression.CompressionLevel.Optimal, leaveOpen: true))
        {
            zlib.Write(Encoding.UTF8.GetBytes("""{"orderId":42}"""));
        }

        var decoded = BodyDecoder.Decode(output.ToArray());

        Assert.NotNull(decoded);
        Assert.Equal("zlib", decoded.Steps[0]);
        Assert.Contains("\"orderId\": 42", decoded.Text, StringComparison.Ordinal);
    }
}
