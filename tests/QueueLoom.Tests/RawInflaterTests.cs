using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// BodyDecoder decodes DEFLATE itself, to know where each stream ends. Its output has to match what real encoders wrote.
public sealed class RawInflaterTests
{
    public static TheoryData<string, int, CompressionLevel> Inputs()
    {
        var data = new TheoryData<string, int, CompressionLevel>();
        foreach (var shape in new[] { "random", "text", "mixed" })
        {
            foreach (var size in new[] { 0, 1, 65_535, 65_536, 65_537, 1024 * 1024 })
            {
                foreach (var level in Enum.GetValues<CompressionLevel>())
                {
                    data.Add(shape, size, level);
                }
            }
        }
        return data;
    }

    private static byte[] Input(string shape, int size)
    {
        var random = new Random(size * 31 + shape.Length);
        var bytes = new byte[size];
        if (shape == "random")
        {
            random.NextBytes(bytes);
            return bytes;
        }
        var text = Encoding.ASCII.GetBytes(string.Join(" ", Enumerable.Range(0, size / 4 + 1).Select(index => $"order{index * 7919 % 1013}")));
        for (var index = 0; index < size; index++)
        {
            // Mixed: runs of noise between the text, so blocks switch between literals and matches.
            bytes[index] = shape == "mixed" && index / 3000 % 2 == 1 ? (byte)random.Next(256) : text[index];
        }
        return bytes;
    }

    private static byte[] Compress(byte[] data, CompressionLevel level, string format)
    {
        using var output = new MemoryStream();
        using (Stream stream = format switch
               {
                   "gzip" => new GZipStream(output, level, leaveOpen: true),
                   "zlib" => new ZLibStream(output, level, leaveOpen: true),
                   _ => new DeflateStream(output, level, leaveOpen: true),
               })
        {
            stream.Write(data);
        }
        return output.ToArray();
    }

    [Theory]
    [MemberData(nameof(Inputs))]
    public void InflatesWhatDotNetCompressed(string shape, int size, CompressionLevel level)
    {
        var original = Input(shape, size);

        var raw = Compress(original, level, "deflate");
        if (raw.Length == 0)
        {
            // DeflateStream writes nothing at all for empty input, not even an empty final block.
            raw = [0x03, 0x00];
        }
        var inflater = new RawInflater(raw, BodyDecoder.MaximumDecodedBytes);
        Assert.Equal(InflateResult.Done, inflater.Inflate(0, out var end));
        Assert.Equal(raw.Length, end);
        Assert.True(inflater.Output.SequenceEqual(original));

        foreach (var format in new[] { "gzip", "zlib" })
        {
            var decoded = BodyDecoder.Decode(Compress(original, level, format));
            if (size == 0)
            {
                // Empty output is not taken for a compressed body.
                Assert.True(decoded is null || decoded.Steps[0] != format);
                continue;
            }
            Assert.NotNull(decoded);
            Assert.Equal(format, decoded.Steps[0]);
            Assert.True(decoded.IsComplete, format);
        }
    }

    // Written by Python's zlib at level 9 (dynamic Huffman blocks, back-references up to ~25 KiB away); see the
    // generator in the commit that added it. 1,331,543 bytes once inflated.
    [Theory]
    [InlineData("inflate-python.gz", 10)]
    [InlineData("inflate-python.zlib", 2)]
    public void InflatesWhatPythonCompressed(string file, int start)
    {
        var input = File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "Fixtures", file));
        var inflater = new RawInflater(input, BodyDecoder.MaximumDecodedBytes);

        Assert.Equal(InflateResult.Done, inflater.Inflate(start, out var end));
        Assert.Equal(input.Length - (start == 10 ? 8 : 4), end);
        Assert.Equal(1_331_543, inflater.Length);
        Assert.Equal("ab4aabf8060047b3de73984e59de53f4fd3826ebca5d098a9c2c63ff15ebd0b5",
            Convert.ToHexStringLower(SHA256.HashData(inflater.Output)));
        Assert.True(BodyDecoder.Decode(input)!.IsComplete);
    }

    // Dynamic blocks with hand-picked code lengths, checked against Python's zlib. As in zlib's puff.c, the code-length
    // code must be complete; a literal/length or distance code may be incomplete only as a single code of length 1.
    [Theory]
    [InlineData("05e0010800000000200000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000088")] // incomplete code-length code
    [InlineData("0de1010400000080200000000000000000080000000000000000000000000000000000000000000000f83701")] // two distance codes of length 2
    [InlineData("05c00104000000802000000000000000000300000000000000000000000000000000000000000000008700")] // 'A' and end of block both length 2
    public void RejectsIncompleteHuffmanCodes(string hex)
    {
        Assert.Equal(InflateResult.Invalid, new RawInflater(Convert.FromHexString(hex), BodyDecoder.MaximumDecodedBytes).Inflate(0, out _));
    }

    [Fact]
    public void IncompleteLiteralCodeInZlibIsNotDecoded()
    {
        var decoded = BodyDecoder.Decode(Convert.FromHexString("789c05c00104000000802000000000000000000300000000000000000000000000000000000000000000008700420042"));

        Assert.True(decoded is null || decoded.Steps[0] != "zlib" || !decoded.IsComplete);
    }

    [Theory]
    [InlineData("0de0010400000080200000000000000000080000000000000000000000000000000000000000000000f82c", "AAAA")] // one distance code, length 1
    [InlineData("05e001040000000010000000000000000008000000000000000000000000000000000000000000000044", "AA")] // no distance codes
    public void AcceptsTheDegenerateCodesZlibAllows(string hex, string expected)
    {
        var inflater = new RawInflater(Convert.FromHexString(hex), BodyDecoder.MaximumDecodedBytes);

        Assert.Equal(InflateResult.Done, inflater.Inflate(0, out _));
        Assert.Equal(expected, Encoding.ASCII.GetString(inflater.Output));
    }
}
