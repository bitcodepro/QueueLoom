namespace QueueLoom.Core.ServiceBus;

/// <summary>How a DEFLATE stream ended.</summary>
internal enum InflateResult
{
    /// <summary>The final block ended; the stream's last byte is just before the end position.</summary>
    Done,

    /// <summary>The input ran out before the final block ended: the stream is cut off.</summary>
    CutOff,

    /// <summary>The output reached its limit first.</summary>
    Capped,

    /// <summary>Not a valid DEFLATE stream.</summary>
    Invalid,
}

/// <summary>
/// A small DEFLATE decoder (RFC 1951, after zlib's puff). DeflateStream reads ahead of the end of the stream and stops
/// quietly at a cut, so it cannot say where a gzip member or zlib stream ends; this decoder reports the exact byte, so
/// the trailer after it can be checked.
/// </summary>
internal sealed class RawInflater
{
    private static readonly short[] LengthBase = [3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258];
    private static readonly short[] LengthExtra = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0];
    private static readonly short[] DistanceBase = [1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577];
    private static readonly short[] DistanceExtra = [0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13];
    private static readonly byte[] CodeLengthOrder = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15];

    private readonly byte[] _input;
    private readonly int _limit;
    private byte[] _output = new byte[4096];
    private int _position;
    private int _bitBuffer;
    private int _bitCount;
    private int _streamStart;

    public RawInflater(byte[] input, int limit)
    {
        _input = input;
        _limit = limit;
    }

    /// <summary>Everything decoded so far, over all the streams.</summary>
    public int Length { get; private set; }

    public ReadOnlySpan<byte> Output => _output.AsSpan(0, Length);

    /// <summary>Decodes one DEFLATE stream from <paramref name="start"/>, after what earlier streams produced.</summary>
    public InflateResult Inflate(int start, out int end)
    {
        _position = start;
        _bitBuffer = 0;
        _bitCount = 0;
        _streamStart = Length;
        end = start;
        try
        {
            bool last;
            do
            {
                last = Bits(1) == 1;
                switch (Bits(2))
                {
                    case 0:
                        Stored();
                        break;
                    case 1:
                        Codes(FixedLengths.Value, FixedDistances.Value);
                        break;
                    case 2:
                        Dynamic();
                        break;
                    default:
                        return InflateResult.Invalid;
                }
            }
            while (!last);
            // Bits left over in the last byte are padding; the next byte is the trailer.
            end = _position;
            return InflateResult.Done;
        }
        catch (CutOffException)
        {
            return InflateResult.CutOff;
        }
        catch (CappedException)
        {
            return InflateResult.Capped;
        }
        catch (InvalidDataException)
        {
            return InflateResult.Invalid;
        }
    }

    private int Bits(int need)
    {
        var value = _bitBuffer;
        while (_bitCount < need)
        {
            if (_position >= _input.Length)
            {
                throw new CutOffException();
            }
            value |= _input[_position++] << _bitCount;
            _bitCount += 8;
        }
        _bitBuffer = value >> need;
        _bitCount -= need;
        return value & ((1 << need) - 1);
    }

    private void Put(byte value)
    {
        if (Length >= _limit)
        {
            throw new CappedException();
        }
        if (Length == _output.Length)
        {
            Array.Resize(ref _output, (int)Math.Min((long)_output.Length * 2, _limit));
        }
        _output[Length++] = value;
    }

    private void Stored()
    {
        _bitBuffer = 0;
        _bitCount = 0;
        if (_position + 4 > _input.Length)
        {
            throw new CutOffException();
        }
        var length = _input[_position] | _input[_position + 1] << 8;
        if ((_input[_position + 2] | _input[_position + 3] << 8) != (~length & 0xFFFF))
        {
            throw new InvalidDataException();
        }
        _position += 4;
        for (var index = 0; index < length; index++)
        {
            if (_position >= _input.Length)
            {
                throw new CutOffException();
            }
            Put(_input[_position++]);
        }
    }

    /// <summary>A canonical Huffman code: how many codes have each length, and the symbols in code order.</summary>
    private sealed class Huffman(short[] counts, short[] symbols)
    {
        public short[] Counts { get; } = counts;

        public short[] Symbols { get; } = symbols;

        public static Huffman Build(ReadOnlySpan<byte> lengths)
        {
            var counts = new short[16];
            foreach (var length in lengths)
            {
                counts[length]++;
            }
            var left = 1;
            for (var length = 1; length < 16; length++)
            {
                left = (left << 1) - counts[length];
                if (left < 0)
                {
                    // Over-subscribed: more codes than the lengths allow.
                    throw new InvalidDataException();
                }
            }
            var offsets = new short[16];
            for (var length = 1; length < 15; length++)
            {
                offsets[length + 1] = (short)(offsets[length] + counts[length]);
            }
            var symbols = new short[lengths.Length];
            for (var symbol = 0; symbol < lengths.Length; symbol++)
            {
                if (lengths[symbol] != 0)
                {
                    symbols[offsets[lengths[symbol]]++] = (short)symbol;
                }
            }
            return new Huffman(counts, symbols);
        }
    }

    private static readonly Lazy<Huffman> FixedLengths = new(() =>
    {
        var lengths = new byte[288];
        lengths.AsSpan(0, 144).Fill(8);
        lengths.AsSpan(144, 112).Fill(9);
        lengths.AsSpan(256, 24).Fill(7);
        lengths.AsSpan(280, 8).Fill(8);
        return Huffman.Build(lengths);
    });

    private static readonly Lazy<Huffman> FixedDistances = new(() => Huffman.Build(Enumerable.Repeat((byte)5, 30).ToArray()));

    private int Decode(Huffman huffman)
    {
        int code = 0, first = 0, index = 0;
        for (var length = 1; length < 16; length++)
        {
            code |= Bits(1);
            var count = huffman.Counts[length];
            if (code - count < first)
            {
                return huffman.Symbols[index + (code - first)];
            }
            index += count;
            first = (first + count) << 1;
            code <<= 1;
        }
        // A code the lengths left unused.
        throw new InvalidDataException();
    }

    private void Codes(Huffman lengths, Huffman distances)
    {
        while (true)
        {
            var symbol = Decode(lengths);
            if (symbol < 256)
            {
                Put((byte)symbol);
                continue;
            }
            if (symbol == 256)
            {
                return;
            }
            symbol -= 257;
            if (symbol >= 29)
            {
                throw new InvalidDataException();
            }
            var length = LengthBase[symbol] + Bits(LengthExtra[symbol]);
            symbol = Decode(distances);
            if (symbol >= 30)
            {
                throw new InvalidDataException();
            }
            var distance = DistanceBase[symbol] + Bits(DistanceExtra[symbol]);
            // A stream only refers back into its own output, never into an earlier gzip member's.
            if (distance > Length - _streamStart)
            {
                throw new InvalidDataException();
            }
            for (var copied = 0; copied < length; copied++)
            {
                Put(_output[Length - distance]);
            }
        }
    }

    private void Dynamic()
    {
        var lengthCount = Bits(5) + 257;
        var distanceCount = Bits(5) + 1;
        var codeCount = Bits(4) + 4;
        if (lengthCount > 286 || distanceCount > 30)
        {
            throw new InvalidDataException();
        }
        var codeLengths = new byte[19];
        for (var index = 0; index < codeCount; index++)
        {
            codeLengths[CodeLengthOrder[index]] = (byte)Bits(3);
        }
        var codeLengthCode = Huffman.Build(codeLengths);
        var lengths = new byte[lengthCount + distanceCount];
        for (var index = 0; index < lengths.Length;)
        {
            var symbol = Decode(codeLengthCode);
            if (symbol < 16)
            {
                lengths[index++] = (byte)symbol;
                continue;
            }
            byte value = 0;
            int repeat;
            if (symbol == 16)
            {
                if (index == 0)
                {
                    throw new InvalidDataException();
                }
                value = lengths[index - 1];
                repeat = 3 + Bits(2);
            }
            else
            {
                repeat = symbol == 17 ? 3 + Bits(3) : 11 + Bits(7);
            }
            if (index + repeat > lengths.Length)
            {
                throw new InvalidDataException();
            }
            lengths.AsSpan(index, repeat).Fill(value);
            index += repeat;
        }
        if (lengths[256] == 0)
        {
            // Without an end-of-block code the block could never end.
            throw new InvalidDataException();
        }
        Codes(Huffman.Build(lengths.AsSpan(0, lengthCount)), Huffman.Build(lengths.AsSpan(lengthCount)));
    }

    private sealed class CutOffException : Exception;

    private sealed class CappedException : Exception;
}
