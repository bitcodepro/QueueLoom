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
    private ulong _bitBuffer;
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
            end = _position - _bitCount / 8;
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

    /// <summary>Tops the bit buffer up to at least 57 bits, or with whatever input is left.</summary>
    private void Refill()
    {
        while (_bitCount <= 56 && _position < _input.Length)
        {
            _bitBuffer |= (ulong)_input[_position++] << _bitCount;
            _bitCount += 8;
        }
    }

    private int Bits(int need)
    {
        if (_bitCount < need)
        {
            Refill();
            if (_bitCount < need)
            {
                throw new CutOffException();
            }
        }
        var value = (int)(_bitBuffer & ((1UL << need) - 1));
        _bitBuffer >>= need;
        _bitCount -= need;
        return value;
    }

    /// <summary>Room for <paramref name="count"/> more bytes; what fits under the limit, when they do not all fit.</summary>
    private int Reserve(int count)
    {
        var room = Math.Min(count, _limit - Length);
        if (Length + room > _output.Length)
        {
            Array.Resize(ref _output, (int)Math.Min(Math.Max((long)_output.Length * 2, Length + room), _limit));
        }
        return room;
    }

    private void Put(byte value)
    {
        if (Reserve(1) == 0)
        {
            throw new CappedException();
        }
        _output[Length++] = value;
    }

    private void Stored()
    {
        // The rest of the current byte is padding; whole bytes already in the bit buffer go back to the input.
        _position -= _bitCount / 8;
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
        var available = Math.Min(length, _input.Length - _position);
        var room = Reserve(available);
        _input.AsSpan(_position, room).CopyTo(_output.AsSpan(Length));
        Length += room;
        _position += room;
        if (room < available)
        {
            throw new CappedException();
        }
        if (available < length)
        {
            throw new CutOffException();
        }
    }

    /// <summary>A canonical Huffman code: how many codes have each length, and the symbols in code order.</summary>
    private sealed class Huffman(short[] counts, short[] symbols, int[] fast)
    {
        public const int FastBits = 9;

        public short[] Counts { get; } = counts;

        public short[] Symbols { get; } = symbols;

        /// <summary>By the next nine input bits: symbol &lt;&lt; 4 | code length, or 0 for a longer (or unused) code.</summary>
        public int[] Fast { get; } = fast;

        /// <summary>
        /// As zlib's puff.c: a code with unused code space is invalid unless <paramref name="single"/> allows the one
        /// degenerate case (a single code of length 1, or no codes at all), or <paramref name="anyIncomplete"/> is set
        /// (the fixed distance code, whose 30 codes leave two unused).
        /// </summary>
        public static Huffman Build(ReadOnlySpan<byte> lengths, bool single = false, bool anyIncomplete = false)
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
            if (left > 0 && !anyIncomplete && counts[0] != lengths.Length &&
                !(single && counts[1] == 1 && counts[0] + 1 == lengths.Length))
            {
                // Incomplete: some bit patterns decode to nothing.
                throw new InvalidDataException();
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
            // Codes are read a bit at a time from the low end, so the table is indexed by each code reversed.
            var fast = new int[1 << FastBits];
            int code = 0, next = 0;
            for (var length = 1; length <= FastBits; length++)
            {
                for (var index = 0; index < counts[length]; index++, code++)
                {
                    var reversed = 0;
                    for (var bit = 0; bit < length; bit++)
                    {
                        reversed |= (code >> bit & 1) << (length - 1 - bit);
                    }
                    for (var fill = reversed; fill < fast.Length; fill += 1 << length)
                    {
                        fast[fill] = symbols[next + index] << 4 | length;
                    }
                }
                next += counts[length];
                code <<= 1;
            }
            return new Huffman(counts, symbols, fast);
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

    private static readonly Lazy<Huffman> FixedDistances = new(() => Huffman.Build(Enumerable.Repeat((byte)5, 30).ToArray(), anyIncomplete: true));

    private int Decode(Huffman huffman)
    {
        if (_bitCount < Huffman.FastBits)
        {
            Refill();
        }
        if (_bitCount >= Huffman.FastBits && huffman.Fast[(int)(_bitBuffer & ((1 << Huffman.FastBits) - 1))] is var entry and not 0)
        {
            _bitBuffer >>= entry & 15;
            _bitCount -= entry & 15;
            return entry >> 4;
        }
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
            var room = Reserve(length);
            if (distance >= room)
            {
                _output.AsSpan(Length - distance, room).CopyTo(_output.AsSpan(Length));
                Length += room;
            }
            else
            {
                // An overlapping copy repeats what it has just written.
                for (var copied = 0; copied < room; copied++, Length++)
                {
                    _output[Length] = _output[Length - distance];
                }
            }
            if (room < length)
            {
                throw new CappedException();
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
        Codes(Huffman.Build(lengths.AsSpan(0, lengthCount), single: true), Huffman.Build(lengths.AsSpan(lengthCount), single: true));
    }

    private sealed class CutOffException : Exception;

    private sealed class CappedException : Exception;
}
