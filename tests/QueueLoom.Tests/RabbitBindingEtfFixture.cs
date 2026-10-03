using System.Buffers.Binary;
using System.Numerics;
using System.Text;

namespace QueueLoom.Tests;

// Produces independently specified ETF wire data for the fake management server.
internal static class RabbitBindingEtfFixture
{
    internal static byte[] Encode(object? value)
    {
        using var bytes = new MemoryStream();
        bytes.WriteByte(131);
        Write(value);
        return bytes.ToArray();

        void Int32(uint number)
        {
            var buffer = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(buffer, number);
            bytes.Write(buffer);
        }
        void Write(object? term)
        {
            switch (term)
            {
                case null: Atom("undefined"); break;
                case bool flag: Atom(flag ? "true" : "false"); break;
                case string text:
                    var binary = Encoding.UTF8.GetBytes(text);
                    bytes.WriteByte(109); Int32((uint)binary.Length); bytes.Write(binary); break;
                case long number:
                    var big = BigInteger.Abs(new BigInteger(number)).ToByteArray(isUnsigned: true, isBigEndian: false);
                    bytes.WriteByte(110); bytes.WriteByte((byte)big.Length); bytes.WriteByte(number < 0 ? (byte)1 : (byte)0); bytes.Write(big); break;
                case double number:
                    var floating = new byte[8];
                    BinaryPrimitives.WriteDoubleBigEndian(floating, number);
                    bytes.WriteByte(70); bytes.Write(floating); break;
                case Dictionary<string, object?> map:
                    bytes.WriteByte(116); Int32((uint)map.Count);
                    foreach (var pair in map) { Write(pair.Key); Write(pair.Value); }
                    break;
                case object?[] array:
                    bytes.WriteByte(108); Int32((uint)array.Length);
                    foreach (var item in array) Write(item);
                    bytes.WriteByte(106); break;
                default: throw new InvalidOperationException($"Unsupported test ETF value {term.GetType()}");
            }
        }
        void Atom(string text)
        {
            var atom = Encoding.UTF8.GetBytes(text);
            bytes.WriteByte(119); bytes.WriteByte((byte)atom.Length); bytes.Write(atom);
        }
    }
}
