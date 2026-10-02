using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class ProtoEnumBoundaryRegressionTests
{
    [Theory]
    [InlineData("-0x80000000", int.MinValue)]
    [InlineData("-0X80000000", int.MinValue)]
    [InlineData("-2147483648", int.MinValue)]
    [InlineData("2147483647", int.MaxValue)]
    [InlineData("0x7fffffff", int.MaxValue)]
    [InlineData("-2147483647", -2147483647)]
    [InlineData("-0x7fffffff", -2147483647)]
    [InlineData("-1", -1)]
    [InlineData("-0x1", -1)]
    [InlineData("0", 0)]
    public void SignedInt32Boundaries_LoadWithExactValues(string literal, int expected)
    {
        var schema = Load(literal);
        Assert.Equal(expected == 0 ? "UNKNOWN" : "VALUE", schema.FindEnum("State")!.Values[expected]);
        Assert.NotNull(schema.FindMessage("Sample"));
    }

    [Theory]
    [InlineData("2147483648")]
    [InlineData("0x80000000")]
    [InlineData("-2147483649")]
    [InlineData("-0x80000001")]
    [InlineData("0xffffffff")]
    [InlineData("-0xffffffff")]
    [InlineData("0x100000000")]
    [InlineData("0xffffffffffffffff")]
    [InlineData("-0x10000000000000000")]
    [InlineData("99999999999999999999999999999")]
    [InlineData("0x")]
    [InlineData("-0x")]
    [InlineData("0xGG")]
    [InlineData("-0xGG")]
    public void MalformedOrOutOfRangeEnumValues_AreControlledSchemaErrors(string literal)
    {
        var error = Assert.Throws<ProtoSchemaException>(() => Load(literal));
        Assert.Contains("boundary.proto", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("-0x80000000")]
    [InlineData("0x80000000")]
    [InlineData("2147483648")]
    public void EnumMagnitudeAllowance_DoesNotAllowNegativeOrOverflowingFieldNumbers(string literal)
    {
        Assert.Throws<ProtoSchemaException>(() => ProtoSchemaSet.FromProtoFiles(
            [("field.proto", $"message Sample {{ string value = {literal}; }}")]));
    }

    private static ProtoSchemaSet Load(string literal) => ProtoSchemaSet.FromProtoFiles(
        [("boundary.proto", $"syntax = \"proto3\"; enum State {{ UNKNOWN = 0; VALUE = {literal}; }} message Sample {{ State state = 1; }}")]);
}
