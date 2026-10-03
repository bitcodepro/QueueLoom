using System.Text.Json;
using System.Numerics;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Routing;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

public sealed class RabbitBindingWireTests
{
    [Theory]
    [InlineData("250", "250.0", false)]
    [InlineData("250.0", "250", false)]
    [InlineData("250.0", "2.5e2", true)]
    [InlineData("250.00", "250.0", true)]
    [InlineData("0", "-0.0", false)]
    [InlineData("-0.0", "0.0", true)]
    [InlineData("9007199254740993", "9007199254740992", false)]
    [InlineData("9007199254740993", "9007199254740993.0", false)]
    [InlineData("9223372036854775808", "9223372036854775809", false)]
    [InlineData("250", "\"250\"", false)]
    [InlineData("true", "\"true\"", false)]
    [InlineData("false", "true", false)]
    [InlineData("null", "\"null\"", false)]
    [InlineData("null", "\"undefined\"", false)]
    [InlineData("\"EU\"", "\"eu\"", false)]
    [InlineData("\"\\u0045U\"", "\"EU\"", true)]
    [InlineData("[250]", "[250.0]", false)]
    [InlineData("[1,2]", "[2,1]", false)]
    [InlineData("{\"amount\":250}", "{\"amount\":250.0}", false)]
    [InlineData("{\"b\":true,\"a\":null}", "{\"a\":null,\"b\":true}", true)]
    [InlineData("{\"A\":1}", "{\"a\":1}", false)]
    [InlineData("{\"a\":1,\"a\":2}", "{\"a\":1,\"a\":2}", false)]
    public void IdentityRespectsRabbitTypesAndCanonicalization(string a, string b, bool equal)
    {
        using var left = JsonDocument.Parse(a);
        using var right = JsonDocument.Parse(b);
        Assert.Equal(equal, RabbitMqBindingJson.Equal(left.RootElement, right.RootElement));
        Assert.Equal(equal, RabbitMqBindingJson.Equal(right.RootElement, left.RootElement));
    }

    [Fact]
    public void TypedBindingPayloadDistinguishesVoidBinaryTextAndFloatingTokens()
    {
        var encoded = RabbitBindingEtfFixture.Encode(new object?[] { new Dictionary<string, object?>
        {
            ["arguments"] = new Dictionary<string, object?>
            {
                ["trace"] = null, ["literal"] = "undefined", ["integer"] = 250L, ["floating"] = 250d,
                ["flag"] = false, ["unicode"] = "\u041f\u0440\u0438\u0432\u0435\u0442", ["pairs"] = new object?[] { new object?[] { "key", 1L } }
            }
        } });
        var arguments = Assert.Single(RabbitMqBindingTerms.Decode(encoded)).GetProperty("arguments");
        Assert.Equal(JsonValueKind.Null, arguments.GetProperty("trace").ValueKind);
        Assert.Equal("undefined", arguments.GetProperty("literal").GetString());
        Assert.Equal("250", arguments.GetProperty("integer").GetRawText());
        Assert.Equal("250.0", arguments.GetProperty("floating").GetRawText());
        Assert.False(arguments.GetProperty("flag").GetBoolean());
        Assert.Equal("\u041f\u0440\u0438\u0432\u0435\u0442", arguments.GetProperty("unicode").GetString());
        Assert.Equal(JsonValueKind.Array, arguments.GetProperty("pairs")[0].ValueKind);
    }

    [Fact]
    public void ErlangProplistAndMapHaveTheSameBindingProjection()
    {
        // [ [{arguments, #{<<"trace">> => undefined, <<"literal">> => <<"undefined">>}}] ]
        var wire = Convert.FromHexString("836C000000016C0000000168027709617267756D656E747374000000026D0000000574726163657709756E646566696E65646D000000076C69746572616C6D00000009756E646566696E65646A6A");
        var arguments = Assert.Single(RabbitMqBindingTerms.Decode(wire)).GetProperty("arguments");
        Assert.Equal(JsonValueKind.Null, arguments.GetProperty("trace").ValueKind);
        Assert.Equal("undefined", arguments.GetProperty("literal").GetString());
    }

    [Theory]
    [InlineData("")]
    [InlineData("83")]
    [InlineData("836CFFFFFFFF")]
    [InlineData("836C000000016D0000000161")]
    [InlineData("836A00")]
    [InlineData("837400000000")]
    [InlineData("8370")]
    [InlineData("8371")]
    public void MalformedTypedDataCannotSupplyABindingIdentity(string hex) =>
        Assert.Throws<InvalidOperationException>(() => RabbitMqBindingTerms.Decode(Convert.FromHexString(hex)));

    [Fact]
    public void ExcessiveTermDepthIsRejectedBeforeProjection()
    {
        object? term = null;
        for (var depth = 0; depth < 66; depth++) term = new object?[] { term };
        Assert.Throws<InvalidOperationException>(() => RabbitMqBindingTerms.Decode(RabbitBindingEtfFixture.Encode(term)));
    }

    [Theory]
    [InlineData("[250.0]", "array")]
    [InlineData("{\"amount\":250.0}", "table")]
    [InlineData("9223372036854775808", "wide")]
    public void UnsupportedEditorTypesRemainTypedInsteadOfSilentlyBecomingTextOrDouble(string jsonValue, string kind)
    {
        using var json = JsonDocument.Parse("{\"properties_key\":\"existing\",\"routing_key\":\"\",\"arguments\":{\"x-match\":\"all\",\"payload\":" + jsonValue + "}}");
        var rule = RabbitMqWorkspace.ToRule("headers", json.RootElement, 0, 1);
        switch (kind)
        {
            case "array": Assert.IsType<object?[]>(rule.Arguments["payload"]); break;
            case "table": Assert.IsType<Dictionary<string, object?>>(rule.Arguments["payload"]); break;
            default: Assert.Equal(BigInteger.Parse(jsonValue), Assert.IsType<BigInteger>(rule.Arguments["payload"])); break;
        }
        var editor = new RuleEditorViewModel("headers", "orders", rule, RoutingService.RabbitMq);
        Assert.Null(editor.TryBuild());
        Assert.False(string.IsNullOrWhiteSpace(editor.Error));
    }

    [Fact]
    public void OutgoingFloatingProjectionKeepsWholeDoublesAndSinglePrecisionValues()
    {
        var projected = JsonSerializer.SerializeToElement(new Dictionary<string, object?>
        { ["whole"] = 250d, ["fraction"] = 1.5d, ["single"] = 1.2f, ["integer"] = long.MaxValue }, RabbitMqBindingJson.Options);
        Assert.Equal("250.0", projected.GetProperty("whole").GetRawText());
        Assert.Equal(1.5d, projected.GetProperty("fraction").GetDouble());
        Assert.Equal((double)1.2f, projected.GetProperty("single").GetDouble());
        Assert.Equal(long.MaxValue, projected.GetProperty("integer").GetInt64());
        Assert.Throws<JsonException>(() => JsonSerializer.Serialize(double.NaN, RabbitMqBindingJson.Options));
    }
}
