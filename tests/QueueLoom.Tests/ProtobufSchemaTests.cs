using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

/// <summary>
/// .proto files and descriptor sets. The bodies and the descriptor set were written by protoc and the Python
/// protobuf library from <see cref="Proto"/>, so names, enums, maps, packed lists and oneofs match what real
/// senders produce.
/// </summary>
public sealed class ProtobufSchemaTests
{
    private const string Proto = """
        syntax = "proto3";
        package shop.orders.v1;

        // An order as the shop publishes it.
        message OrderCreated {
          string id = 1;
          Status status = 2;
          repeated Line lines = 3;
          map<string, int32> stock = 4;
          repeated int32 scores = 5;
          oneof payment {
            string card = 6;
            Iban iban = 7;
          }
          double total = 8;
          sint64 delta = 9;
          bool paid = 10;
          bytes signature = 11;
          /* nested */
          message Line {
            string sku = 1;
            uint32 qty = 2 [deprecated = true];
          }
          enum Status {
            STATUS_UNKNOWN = 0;
            STATUS_FAILED = 3;
          }
        }

        message Iban { string value = 1; }

        message Ping { int64 at = 1; }
        """;

    private const string Order = "CghPUkQtMTA0MhADGgcKA0EtMRACGgcKA0ItNxABIgcKA0EtMRAFKgQBAqwCOgYKBERFODlBAAAAAABQb0BIBVABWgIBAg==";

    private const string Ping = "CKCp77cG";

    private const string DescriptorSet =
        "CpAFCgxvcmRlcnMucHJvdG8SDnNob3Aub3JkZXJzLnYxIrEECgxPcmRlckNyZWF0ZWQSDgoCaWQYASABKAlSAmlkEjsKBnN0YXR1cxgCIAEoDjIjLnNob3Aub3JkZXJzLnYxLk9yZGVyQ3JlYXRlZC5TdGF0dXNSBnN0YXR1cxI3CgVsaW5lcxgDIAMoCzIhLnNob3Aub3JkZXJzLnYxLk9yZGVyQ3JlYXRlZC5MaW5lUgVsaW5lcxI9CgVzdG9jaxgEIAMoCzInLnNob3Aub3JkZXJzLnYxLk9yZGVyQ3JlYXRlZC5TdG9ja0VudHJ5UgVzdG9jaxIWCgZzY29yZXMYBSADKAVSBnNjb3JlcxIUCgRjYXJkGAYgASgJSABSBGNhcmQSKgoEaWJhbhgHIAEoCzIULnNob3Aub3JkZXJzLnYxLkliYW5IAFIEaWJhbhIUCgV0b3RhbBgIIAEoAVIFdG90YWwSFAoFZGVsdGEYCSABKBJSBWRlbHRhEhIKBHBhaWQYCiABKAhSBHBhaWQSHAoJc2lnbmF0dXJlGAsgASgMUglzaWduYXR1cmUaOAoKU3RvY2tFbnRyeRIQCgNrZXkYASABKAlSA2tleRIUCgV2YWx1ZRgCIAEoBVIFdmFsdWU6AjgBGi4KBExpbmUSEAoDc2t1GAEgASgJUgNza3USFAoDcXR5GAIgASgNQgIYAVIDcXR5Ii8KBlN0YXR1cxISCg5TVEFUVVNfVU5LTk9XThAAEhEKDVNUQVRVU19GQUlMRUQQA0IJCgdwYXltZW50IhwKBEliYW4SFAoFdmFsdWUYASABKAlSBXZhbHVlIhYKBFBpbmcSDgoCYXQYASABKANSAmF0YgZwcm90bzM=";

    private static void AssertOrder(string json)
    {
        using var document = JsonDocument.Parse(json);
        var order = document.RootElement;
        Assert.Equal("ORD-1042", order.GetProperty("id").GetString());
        Assert.Equal("STATUS_FAILED", order.GetProperty("status").GetString());
        Assert.Equal(["A-1", "B-7"], order.GetProperty("lines").EnumerateArray().Select(line => line.GetProperty("sku").GetString()));
        Assert.Equal(2u, order.GetProperty("lines")[0].GetProperty("qty").GetUInt32());
        Assert.Equal(5, order.GetProperty("stock").GetProperty("A-1").GetInt32());
        Assert.Equal([1, 2, 300], order.GetProperty("scores").EnumerateArray().Select(score => score.GetInt32()));
        Assert.Equal("DE89", order.GetProperty("iban").GetProperty("value").GetString());
        Assert.Equal(250.5, order.GetProperty("total").GetDouble());
        Assert.Equal(-3, order.GetProperty("delta").GetInt64());
        Assert.True(order.GetProperty("paid").GetBoolean());
        Assert.Equal("AQI=", order.GetProperty("signature").GetString());
    }

    [Fact]
    public void A_proto_file_names_every_field()
    {
        var schemas = ProtoSchemaSet.FromProtoFiles([("orders.proto", Proto)]);
        Assert.Equal(["shop.orders.v1.Iban", "shop.orders.v1.OrderCreated", "shop.orders.v1.OrderCreated.Line", "shop.orders.v1.Ping"],
            schemas.Messages.Select(message => message.FullName).Order(StringComparer.Ordinal));
        var decoded = ProtoDecoder.Decode(Convert.FromBase64String(Order), schemas.Resolve("OrderCreated")!, schemas)!.Value;
        Assert.Equal(1, decoded.Fit);
        AssertOrder(decoded.Json);
    }

    [Fact]
    public void A_descriptor_set_from_protoc_reads_the_same()
    {
        var schemas = ProtoSchemaSet.FromDescriptorSet(Convert.FromBase64String(DescriptorSet));
        Assert.NotNull(schemas.FindEnum("shop.orders.v1.OrderCreated.Status"));
        Assert.True(schemas.FindMessage("shop.orders.v1.OrderCreated.StockEntry")!.IsMapEntry);
        AssertOrder(ProtoDecoder.Decode(Convert.FromBase64String(Order), schemas.Resolve("shop.orders.v1.OrderCreated")!, schemas)!.Value.Json);
    }

    [Fact]
    public void Without_a_hint_the_type_that_fits_is_picked()
    {
        var schemas = ProtoSchemaSet.FromProtoFiles([("orders.proto", Proto)]);
        var order = ProtoDecoder.DecodeBestFit(Convert.FromBase64String(Order), schemas)!.Value;
        Assert.Equal("shop.orders.v1.OrderCreated", order.Type.FullName);
        var ping = ProtoDecoder.DecodeBestFit(Convert.FromBase64String(Ping), schemas, hint: "shop.orders.v1.Ping")!.Value;
        Assert.Equal(1727780000, JsonDocument.Parse(ping.Json).RootElement.GetProperty("at").GetInt64());
    }

    [Fact]
    public void The_body_decoder_uses_the_loaded_types_and_the_content_type()
    {
        var schemas = ProtoSchemaSet.FromProtoFiles([("orders.proto", Proto)]);
        var named = BodyDecoder.Decode(Convert.FromBase64String(Ping), "application/x-protobuf; messageType=shop.orders.v1.Ping", protos: schemas)!;
        Assert.Equal(["Protobuf (shop.orders.v1.Ping)"], named.Steps);
        Assert.Contains("as the message says", named.Note, StringComparison.Ordinal);

        var guessed = BodyDecoder.Decode(Convert.FromBase64String(Order), protos: schemas)!;
        Assert.Equal(["Protobuf (shop.orders.v1.OrderCreated)"], guessed.Steps);
        AssertOrder(guessed.Text);

        var unnamed = BodyDecoder.Decode(Convert.FromBase64String(Order), protos: ProtoSchemaSet.Empty)!;
        Assert.Equal(["Protobuf (no schema)"], unnamed.Steps);
    }

    [Fact]
    public void A_Schema_Registry_body_is_read_with_the_registry_proto_and_its_message_index()
    {
        // Magic byte, schema id 7, message indexes [2] (zigzag 2 → 4 after a count of 1 → 2): the third top-level message, Ping.
        var framed = new byte[] { 0, 0, 0, 0, 7, 2, 4 }.Concat(Convert.FromBase64String(Ping)).ToArray();
        var decoded = BodyDecoder.Decode(framed, schema: new MessageSchema(7, MessageSchemaType.Protobuf, Proto), protos: ProtoSchemaSet.Empty)!;
        Assert.Equal(["Schema Registry", "Protobuf"], decoded.Steps);
        Assert.Contains("message shop.orders.v1.Ping", decoded.Note, StringComparison.Ordinal);
        Assert.Equal(1727780000, JsonDocument.Parse(decoded.Text).RootElement.GetProperty("at").GetInt64());

        // No indexes (a single 0) means the first message.
        var first = new byte[] { 0, 0, 0, 0, 7, 0 }.Concat(Convert.FromBase64String(Order)).ToArray();
        AssertOrder(BodyDecoder.Decode(first, schema: new MessageSchema(7, MessageSchemaType.Protobuf, Proto), protos: ProtoSchemaSet.Empty)!.Text);
    }

    [Theory]
    [InlineData("application/x-protobuf; messageType=shop.Order", null, "shop.Order")]
    [InlineData("application/protobuf; proto=\"a.B\"", null, "a.B")]
    [InlineData("application/octet-stream", "shop.Ping", "shop.Ping")]
    [InlineData(null, null, null)]
    public void The_message_type_comes_from_the_content_type_or_a_property(string? contentType, string? property, string? expected) =>
        Assert.Equal(expected, ProtoSchemaCatalog.HintFrom(contentType,
            property is null ? [] : [new MessageApplicationProperty("MessageType", ApplicationPropertyType.String, property)]));

    [Fact]
    public void A_folder_of_proto_files_is_loaded_with_its_imports()
    {
        var folder = Directory.CreateTempSubdirectory("queueloom-proto");
        try
        {
            Directory.CreateDirectory(Path.Combine(folder.FullName, "common"));
            File.WriteAllText(Path.Combine(folder.FullName, "common", "money.proto"), """
                syntax = "proto3";
                package shop.common;
                message Money { string currency = 1; int64 units = 2; }
                """);
            File.WriteAllText(Path.Combine(folder.FullName, "invoice.proto"), """
                syntax = "proto3";
                package shop.billing;
                import "common/money.proto";
                message Invoice { string number = 1; shop.common.Money amount = 2; }
                """);
            var schemas = ProtoSchemaSet.Load(folder.FullName);
            Assert.Equal(2, schemas.Sources.Count);
            Assert.Equal("shop.common.Money", schemas.Resolve("Invoice")!.FieldsByNumber[2].TypeName);
        }
        finally
        {
            folder.Delete(recursive: true);
        }
    }

    [Theory]
    [InlineData("message A { string x = ; }", "field number")]
    [InlineData("message A { string x = 1 }", "Expected ';'")]
    [InlineData("message A { string x = 1;", "ends too early")]
    public void Broken_proto_files_say_what_is_wrong(string proto, string reason)
    {
        var exception = Assert.Throws<ProtoSchemaException>(() => ProtoSchemaSet.FromProtoFiles([("broken.proto", proto)]));
        Assert.Contains(reason, exception.Message, StringComparison.Ordinal);
        Assert.StartsWith("broken.proto: ", exception.Message, StringComparison.Ordinal);
    }
}
