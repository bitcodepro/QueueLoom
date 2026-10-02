using QueueLoom.Core.ServiceBus;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class MalformedSchemaRegressionTests
{
    [Fact]
    public void ValidNegativeEnumValues_ContinueToLoad()
    {
        var schema = ProtoSchemaSet.FromProtoFiles(
            [("valid.proto", "enum State { UNKNOWN = 0; FAILED = -1; } message Valid { State state = 1; }")]);
        Assert.Equal("FAILED", schema.FindEnum("State")!.Values[-1]);
    }

    [Theory]
    [InlineData("0x")]
    [InlineData("0xGG")]
    [InlineData("0x100000000")]
    public void InvalidHexNumbers_AreControlledSchemaErrors(string number)
    {
        var error = Assert.Throws<ProtoSchemaException>(() => ProtoSchemaSet.FromProtoFiles(
            [("broken.proto", $"message Broken {{ string value = {number}; }}")]));
        Assert.Contains("broken.proto", error.Message, StringComparison.Ordinal);
        Assert.Contains("field number", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DescriptorWithoutMessageName_IsAControlledSchemaError()
    {
        var error = Assert.Throws<ProtoSchemaException>(() => ProtoSchemaSet.FromDescriptorSet([10, 2, 34, 0], "broken.desc"));
        Assert.Contains("broken.desc", error.Message, StringComparison.Ordinal);
        Assert.Contains("name", error.Message, StringComparison.OrdinalIgnoreCase);
    }
}

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("descriptor")]
    [InlineData("hex-format")]
    [InlineData("hex-overflow")]
    public async Task DesktopImport_MalformedSchemaReportsErrorAndRetainsValidSchema(string malformed)
    {
        using var directory = new TemporaryDirectory();
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var valid = Path.Combine(directory.Path, "valid.proto");
        await File.WriteAllTextAsync(valid, "message Valid { string value = 1; }");
        await vm.LoadProtobufSchemasAsync(valid);
        var retained = ProtoSchemaCatalog.Current;
        try
        {
            var broken = Path.Combine(directory.Path, malformed == "descriptor" ? "broken.desc" : "broken.proto");
            if (malformed == "descriptor") await File.WriteAllBytesAsync(broken, [10, 2, 34, 0]);
            else await File.WriteAllTextAsync(broken, $"message Broken {{ string value = {(malformed == "hex-format" ? "0xGG" : "0x100000000")}; }}");
            Assert.Null(await Record.ExceptionAsync(() => vm.LoadProtobufSchemasAsync(broken)));
            Assert.Same(retained, ProtoSchemaCatalog.Current);
            Assert.Equal(valid, vm.ProtobufSchemaPath);
            Assert.StartsWith("Could not load", vm.ProtobufSchemaStatus, StringComparison.Ordinal);
            Assert.Equal(vm.ProtobufSchemaStatus, vm.StatusText);
        }
        finally { vm.ClearProtobufSchemasCommand.Execute(null); }
    }
}
