using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

public sealed class MalformedSchemaUiRegressionTests
{
    [Theory]
    [InlineData("descriptor")]
    [InlineData("hex-format")]
    [InlineData("hex-overflow")]
    public Task ImportError_KeepsTheLoadedSchemaAndDecodedBody(string malformed) => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var folder = Directory.CreateTempSubdirectory("queueloom-malformed-ui");
        await using var fixture = await WindowFixture.OpenAsync();
        try
        {
            var valid = Path.Combine(folder.FullName, "valid.proto");
            await File.WriteAllTextAsync(valid, "message Good { string value = 1; }");
            await fixture.OpenDeadLettersAsync();
            var message = new MessageItemViewModel(new BrowsedMessage(ServiceBusEntityReference.Queue("orders"),
                ServiceBusSubQueue.DeadLetter, 999, new byte[] { 10, 2, 111, 107 },
                new EditableMessageProperties(ContentType: "application/x-protobuf; messageType=Good")));
            fixture.ViewModel.Messages.Add(message);
            fixture.ViewModel.SelectedMessage = message;
            await fixture.ViewModel.LoadProtobufSchemasAsync(valid);
            await fixture.SettleAsync();
            Assert.Contains("\"value\": \"ok\"", message.DecodedText, StringComparison.Ordinal);
            var retained = ProtoSchemaCatalog.Current;
            var broken = Path.Combine(folder.FullName, malformed == "descriptor" ? "broken.desc" : "broken.proto");
            if (malformed == "descriptor") await File.WriteAllBytesAsync(broken, [10, 2, 34, 0]);
            else await File.WriteAllTextAsync(broken, $"message Broken {{ string value = {(malformed == "hex-format" ? "0xGG" : "0x100000000")}; }}");
            Assert.Null(await Record.ExceptionAsync(() => fixture.ViewModel.LoadProtobufSchemasAsync(broken)));
            await fixture.SettleAsync();
            Assert.StartsWith("Could not load", fixture.ViewModel.ProtobufSchemaStatus, StringComparison.Ordinal);
            Assert.Equal(valid, fixture.ViewModel.ProtobufSchemaPath);
            Assert.Same(retained, ProtoSchemaCatalog.Current);
            Assert.Contains("\"value\": \"ok\"", message.DecodedText, StringComparison.Ordinal);
            Assert.Empty(BindingErrors.Instance.Messages);
        }
        finally { fixture.ViewModel.ClearProtobufSchemasCommand.Execute(null); folder.Delete(recursive: true); }
    });
}
