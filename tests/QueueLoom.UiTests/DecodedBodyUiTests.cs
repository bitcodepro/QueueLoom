using System.IO.Compression;
using System.Text;
using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.VisualTree;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

public sealed class DecodedBodyUiTests
{
    [Fact]
    public Task PackedBodies_OpenOnTheDecodedTab() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.OpenDeadLettersAsync();
        var plain = fixture.ViewModel.SelectedMessage!;
        var packed = new MessageItemViewModel(new BrowsedMessage(
            ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 900,
            Encoding.UTF8.GetBytes(Convert.ToBase64String(Gzip("""{"orderId":1042,"lines":[{"sku":"A-1","qty":2}],"paid":false}"""))),
            new EditableMessageProperties(MessageId: "order-1042-packed", ContentType: "application/json+gzip"),
            deadLetterReason: "MaxDeliveryCountExceeded"));
        fixture.ViewModel.Messages.Insert(0, packed);

        fixture.ViewModel.SelectedMessage = packed;
        await fixture.SettleAsync();
        var details = fixture.Window.GetVisualDescendants().OfType<MessageDetailsView>().First(view => view.IsEffectivelyVisible);
        var tabs = details.GetVisualDescendants().OfType<TabControl>().First();
        Assert.Equal("Decoded", ((TabItem)tabs.SelectedItem!).Header);
        Assert.Equal(["base64", "gzip", "JSON"], packed.DecodedSteps);
        await fixture.SettleAsync();
        fixture.Window.UpdateLayout();
        var editor = details.GetVisualDescendants().OfType<QueueLoom.App.Controls.CodeEditor>()
            .Single(control => control.IsEffectivelyVisible);
        Assert.Contains("\"orderId\": 1042", editor.Text, StringComparison.Ordinal);

        using (var frame = fixture.Window.CaptureRenderedFrame())
        {
            var directory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
                await using var file = File.Create(Path.Combine(directory, "dark-decoded-body.png"));
                frame!.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
            }
        }

        fixture.ViewModel.SelectedMessage = plain;
        await fixture.SettleAsync();
        Assert.Equal("Body", ((TabItem)tabs.SelectedItem!).Header);
        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
    });

    [Fact]
    public Task Protobuf_GetsItsFieldNamesOnceProtoFilesAreLoaded() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var folder = Directory.CreateTempSubdirectory("queueloom-proto-ui");
        await using var fixture = await WindowFixture.OpenAsync();
        try
        {
            File.WriteAllText(Path.Combine(folder.FullName, "ping.proto"), """
                syntax = "proto3";
                package shop.v1;
                message Ping { int64 at = 1; string from = 2; }
                """);
            await fixture.OpenDeadLettersAsync();
            // Ping { at: 1727780000, from: "billing" }, as protoc writes it.
            var body = Convert.FromBase64String("CKCp77cG").Concat(new byte[] { 0x12, 7 }).Concat(Encoding.UTF8.GetBytes("billing")).ToArray();
            var message = new MessageItemViewModel(new BrowsedMessage(
                ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 901, body,
                new EditableMessageProperties(MessageId: "ping-1", ContentType: "application/x-protobuf"),
                deadLetterReason: "MaxDeliveryCountExceeded"));
            fixture.ViewModel.Messages.Insert(0, message);
            fixture.ViewModel.SelectedMessage = message;
            await fixture.SettleAsync();

            Assert.True(message.ShowsProtobufFieldNumbers);
            var details = fixture.Window.GetVisualDescendants().OfType<MessageDetailsView>().First(view => view.IsEffectivelyVisible);
            Assert.Contains(details.GetVisualDescendants().OfType<Button>(),
                button => button.IsEffectivelyVisible && button.Content as string == "Load .proto files…");

            await fixture.ViewModel.LoadProtobufSchemasAsync(folder.FullName);
            await fixture.SettleAsync();
            Assert.Equal(["Protobuf (shop.v1.Ping)"], message.DecodedSteps);
            Assert.Contains("\"from\": \"billing\"", message.DecodedText, StringComparison.Ordinal);
            Assert.False(message.ShowsProtobufFieldNumbers);
            Assert.StartsWith("1 message type(s) from 1 file(s)", fixture.ViewModel.ProtobufSchemaStatus, StringComparison.Ordinal);
            Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
        }
        finally
        {
            fixture.ViewModel.ClearProtobufSchemasCommand.Execute(null);
            folder.Delete(recursive: true);
        }
    });

    private static byte[] Gzip(string text)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest))
        {
            gzip.Write(Encoding.UTF8.GetBytes(text));
        }
        return output.ToArray();
    }
}
