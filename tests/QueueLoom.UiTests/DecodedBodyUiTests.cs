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
