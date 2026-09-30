using System.Text;
using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.VisualTree;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

public sealed class NewDialogsUiTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    [Fact]
    public Task CompareAndResendDialogs_RenderWithoutBindingErrors() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var first = Message(101, """{"orderId":1042,"total":59.90,"currency":"EUR","tenant":"staging-eu","items":[{"sku":"A-1","qty":2}]}""", "Timeout");
        var second = Message(102, """{"orderId":1042,"total":64.90,"currency":"EUR","tenant":"prod-eu","items":[{"sku":"A-1","qty":2},{"sku":"B-7","qty":1}]}""", "ValidationFailed");

        var compare = new CompareDialogWindow(new CompareDialogViewModel(new MessageItemViewModel(first), new MessageItemViewModel(second)));
        compare.Show();
        Assert.Contains(compare.GetVisualDescendants().OfType<TextBlock>(),
            text => text.IsEffectivelyVisible && text.Text?.Contains("differ", StringComparison.Ordinal) == true);
        await Save(compare, "compare-messages.png");
        compare.Close();

        var resend = new ResendDialogViewModel([first, second], [Orders, ServiceBusEntityReference.Topic("orders-retry")], "Staging", false)
        {
            FindText = "staging-eu",
            ReplaceText = "prod-eu",
            SendLater = true,
            SendAtText = "03:00"
        };
        var window = new ResendDialogWindow(resend);
        window.Show();
        Assert.Contains(window.GetVisualDescendants().OfType<Button>(), button => button.IsEffectivelyVisible && button.Content as string == "Schedule copies");
        Assert.StartsWith("Changes 1 of 2 messages.", resend.RewritePreview, StringComparison.Ordinal);
        await Save(window, "resend-dialog.png");
        window.Close();

        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
    });

    private static BrowsedMessage Message(long sequence, string body, string reason) =>
        new(Orders, ServiceBusSubQueue.DeadLetter, sequence, Encoding.UTF8.GetBytes(body),
            new EditableMessageProperties($"order-{sequence}", "corr-1042", "application/json", "order.created"),
            [new("tenant", Core.ServiceBus.ApplicationPropertyType.String, sequence == 101 ? "staging-eu" : "prod-eu")],
            ServiceBusMessageState.Active, deliveryCount: 10, enqueuedAt: new DateTimeOffset(2026, 9, 30, 9, (int)(sequence - 100), 0, TimeSpan.Zero),
            deadLetterReason: reason);

    private static async Task Save(Window window, string name)
    {
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        using var frame = window.CaptureRenderedFrame();
        var directory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
        if (!string.IsNullOrWhiteSpace(directory) && frame is not null)
        {
            Directory.CreateDirectory(directory);
            await using var file = File.Create(Path.Combine(directory, name));
            frame.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
        }
    }
}
