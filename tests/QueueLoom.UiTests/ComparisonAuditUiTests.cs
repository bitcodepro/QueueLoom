using System.Text;
using Avalonia.Controls;
using Avalonia.Threading;
using Avalonia.VisualTree;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

public sealed class ComparisonAuditUiTests
{
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public Task ComparisonAudit_SourcePreviewWarningDoesNotClaimALineLimit(bool truncateLeft, bool truncateRight) => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var prefix = "{\"value\":\"same\"}";
        var complete = prefix + "  ";
        BrowsedMessage Preview(bool truncated) => new(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter,
            1, Encoding.UTF8.GetBytes(truncated ? prefix : complete), EditableMessageProperties.Empty,
            originalBodySize: Encoding.UTF8.GetByteCount(complete));
        var window = new CompareDialogWindow(new CompareDialogViewModel(new MessageItemViewModel(Preview(truncateLeft)),
            new MessageItemViewModel(Preview(truncateRight))));
        window.Show();
        try
        {
            await SettleAsync();
            var texts = VisibleTexts(window).ToArray();
            Assert.Contains(texts, text => text.Contains("incomplete", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(texts, text => text.Contains("retained", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(texts, text => text.Contains("are the same", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(texts, text => text.Contains(MessageComparison.MaximumLines.ToString("N0"), StringComparison.Ordinal));
            Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
        }
        finally { window.Close(); }
    });

    [Fact]
    public Task ComparisonAudit_WindowDisclosesAnIncompleteBodyComparison() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var prefix = string.Join('\n', Enumerable.Repeat("same", MessageComparison.MaximumLines));
        var left = Message(prefix + "\nfirst");
        var right = Message(prefix + "\nother");
        var window = new CompareDialogWindow(new CompareDialogViewModel(new MessageItemViewModel(left), new MessageItemViewModel(right)));
        window.Show();
        try
        {
            await SettleAsync();
            Assert.Contains(VisibleTexts(window), text => text.Contains("incomplete", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(VisibleTexts(window), text => text.Contains("are the same", StringComparison.OrdinalIgnoreCase));
            Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
        }
        finally { window.Close(); }
    });

    [Fact]
    public Task ComparisonAudit_PropertyTabPreservesBrokerIdsAndApplicationTypes() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var left = Message("same", new(MessageId: "first"),
            [new("Message ID", ApplicationPropertyType.String, "constant"), new("attempt", ApplicationPropertyType.String, "42")]);
        var right = Message("same", new(MessageId: "other"),
            [new("Message ID", ApplicationPropertyType.String, "constant"), new("attempt", ApplicationPropertyType.Int32, "42")]);
        var window = new CompareDialogWindow(new CompareDialogViewModel(new MessageItemViewModel(left), new MessageItemViewModel(right)));
        window.Show();
        try
        {
            window.GetVisualDescendants().OfType<TabControl>().Single().SelectedIndex = 1;
            await SettleAsync();
            var texts = VisibleTexts(window).ToArray();
            Assert.Contains("first", texts);
            Assert.Contains("other", texts);
            Assert.Contains("[String] 42", texts);
            Assert.Contains("[Int32] 42", texts);
            Assert.Contains("Application: Message ID", texts);
            Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
        }
        finally { window.Close(); }
    });

    private static IEnumerable<string> VisibleTexts(Window window) => window.GetVisualDescendants().OfType<TextBlock>()
        .Where(text => text.IsEffectivelyVisible && text.Text is not null).Select(text => text.Text!);

    private static async Task SettleAsync()
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            Dispatcher.UIThread.RunJobs();
            await Task.Delay(10);
        }
        Dispatcher.UIThread.RunJobs();
    }

    private static BrowsedMessage Message(string body, EditableMessageProperties? properties = null,
        MessageApplicationProperty[]? application = null) => new(ServiceBusEntityReference.Queue("orders"),
        ServiceBusSubQueue.DeadLetter, 1, Encoding.UTF8.GetBytes(body), properties ?? EditableMessageProperties.Empty, application);
}
