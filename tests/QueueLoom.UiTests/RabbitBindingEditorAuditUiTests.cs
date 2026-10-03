using Avalonia.Automation;
using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Threading;
using Avalonia.VisualTree;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.Routing;

namespace QueueLoom.UiTests;

public sealed class RabbitBindingEditorAuditUiTests
{
    [Theory]
    [InlineData("region = 'EU'\nregion = 'US'", "duplicated")]
    [InlineData("x-match = 'any'\nregion = 'EU'", "x-match")]
    public Task BindingAudit_InvalidHeadersKeepDialogOpenWithVisibleError(string text, string expectedError) => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var editor = new RuleEditorViewModel("by-header", "orders", service: RoutingService.RabbitMq,
            bindingKind: RuleFilterKind.HeadersBinding);
        var owner = new Window();
        var window = new RuleEditorWindow(editor);
        owner.Show();
        var result = window.ShowDialog<SubscriptionRule?>(owner);
        try
        {
            await SettleAsync();
            var input = window.GetVisualDescendants().OfType<TextBox>()
                .Single(control => AutomationProperties.GetName(control) == "Binding headers");
            input.Text = text;
            await SettleAsync();
            Assert.Equal(text, editor.Headers);
            Confirm(window, editor);
            await SettleAsync();

            Assert.True(window.IsVisible);
            Assert.False(result.IsCompleted);
            Assert.Contains(window.GetVisualDescendants().OfType<TextBlock>().Where(control => control.IsEffectivelyVisible),
                control => control.Text?.Contains(expectedError, StringComparison.OrdinalIgnoreCase) == true);
            Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
        }
        finally { window.Close(); owner.Close(); }
    });

    [Fact]
    public Task BindingAudit_SavingExistingPresenceHeaderPreservesNull() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        var existing = new SubscriptionRule("presence-binding", RuleFilterKind.HeadersBinding)
        {
            Arguments = new Dictionary<string, object?> { ["x-match"] = "all", ["trace"] = null }
        };
        var editor = new RuleEditorViewModel("by-header", "orders", existing, RoutingService.RabbitMq);
        var owner = new Window();
        var window = new RuleEditorWindow(editor);
        owner.Show();
        var result = window.ShowDialog<SubscriptionRule?>(owner);
        try
        {
            await SettleAsync();
            Confirm(window, editor);
            var saved = await result.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.NotNull(saved);
            Assert.Null(saved.Arguments["trace"]);
            Assert.False(window.IsVisible);
            Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
        }
        finally { window.Close(); owner.Close(); }
    });

    private static void Confirm(Window window, RuleEditorViewModel editor)
    {
        var button = window.GetVisualDescendants().OfType<Button>().Single(control => Equals(control.Content, editor.ConfirmLabel));
        button.RaiseEvent(new RoutedEventArgs(Button.ClickEvent));
    }

    private static async Task SettleAsync()
    {
        for (var attempt = 0; attempt < 3; attempt++) { Dispatcher.UIThread.RunJobs(); await Task.Delay(10); }
        Dispatcher.UIThread.RunJobs();
    }
}
