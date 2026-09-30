using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.VisualTree;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

public sealed class RoutingUiTests
{
    [Fact]
    public Task RoutingAndRuleEditor_RenderWithoutBindingErrors() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        SubscriptionRules[] rules =
        [
            new("billing", [new SubscriptionRule("eu-large", RuleFilterKind.Sql, "region = 'EU' AND amount > 100", Action: "SET priority = 'high'")]),
            new("shipping", [new SubscriptionRule("created", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields(Subject: "order.created"))]),
            new("audit", [new SubscriptionRule(SubscriptionRule.DefaultRuleName, RuleFilterKind.True)]),
            new("legacy-crm", [])
        ];
        var services = new TopicRoutingServices(
            _ => Task.FromResult<IReadOnlyList<SubscriptionRules>>(rules),
            (_, _, _, _) => Task.CompletedTask,
            (_, _, _) => Task.CompletedTask,
            _ => Task.FromResult<SubscriptionRule?>(null),
            (_, _, _) => Task.FromResult(false));
        var draft = new MessageDraft(new EditableMessageBody("{}", MessageBodyFormat.Json),
            new EditableMessageProperties(MessageId: "order-1042", Subject: "order.created"),
            [new MessageApplicationProperty("region", ApplicationPropertyType.String, "eu"), new MessageApplicationProperty("amount", ApplicationPropertyType.Int32, "250")]);
        var viewModel = new TopicRoutingViewModel("orders", "Payments · Prod", true, "Rule changes apply at once to new messages; messages already in a subscription stay.",
            services, draft, "Dead letter order-1042 from orders / billing");

        var window = new TopicRoutingWindow(viewModel);
        window.Show();
        await viewModel.LoadAsync();
        Avalonia.Threading.Dispatcher.UIThread.RunJobs();
        Assert.Equal("2 of 4 subscriptions receive it.", viewModel.Headline);
        Assert.Contains("region is 'eu'", viewModel.Subscriptions.Single(item => item.Name == "billing").ResultText, StringComparison.Ordinal);
        Assert.Contains(window.GetVisualDescendants().OfType<Button>(), button => button.IsEffectivelyVisible && button.Content as string == "Add rule…");
        await Save(window, "rules-and-routing.png");
        window.Close();

        var editor = new RuleEditorWindow(new RuleEditorViewModel("orders", "billing", rules[0].Rules[0]) { SqlExpression = "region IN ('EU', 'eu') AND amount > 100" });
        editor.Show();
        await Save(editor, "rule-editor.png");
        editor.Close();

        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
    });

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
