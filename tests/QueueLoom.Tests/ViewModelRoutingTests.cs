using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    private static FakeWorkspace RoutingWorkspace() => new()
    {
        SupportsSubscriptionRules = true,
        Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [],
        [
            new ServiceBusTopic("orders", ServiceBusEntityRuntime.Empty,
            [
                new ServiceBusSubscription("orders", "billing", ServiceBusEntityRuntime.Empty),
                new ServiceBusSubscription("orders", "shipping", ServiceBusEntityRuntime.Empty)
            ])
        ]),
        TopicRules =
        {
            ["orders"] =
            [
                new SubscriptionRules("billing", [new SubscriptionRule("eu", RuleFilterKind.Sql, "region = 'EU'")]),
                new SubscriptionRules("shipping", [new SubscriptionRule("shipped", RuleFilterKind.Correlation,
                    Correlation: new CorrelationFilterFields(Subject: "order.shipped"))])
            ]
        }
    };

    [Fact]
    public async Task Routing_ShowsWhereADraftGoesAndWhyNot()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var workspace = RoutingWorkspace();
        var dialogs = new FakeDialogService();
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        Assert.True(viewModel.SupportsRouting);

        viewModel.SelectedDestination = viewModel.Destinations.Single(destination => destination.Reference.Kind == ServiceBusEntityKind.Topic);
        viewModel.DraftSubject = "order.created";
        viewModel.DraftApplicationProperties = """{ "region": { "type": "String", "value": "eu" } }""";
        await viewModel.CheckDraftRoutingCommand.ExecuteAsync();

        var routing = Assert.Single(dialogs.RoutingDialogs);
        Assert.True(routing.IsDropped);
        Assert.Contains("region = 'eu'", routing.TestProperties, StringComparison.Ordinal);
        var billing = routing.Subscriptions.Single(item => item.Name == "billing");
        Assert.True(billing.Skips);
        Assert.Contains("region is 'eu'", billing.ResultText, StringComparison.Ordinal);
        Assert.False(routing.CanEdit);
        Assert.Contains("Allow creating, changing and deleting queues", routing.EditHint, StringComparison.Ordinal);

        routing.TestProperties = "region = 'EU'";
        routing.Check();
        Assert.True(billing.Receives);
        Assert.Equal("1 of 2 subscriptions receive it.", routing.Headline);
    }

    [Fact]
    public async Task Routing_RulesAreAddedAndDeletedWhenManagementIsAllowed()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { AllowQueueManagement = true };
        var workspace = RoutingWorkspace();
        var dialogs = new FakeDialogService
        {
            ConfirmResult = true,
            RuleEdit = editor =>
            {
                editor.Name = "created";
                editor.Kind = RuleEditorViewModel.Kinds.Single(kind => kind.Kind == RuleFilterKind.Correlation);
                editor.Subject = "order.created";
                return editor.TryBuild();
            },
            OnRouting = async routing =>
            {
                Assert.True(routing.CanEdit);
                routing.Selected = routing.Subscriptions.Single(item => item.Name == "shipping");
                await routing.AddRuleCommand.ExecuteAsync();
                var shipping = routing.Subscriptions.Single(item => item.Name == "shipping");
                Assert.Equal(["shipped", "created"], shipping.Rules.Select(rule => rule.Name));

                var billing = routing.Subscriptions.Single(item => item.Name == "billing");
                await routing.DeleteRuleCommand.ExecuteAsync(billing.Rules.Single());
                Assert.Equal("Has no rules, so it receives no messages at all.", routing.Subscriptions.Single(item => item.Name == "billing").Warning);
                Assert.Contains("1 receive nothing", routing.SubscriptionsCaption, StringComparison.Ordinal);
            }
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.IsTopic);

        await viewModel.OpenTopicRoutingCommand.ExecuteAsync();

        Assert.Equal(["add orders/shipping/created", "delete orders/billing/eu"], workspace.RuleChanges);
        var confirmation = dialogs.Confirmations.Last();
        Assert.Equal("billing", confirmation.RequiredText);
        Assert.Contains("receives no messages at all", confirmation.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("region = 'EU'", "EU")]
    [InlineData("amount = 250", 250L)]
    [InlineData("rate = 0.5", 0.5)]
    [InlineData("vip = true", true)]
    [InlineData("plain = acme", "acme")]
    [InlineData("name = 'O''Brien'", "O'Brien")]
    [InlineData("note = 'line1\\nline2'", "line1\nline2")]
    public void Routing_TestPropertiesAreTypedLikeSql(string line, object value) =>
        Assert.Equal(value, Assert.Single(TopicRoutingViewModel.ParseProperties(line)).Value);

    [Fact]
    public void RuleEditor_ChecksTheFilterAsItIsTyped()
    {
        var editor = new RuleEditorViewModel("orders", "billing") { Name = "eu", SqlExpression = "region = = 'EU'" };
        Assert.True(editor.HasSqlProblem);
        Assert.StartsWith("At character", editor.SqlCheck, StringComparison.Ordinal);
        Assert.Null(editor.TryBuild());

        editor.SqlExpression = "region = 'EU'";
        Assert.False(editor.HasSqlProblem);
        Assert.Equal(new SubscriptionRule("eu", RuleFilterKind.Sql, "region = 'EU'"), editor.TryBuild());

        editor.Kind = RuleEditorViewModel.Kinds[1];
        Assert.Null(editor.TryBuild());
        Assert.Contains("at least one field", editor.Error, StringComparison.Ordinal);
        editor.CorrelationProperties = "tenant = acme";
        Assert.Equal("acme", editor.TryBuild()!.Correlation!.Properties["tenant"]);
    }
}

public sealed class RoutingReviewRegressionTests
{
    private static readonly SubscriptionRule TypedRule = new("typed", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields(
        Subject: "order.created", ReplyToSessionId: "reply-7")
    {
        Properties = new Dictionary<string, object>
        {
            ["amount"] = 250,
            ["total"] = 250L,
            ["rate"] = 0.5,
            ["vip"] = true,
            ["tenant"] = "acme",
            ["due"] = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc)
        }
    });

    [Fact]
    public void CorrelationValues_KeepTheirTypeWhenOnlyTheActionChanges()
    {
        var editor = new RuleEditorViewModel("orders", "billing", TypedRule) { Action = "SET seen = true" };

        var saved = editor.TryBuild()!;

        Assert.Equal("SET seen = true", saved.Action);
        Assert.Equal("reply-7", saved.Correlation!.ReplyToSessionId);
        Assert.Equal("order.created", saved.Correlation.Subject);
        foreach (var (name, value) in TypedRule.Correlation!.Properties)
        {
            Assert.Equal(value.GetType(), saved.Correlation.Properties[name].GetType());
            Assert.Equal(value, saved.Correlation.Properties[name]);
        }
    }

    [Fact]
    public void CorrelationEditor_RejectsTypesServiceBusRulesDoNotAccept()
    {
        var editor = new RuleEditorViewModel("orders", "billing")
        {
            Name = "trace",
            Kind = RuleEditorViewModel.Kinds[1],
            CorrelationProperties = "trace = <Guid> 4f3c2a1b-0000-4000-8000-000000000001"
        };

        Assert.Null(editor.TryBuild());
        Assert.Contains("not a Guid", editor.Error, StringComparison.Ordinal);

        editor.CorrelationProperties = "count = <Int32> 7\ndue = <DateTime> 2026-10-01T12:00:00.0000000Z";
        var properties = editor.TryBuild()!.Correlation!.Properties;
        Assert.Equal(7, properties["count"]);
        Assert.IsType<DateTime>(properties["due"]);
    }

    [Fact]
    public void CorrelationEditor_ReadsNumbersBooleansAndQuotedText()
    {
        var editor = new RuleEditorViewModel("orders", "billing")
        {
            Name = "typed",
            Kind = RuleEditorViewModel.Kinds[1],
            CorrelationProperties = "amount = 250\ncode = '250'\nvip = true\nrate = 1.5\nplain = acme"
        };

        var properties = editor.TryBuild()!.Correlation!.Properties;

        Assert.Equal(250L, properties["amount"]);
        Assert.Equal("250", properties["code"]);
        Assert.Equal(true, properties["vip"]);
        Assert.Equal(1.5, properties["rate"]);
        Assert.Equal("acme", properties["plain"]);
        Assert.Contains("amount = 250 AND code = '250'", new CorrelationFilterFields { Properties = properties }.Describe(), StringComparison.Ordinal);
    }

    [Fact]
    public void CorrelationMatching_RequiresTheSameTypeAsServiceBusDoes()
    {
        // Confirmed on the Service Bus emulator: a correlation filter on the Int64 250 takes neither the Int32 250 nor the text '250'.
        var rule = new SubscriptionRule("amount", RuleFilterKind.Correlation,
            Correlation: new CorrelationFilterFields { Properties = new Dictionary<string, object> { ["amount"] = 250L } });

        RoutingOutcome Route(ApplicationPropertyType type, string value) => TopicRouting.Check(rule,
            new RoutingMessage(EditableMessageProperties.Empty, [new MessageApplicationProperty("amount", type, value)])).Outcome;

        Assert.Equal(RoutingOutcome.Receives, Route(ApplicationPropertyType.Int64, "250"));
        Assert.Equal(RoutingOutcome.Skips, Route(ApplicationPropertyType.Int32, "250"));
        Assert.Equal(RoutingOutcome.Skips, Route(ApplicationPropertyType.String, "250"));
        Assert.Contains("amount should be 250 but is <Int32> 250", TopicRouting.Check(rule, new RoutingMessage(EditableMessageProperties.Empty,
            [new MessageApplicationProperty("amount", ApplicationPropertyType.Int32, "250")])).Explanation, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FailedRuleLoading_IsShownAsAnErrorNotAsADroppedMessage()
    {
        var services = new TopicRoutingServices(
            _ => throw new InvalidOperationException("Reading subscription rules needs the Azure Service Bus Data Owner role."),
            (_, _, _, _) => Task.CompletedTask, (_, _, _) => Task.CompletedTask,
            _ => Task.FromResult<SubscriptionRule?>(null), (_, _, _) => Task.FromResult(false));
        var draft = new MessageDraft(EditableMessageBody.Empty, new EditableMessageProperties(Subject: "order.created"));
        var routing = new TopicRoutingViewModel("orders", "Prod", false, string.Empty, services, draft, "Dead letter order-1");

        await routing.LoadAsync();
        routing.Check();

        Assert.Contains("Data Owner", routing.Error, StringComparison.Ordinal);
        Assert.False(routing.HasHeadline);
        Assert.False(routing.IsDropped);
        Assert.Empty(routing.Subscriptions);
        Assert.False(routing.CheckCommand.CanExecute(null));
    }

    [Fact]
    public async Task RoutingWindow_ChecksTheSamePropertiesAsDirectRouting()
    {
        SubscriptionRules[] rules =
        [
            new("by-reply-session", [new SubscriptionRule("sql", RuleFilterKind.Sql, "sys.ReplyToSessionId = 'reply-7'")]),
            new("by-partition", [new SubscriptionRule("sql", RuleFilterKind.Sql, "sys.PartitionKey = 'p-3'")]),
            new("by-correlation", [new SubscriptionRule("corr", RuleFilterKind.Correlation,
                Correlation: new CorrelationFilterFields(ReplyToSessionId: "reply-7"))]),
            new("padded", [new SubscriptionRule("corr", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields(Subject: " order "))])
        ];
        var draft = new MessageDraft(EditableMessageBody.Empty,
            new EditableMessageProperties(MessageId: "m-1", Subject: " order ", ReplyToSessionId: "reply-7", PartitionKey: "p-3"));
        var services = new TopicRoutingServices(_ => Task.FromResult<IReadOnlyList<SubscriptionRules>>(rules),
            (_, _, _, _) => Task.CompletedTask, (_, _, _) => Task.CompletedTask,
            _ => Task.FromResult<SubscriptionRule?>(null), (_, _, _) => Task.FromResult(false));
        var routing = new TopicRoutingViewModel("orders", "Dev", false, string.Empty, services, draft, "Draft");

        await routing.LoadAsync();

        var direct = TopicRouting.Route("orders", rules, RoutingMessage.From(draft));
        Assert.Equal(direct.Subscriptions.Select(item => (item.Subscription, item.Outcome)),
            routing.Subscriptions.Select(item => (item.Name, item.Result!.Outcome)));
        Assert.All(routing.Subscriptions, item => Assert.True(item.Receives, item.Name));
    }
}
