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
    [InlineData("region = 'EU'", ApplicationPropertyType.String, "EU")]
    [InlineData("amount = 250", ApplicationPropertyType.Int64, "250")]
    [InlineData("rate = 0.5", ApplicationPropertyType.Double, "0.5")]
    [InlineData("vip = true", ApplicationPropertyType.Boolean, "true")]
    [InlineData("note = it''s fine", ApplicationPropertyType.String, "it''s fine")]
    [InlineData("name = 'O''Brien'", ApplicationPropertyType.String, "O'Brien")]
    public void Routing_TestPropertiesAreTypedLikeSql(string line, ApplicationPropertyType type, string value)
    {
        var property = Assert.Single(TopicRoutingViewModel.ParseProperties(line));
        Assert.Equal((type, value), (property.Type, property.Value));
    }

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
