using System.Reflection;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

// Service Bus rules have no ETag or version (RuleProperties: Name, Filter, Action only) and UpdateRuleAsync replaces
// whatever is there, so a second operator's change made while the first had the dialog open used to be overwritten.
public sealed class AzureRuleConcurrencyTests
{
    private static readonly SubscriptionRule AsRead = AzureServiceBusWorkspace.ToRule(
        ServiceBusModelFactory.RuleProperties("eu", new SqlRuleFilter("region = 'EU'"), null));

    [Fact]
    public async Task Replace_RefusesWhenAnotherOperatorChangedTheRuleSinceItWasRead()
    {
        var administration = new RulesAdministration();
        administration.Rules["eu"] = ServiceBusModelFactory.RuleProperties("eu", new SqlRuleFilter("region = 'US'"), null);
        await using var workspace = Connected(administration);
        var edited = new SubscriptionRule("eu", RuleFilterKind.Sql, "region = 'FR'") { Original = AsRead };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workspace.SaveSubscriptionRuleAsync("orders", "billing", edited, replace: true));

        Assert.Contains("changed since you opened it", error.Message, StringComparison.Ordinal);
        Assert.Contains("review again", error.Message, StringComparison.Ordinal);
        Assert.Empty(administration.Writes);
        Assert.Equal("region = 'US'", ((SqlRuleFilter)administration.Rules["eu"].Filter).SqlExpression);
    }

    [Theory]
    [InlineData("action")]
    [InlineData("type")]
    [InlineData("gone")]
    public async Task Replace_TreatsAnyDifferenceAsAChange(string change)
    {
        var read = new CorrelationRuleFilter { Subject = "order.created" };
        read.ApplicationProperties["amount"] = 250L;
        var live = new CorrelationRuleFilter { Subject = "order.created" };
        live.ApplicationProperties["amount"] = change == "type" ? "250" : 250L;
        var administration = new RulesAdministration();
        if (change != "gone")
            administration.Rules["typed"] = ServiceBusModelFactory.RuleProperties("typed", live,
                change == "action" ? new SqlRuleAction("SET seen = true") : null);
        await using var workspace = Connected(administration);
        var original = AzureServiceBusWorkspace.ToRule(ServiceBusModelFactory.RuleProperties("typed", read, null));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.SaveSubscriptionRuleAsync("orders", "billing",
            original with { Action = "SET priority = 'high'", Original = original }, replace: true));

        Assert.Contains("review again", error.Message, StringComparison.Ordinal);
        Assert.Empty(administration.Writes);
    }

    [Fact]
    public async Task Replace_UnchangedLiveRuleIsUpdated()
    {
        var administration = new RulesAdministration();
        administration.Rules["eu"] = ServiceBusModelFactory.RuleProperties("eu", new SqlRuleFilter("region = 'EU'"), null);
        await using var workspace = Connected(administration);

        await workspace.SaveSubscriptionRuleAsync("orders", "billing",
            new SubscriptionRule("eu", RuleFilterKind.Sql, "region = 'FR'") { Original = AsRead }, replace: true);

        Assert.Equal(["update eu"], administration.Writes);
        Assert.Equal("region = 'FR'", ((SqlRuleFilter)administration.Rules["eu"].Filter).SqlExpression);
    }

    [Fact]
    public async Task Delete_RefusesWhenTheRuleChangedSinceItWasRead()
    {
        var administration = new RulesAdministration();
        administration.Rules["eu"] = ServiceBusModelFactory.RuleProperties("eu", new SqlRuleFilter("region = 'US'"), null);
        await using var workspace = Connected(administration);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.DeleteSubscriptionRuleAsync("orders", "billing", AsRead));

        Assert.Contains("changed since you opened it", error.Message, StringComparison.Ordinal);
        Assert.Empty(administration.Writes);

        administration.Rules["eu"] = ServiceBusModelFactory.RuleProperties("eu", new SqlRuleFilter("region = 'EU'"), null);
        await workspace.DeleteSubscriptionRuleAsync("orders", "billing", AsRead);
        Assert.Equal(["delete eu"], administration.Writes);
    }

    // Every other change holds the operation gate, so a disconnect waits for it; rule writes did not.
    [Theory]
    [InlineData("add")]
    [InlineData("delete")]
    public async Task RuleWrites_WaitForTheOperationGateLikeOtherChanges(string write)
    {
        var administration = new RulesAdministration();
        administration.Rules["eu"] = ServiceBusModelFactory.RuleProperties("eu", new SqlRuleFilter("region = 'EU'"), null);
        await using var workspace = Connected(administration);
        var gate = (AsyncOperationGate)typeof(AzureServiceBusWorkspace)
            .GetField("_operationGate", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(workspace)!;
        var lifecycle = await gate.EnterLifecycleAsync();

        var pending = write == "add"
            ? workspace.SaveSubscriptionRuleAsync("orders", "billing", new SubscriptionRule("us", RuleFilterKind.Sql, "region = 'US'"), false)
            : workspace.DeleteSubscriptionRuleAsync("orders", "billing", "eu");
        await Task.Delay(200);
        Assert.False(pending.IsCompleted);
        Assert.Empty(administration.Writes);

        lifecycle.Dispose();
        await pending;
        Assert.Single(administration.Writes);
    }

    [Fact]
    public async Task RoutingWindow_SendsTheRuleAsItWasShownAlongWithTheChange()
    {
        var shown = new SubscriptionRule("eu", RuleFilterKind.Sql, "region = 'EU'");
        SubscriptionRule? saved = null;
        var services = new TopicRoutingServices(
            _ => Task.FromResult<IReadOnlyList<SubscriptionRules>>([new SubscriptionRules("billing", [shown])]),
            (_, rule, _, _) => { saved = rule; return Task.CompletedTask; },
            (_, _, _) => Task.CompletedTask,
            editor => { editor.SqlExpression = "region = 'FR'"; return Task.FromResult(editor.TryBuild()); },
            (_, _, _) => Task.FromResult(true));
        await using var routing = new TopicRoutingViewModel("orders", "Orders", true, "", services);
        await routing.LoadAsync();

        await routing.EditRuleCommand.ExecuteAsync(routing.Subscriptions.Single().Rules.Single());

        Assert.Equal("region = 'FR'", saved!.SqlExpression);
        Assert.Same(shown, saved.Original);
    }

    // A rule created elsewhere with SQL parameters (amount > @min with @min = 100) was shown as its expression only, and
    // saving the edit rebuilt the filter and action without them: Service Bus then refused it, or it evaluated without the
    // parameter. QueueLoom does not edit parameters, so it refuses the save and leaves the rule as it is.
    [Theory]
    [InlineData("filter")]
    [InlineData("action")]
    public async Task Replace_RefusesARuleWithSqlParametersAndLeavesItUnchanged(string where)
    {
        var filter = new SqlRuleFilter("amount > @min");
        var action = new SqlRuleAction("SET tier = @tier");
        if (where == "filter") filter.Parameters["@min"] = 100L;
        else action.Parameters["@tier"] = "gold";
        var administration = new RulesAdministration();
        administration.Rules["large"] = ServiceBusModelFactory.RuleProperties("large", filter, action);
        await using var workspace = Connected(administration);
        var read = AzureServiceBusWorkspace.ToRule(administration.Rules["large"]);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.SaveSubscriptionRuleAsync("orders", "billing",
            read with { SqlExpression = "amount > @min AND region = 'EU'", Original = read }, replace: true));

        Assert.Contains("SQL parameters", error.Message, StringComparison.Ordinal);
        Assert.Contains("nothing was saved", error.Message, StringComparison.Ordinal);
        Assert.Empty(administration.Writes);
        var kept = administration.Rules["large"];
        Assert.Equal(where == "filter" ? 1 : 0, ((SqlRuleFilter)kept.Filter).Parameters.Count);
        Assert.Equal(where == "action" ? 1 : 0, ((SqlRuleAction)kept.Action).Parameters.Count);
    }

    [Fact]
    public async Task Replace_StillSavesARuleWithoutParameters()
    {
        var administration = new RulesAdministration();
        administration.Rules["eu"] = ServiceBusModelFactory.RuleProperties("eu", new SqlRuleFilter("region = 'EU'"), null);
        await using var workspace = Connected(administration);

        await workspace.SaveSubscriptionRuleAsync("orders", "billing",
            new SubscriptionRule("eu", RuleFilterKind.Sql, "region = 'FR'") { Original = AsRead }, replace: true);

        Assert.Equal(["update eu"], administration.Writes);
    }

    private static AzureServiceBusWorkspace Connected(RulesAdministration administration)
    {
        var workspace = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(AzureServiceBusWorkspace).GetField("_administration", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, administration);
        typeof(AzureServiceBusWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
            ViewModelStateTests.CreateProfile("Isolated", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with { AllowQueueManagement = true });
        return workspace;
    }

    private sealed class RulesAdministration : ServiceBusAdministrationClient
    {
        public Dictionary<string, RuleProperties> Rules { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<string> Writes { get; } = [];

        public override Task<Response<RuleProperties>> GetRuleAsync(string topicName, string subscriptionName, string ruleName,
            CancellationToken cancellationToken = default) =>
            Rules.TryGetValue(ruleName, out var rule)
                ? Task.FromResult(Response.FromValue(Copy(rule), null!))
                : Task.FromException<Response<RuleProperties>>(new ServiceBusException("not found", ServiceBusFailureReason.MessagingEntityNotFound));

        public override Task<Response<RuleProperties>> UpdateRuleAsync(string topicName, string subscriptionName, RuleProperties rule,
            CancellationToken cancellationToken = default)
        {
            Writes.Add($"update {rule.Name}");
            Rules[rule.Name] = rule;
            return Task.FromResult(Response.FromValue(rule, null!));
        }

        public override Task<Response<RuleProperties>> CreateRuleAsync(string topicName, string subscriptionName, CreateRuleOptions options,
            CancellationToken cancellationToken = default)
        {
            Writes.Add($"create {options.Name}");
            var rule = ServiceBusModelFactory.RuleProperties(options.Name, options.Filter, options.Action);
            Rules[options.Name] = rule;
            return Task.FromResult(Response.FromValue(rule, null!));
        }

        public override Task<Response> DeleteRuleAsync(string topicName, string subscriptionName, string ruleName,
            CancellationToken cancellationToken = default)
        {
            Writes.Add($"delete {ruleName}");
            Rules.Remove(ruleName);
            return Task.FromResult<Response>(null!);
        }

        // The service returns a fresh object on every read; the workspace changes the one it got before updating.
        private static RuleProperties Copy(RuleProperties rule) => ServiceBusModelFactory.RuleProperties(rule.Name, rule.Filter, rule.Action);
    }
}

// A $Default 1=1 rule next to other rules makes those filter nothing (rules without actions are OR'ed into one copy).
public sealed class AzureCatchAllRuleTests
{
    private static readonly SubscriptionRule Default = new(SubscriptionRule.DefaultRuleName, RuleFilterKind.True, "1=1");
    private static readonly SubscriptionRule Eu = new("eu", RuleFilterKind.Sql, "region = 'EU'");

    [Fact]
    public void Subscription_WithDefaultAndOtherRulesSaysTheOthersChangeNothing()
    {
        var both = new SubscriptionRules("billing", [Default, Eu]);
        Assert.Contains("$Default is 1=1", both.CatchAllNotice, StringComparison.Ordinal);
        Assert.Contains("OR'ed", both.CatchAllNotice, StringComparison.Ordinal);
        var view = new RoutingSubscriptionViewModel(both);
        Assert.True(view.HasCatchAllNotice);
        Assert.False(view.HasWarning); // it does not "receive nothing", so it is not counted as such

        Assert.Null(new SubscriptionRules("billing", [Default]).CatchAllNotice);
        Assert.Null(new SubscriptionRules("billing", [Eu, Eu with { Name = "us", SqlExpression = "region = 'US'" }]).CatchAllNotice);
        Assert.Null(new SubscriptionRules("billing", [Default with { Action = "SET seen = true" }, Eu]).CatchAllNotice);
        Assert.NotNull(new SubscriptionRules("billing", [new SubscriptionRule("all", RuleFilterKind.Sql, " 1 = 1 "), Eu]).CatchAllNotice);
        Assert.Null(new SubscriptionRules("orders", [Default, Eu]) { Service = RoutingService.RabbitMq }.CatchAllNotice);
    }

    [Fact]
    public async Task AddingARule_NextToDefault1Equals1_TellsTheOperatorAndDeletesNothing()
    {
        RuleEditorViewModel? shown = null;
        var deleted = new List<string>();
        var rules = new List<SubscriptionRule> { Default };
        var services = new TopicRoutingServices(
            _ => Task.FromResult<IReadOnlyList<SubscriptionRules>>([new SubscriptionRules("billing", rules.ToArray())]),
            (_, rule, _, _) => { rules.Add(rule); return Task.CompletedTask; },
            (_, rule, _) => { deleted.Add(rule.Name); return Task.CompletedTask; },
            editor =>
            {
                shown = editor;
                editor.Name = "eu";
                editor.SqlExpression = "region = 'EU'";
                return Task.FromResult(editor.TryBuild());
            },
            (_, _, _) => Task.FromResult(true));
        await using var routing = new TopicRoutingViewModel("orders", "Orders", true, "", services);
        await routing.LoadAsync();

        await routing.AddRuleCommand.ExecuteAsync();

        Assert.True(shown!.HasNotice);
        Assert.Contains("$Default on billing is 1=1", shown.Notice, StringComparison.Ordinal);
        Assert.Contains("does not remove it", shown.Notice, StringComparison.Ordinal);
        Assert.Empty(deleted);
        Assert.Equal(["$Default", "eu"], rules.Select(rule => rule.Name));
        Assert.True(routing.Subscriptions.Single().HasCatchAllNotice);

        // Changing $Default itself is how the operator resolves it: no notice there.
        await routing.EditRuleCommand.ExecuteAsync(routing.Subscriptions.Single().Rules.First());
        Assert.False(shown.HasNotice);
    }
}
