using QueueLoom.App.ViewModels;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;

namespace QueueLoom.Tests;

public sealed class StaticRoutingRegressionTests
{
    [Theory]
    [InlineData("initial")]
    [InlineData("refresh")]
    [InlineData("save")]
    [InlineData("delete")]
    public async Task Timeout_IsReportedAndBusyResets(string action)
    {
        var failLoad = action == "initial";
        var services = Services(_ => failLoad
            ? Task.FromException<IReadOnlyList<SubscriptionRules>>(new TaskCanceledException("management timeout"))
            : Task.FromResult<IReadOnlyList<SubscriptionRules>>([Rules()]),
            (_, _, _, _) => Task.FromException(new TaskCanceledException("management timeout")),
            (_, _, _) => Task.FromException(new TaskCanceledException("management timeout")));
        var vm = new TopicRoutingViewModel("events", "Fake", true, "", services);
        var failure = await Record.ExceptionAsync(async () =>
        {
            await vm.LoadAsync();
            if (action == "refresh") { failLoad = true; await vm.RefreshCommand.ExecuteAsync(); }
            if (action == "save") await vm.AddRuleCommand.ExecuteAsync();
            if (action == "delete") await vm.DeleteRuleCommand.ExecuteAsync(vm.Selected!.Rules[0]);
        });
        Assert.Null(failure);
        Assert.False(vm.IsBusy);
        Assert.Contains("management timeout", vm.Error, StringComparison.Ordinal);
        Assert.True(vm.RefreshCommand.CanExecute(null));
    }

    [Fact]
    public async Task RealLoadCancellation_PropagatesAndResetsBusy()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var vm = new TopicRoutingViewModel("events", "Fake", true, "", Services(
            token => Task.FromCanceled<IReadOnlyList<SubscriptionRules>>(token)));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => vm.LoadAsync(cts.Token));
        Assert.False(vm.IsBusy);
        Assert.False(vm.HasError);
    }

    [Fact]
    public async Task RealSaveCancellation_ResetsBusy()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var vm = new TopicRoutingViewModel("events", "Fake", true, "", Services(
            _ => Task.FromResult<IReadOnlyList<SubscriptionRules>>([Rules()]), async (_, _, _, token) =>
            { started.SetResult(); await Task.Delay(Timeout.InfiniteTimeSpan, token); }));
        await vm.LoadAsync();
        var save = vm.AddRuleCommand.ExecuteAsync();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        vm.AddRuleCommand.Cancel();
        await save.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(vm.IsBusy);
        Assert.False(vm.HasError);
    }

    [Theory]
    [InlineData(RoutingService.PubSub, ApplicationPropertyType.Int32, "007", "attributes.code = \"007\"")]
    [InlineData(RoutingService.Sns, ApplicationPropertyType.Boolean, "True", "{\"code\":[\"True\"]}")]
    public async Task Dialog_UneditedAttributeTextMatchesDirectAndEmittedAttributes(
        RoutingService service, ApplicationPropertyType type, string text, string expression)
    {
        var draft = new MessageDraft(EditableMessageBody.Empty, applicationProperties: [new("code", type, text)]);
        var rule = new SubscriptionRule("filter", service == RoutingService.Sns ? RuleFilterKind.SnsFilterPolicy : RuleFilterKind.PubSubFilter) { Expression = expression };
        SubscriptionRules[] rules = [new("sub", [rule]) { Service = service }];
        var direct = TopicRouting.Route("events", rules, RoutingMessage.From(draft), service);
        Assert.Equal(text, AwsMessageMapper.ToSnsAttributes(draft)["code"].StringValue);
        Assert.Equal(RoutingOutcome.Receives, direct.Subscriptions[0].Outcome);
        var vm = new TopicRoutingViewModel("events", "Fake", false, "", Services(
            _ => Task.FromResult<IReadOnlyList<SubscriptionRules>>(rules)), draft, "Draft", service);
        await vm.LoadAsync();
        Assert.Equal(direct.Subscriptions[0].Outcome, vm.Subscriptions[0].Result!.Outcome);
        // Editing another line must not normalize this attribute either.
        vm.TestProperties += "\nother = 'value'";
        vm.Check();
        Assert.True(vm.Subscriptions[0].Receives);
        vm.TestProperties = "code = 'changed'";
        vm.Check();
        Assert.True(vm.Subscriptions[0].Skips);
    }

    [Theory]
    [InlineData(false, false, RoutingOutcome.Skips)]
    [InlineData(false, true, RoutingOutcome.Receives)]
    [InlineData(true, false, RoutingOutcome.Skips)]
    [InlineData(true, true, RoutingOutcome.Receives)]
    public void SnsMissingKey_RequiresNonemptyScope(bool body, bool unrelated, RoutingOutcome expected)
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            unrelated ? new[] { new MessageApplicationProperty("other", ApplicationPropertyType.String, "present") } : [])
            { Body = unrelated ? "{\"other\":\"present\"}" : "{}" };
        Assert.Equal(expected, SnsFilterPolicy.Evaluate("{\"missing\":[{\"exists\":false}]}", body, message).Outcome);
        Assert.Equal(RoutingOutcome.Skips, SnsFilterPolicy.Evaluate("{\"missing\":[{\"exists\":true}]}", body, message).Outcome);
        if (unrelated) Assert.Equal(RoutingOutcome.Receives,
            SnsFilterPolicy.Evaluate("{\"other\":[{\"exists\":true}]}", body, message).Outcome);
    }

    [Theory]
    [InlineData("#.#.created", "created", true)]
    [InlineData("#.#.*", "created", true)]
    [InlineData("#.#.created", "order.created", true)]
    [InlineData("#.#.created", "order.deleted", false)]
    [InlineData("a.#.#.b", "a.b", true)]
    [InlineData("a.#.#.b", "a.x.y.b", true)]
    [InlineData("a.*.b", "a.b", false)]
    [InlineData("a.*.b", "a.x.y.b", false)]
    [InlineData("#", "", true)]
    [InlineData("*", "", false)]
    [InlineData("#.#.*", "", false)]
    [InlineData("created", "Created", false)]
    public void RabbitTopic_ZeroOrMoreWordsAndFallback(string pattern, string key, bool matches)
    {
        Assert.Equal(matches, RabbitBindings.TopicMatches(pattern, key));
        SubscriptionRules[] bindings =
        [new("orders", [new SubscriptionRule("binding", RuleFilterKind.TopicBinding) { Expression = pattern }]) { Service = RoutingService.RabbitMq },
         new("alternate", []) { Service = RoutingService.RabbitMq, IsFallback = true, IsExchange = true }];
        var result = TopicRouting.Route("events", bindings,
            new RoutingMessage(new EditableMessageProperties(Subject: key), Array.Empty<MessageApplicationProperty>()), RoutingService.RabbitMq);
        Assert.Equal(matches ? RoutingOutcome.Receives : RoutingOutcome.Skips, result.Subscriptions[0].Outcome);
        Assert.Equal(matches ? RoutingOutcome.Skips : RoutingOutcome.Receives, result.Subscriptions[1].Outcome);
    }

    private static SubscriptionRules Rules() => new("sub", [new("all", RuleFilterKind.True)]);
    private static TopicRoutingServices Services(Func<CancellationToken, Task<IReadOnlyList<SubscriptionRules>>> load,
        Func<string, SubscriptionRule, bool, CancellationToken, Task>? save = null,
        Func<string, SubscriptionRule, CancellationToken, Task>? delete = null) => new(load,
        save ?? ((_, _, _, _) => Task.CompletedTask), delete ?? ((_, _, _) => Task.CompletedTask),
        _ => Task.FromResult<SubscriptionRule?>(new("all", RuleFilterKind.True)), (_, _, _) => Task.FromResult(true));
}
