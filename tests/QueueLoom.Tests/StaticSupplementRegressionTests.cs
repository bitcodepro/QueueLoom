using System.Text.Json;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

public sealed class StaticSupplementRegressionTests
{
    [Theory]
    [InlineData("", true)]
    [InlineData("created", false)]
    public void RabbitEmptyTopicBinding_MatchesOnlyAnEmptyRoutingKey(string key, bool expected)
    {
        Assert.Equal(expected, RabbitBindings.TopicMatches("", key));
        Assert.Equal(expected, RabbitMqTopologyIndex.TopicMatches("", key));
    }

    [Theory]
    [InlineData("#.#.created")]
    [InlineData("#.#.*")]
    public void RabbitTopology_UsesTheSameWordMatchingForDeadLetterQueues(string pattern)
    {
        using var source = JsonDocument.Parse("""{"name":"orders","arguments":{"x-dead-letter-exchange":"dlx","x-dead-letter-routing-key":"created"}}""");
        using var target = JsonDocument.Parse("""{"name":"parking","arguments":{}}""");
        var index = new RabbitMqTopologyIndex([RabbitQueueInfo.From(source.RootElement), RabbitQueueInfo.From(target.RootElement)],
            [new("dlx", "topic")], [new("dlx", "parking", pattern)]);
        Assert.Equal("parking", index.DeadLetterQueueOf("orders"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SnsMissingKey_OnlyCountsPropertiesInTheSelectedScope(bool bodyScope)
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty, bodyScope
            ? [new MessageApplicationProperty("other", ApplicationPropertyType.String, "present")]
            : Array.Empty<MessageApplicationProperty>()) { Body = bodyScope ? "{}" : "{\"other\":\"present\"}" };
        Assert.Equal(RoutingOutcome.Skips, SnsFilterPolicy.Evaluate("{\"missing\":[{\"exists\":false}]}", bodyScope, message).Outcome);
    }

    [Theory]
    [InlineData("initial")]
    [InlineData("refresh")]
    [InlineData("save")]
    [InlineData("delete")]
    public async Task RoutingDisposal_DrainsIndependentLoadAndCommandCleanup(string action)
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blockLoad = action == "initial";
        async Task Block(CancellationToken token)
        {
            started.TrySetResult();
            try { await Task.Delay(Timeout.InfiniteTimeSpan, token); }
            finally { cleanup.TrySetResult(); await release.Task; }
        }
        var services = new TopicRoutingServices(async token =>
        {
            if (blockLoad) await Block(token);
            return new SubscriptionRules[] { new("sub", [new("all", RuleFilterKind.True)]) };
        }, (_, _, _, token) => Block(token), (_, _, token) => Block(token),
            _ => Task.FromResult<SubscriptionRule?>(new("all", RuleFilterKind.True)), (_, _, _) => Task.FromResult(true));
        var vm = new TopicRoutingViewModel("events", "Fake", true, "", services);
        var lifetime = Assert.IsAssignableFrom<IAsyncDisposable>(vm);
        Task operation;
        if (action == "initial") operation = vm.LoadAsync();
        else
        {
            await vm.LoadAsync();
            blockLoad = action == "refresh";
            operation = action switch
            {
                "refresh" => vm.RefreshCommand.ExecuteAsync(),
                "save" => vm.AddRuleCommand.ExecuteAsync(),
                _ => vm.DeleteRuleCommand.ExecuteAsync(vm.Selected!.Rules[0])
            };
        }
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var disposal = lifetime.DisposeAsync().AsTask();
        var again = lifetime.DisposeAsync().AsTask();
        bool returnedEarly;
        try
        {
            await cleanup.Task.WaitAsync(TimeSpan.FromSeconds(5));
            returnedEarly = disposal.IsCompleted || again.IsCompleted;
        }
        finally { release.TrySetResult(); }
        if (action == "initial") await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation);
        else await operation;
        await Task.WhenAll(disposal, again).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(returnedEarly);
        Assert.False(vm.IsBusy);
        Assert.False(vm.HasError);
        Assert.False(vm.RefreshCommand.CanExecute(null));
    }
}
