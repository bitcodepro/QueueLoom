using QueueLoom.App.ViewModels;
using QueueLoom.App.Models;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class OutageRoutingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Outage_RoutingRefreshPreservesDestinationKindAndFallback(bool fallback)
    {
        var queue = new SubscriptionRules("same", [new("queue-binding", RuleFilterKind.TopicBinding)]) { Service = RoutingService.RabbitMq };
        var exchange = new SubscriptionRules("same", [new("exchange-binding", RuleFilterKind.TopicBinding)]) { Service = RoutingService.RabbitMq, IsExchange = true };
        var alternate = new SubscriptionRules("same", []) { Service = RoutingService.RabbitMq, IsExchange = true, IsFallback = true };
        IReadOnlyList<SubscriptionRules> rules = [exchange, alternate, queue];
        SubscriptionRule? saved = null;
        var services = new TopicRoutingServices(_ => Task.FromResult(rules),
            (_, rule, _, _) => { saved = rule; return Task.CompletedTask; }, (_, _, _) => Task.CompletedTask,
            _ => Task.FromResult<SubscriptionRule?>(new("new-binding", RuleFilterKind.TopicBinding)), (_, _, _) => Task.FromResult(true));
        await using var vm = new TopicRoutingViewModel("events", "Fake", true, "", services, service: RoutingService.RabbitMq);
        await vm.LoadAsync();
        vm.Selected = vm.Subscriptions.Single(s => s.Source.IsExchange && s.Source.IsFallback == fallback);
        rules = [queue, exchange, alternate];
        await vm.LoadAsync();
        Assert.True(vm.Selected!.Source.IsExchange);
        Assert.Equal(fallback, vm.Selected.Source.IsFallback);
        if (!fallback)
        {
            await vm.AddRuleCommand.ExecuteAsync();
            Assert.NotNull(saved);
            Assert.True(saved.ToExchange);
        }
    }
}

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Outage_ReplayTargetDoesNotCrossAnEnvironmentAfterConnectionClears(bool failConnect)
    {
        var a = CreateProfile("A", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var b = CreateProfile("B", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("target", ServiceBusEntityRuntime.Empty)]) };
        await using var vm = CreateViewModel(new FakeProfileRepository([a, b], a.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);
        Assert.NotEqual(NavigationPage.Backups, vm.CurrentPage);
        if (failConnect)
        {
            vm.SelectedProfile = vm.Profiles.Single(p => p.Id == b.Id);
            workspace.OnConnect = () => throw new IOException("isolated connect failure");
            await vm.ConnectCommand.ExecuteAsync();
            workspace.OnConnect = null;
        }
        else await vm.DisconnectCommand.ExecuteAsync();
        Assert.Null(vm.ConnectedProfileId);
        // Continue through the real reconnect; retaining a same-name target here is the regression.
        vm.SelectedProfile = vm.Profiles.Single(p => p.Id == b.Id);
        await vm.ConnectCommand.ExecuteAsync();
        Assert.Equal(b.Id, vm.ConnectedProfileId);
        Assert.Null(vm.ReplayDestination);
    }
}
