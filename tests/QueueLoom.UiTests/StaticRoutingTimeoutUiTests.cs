using Avalonia.Threading;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.Routing;

namespace QueueLoom.UiTests;

public sealed class StaticRoutingTimeoutUiTests
{
    [Theory]
    [InlineData("opened")]
    [InlineData("refresh")]
    [InlineData("save")]
    [InlineData("delete")]
    public Task TimeoutFromWindowOrCommand_DoesNotEscapeDispatcher(string action) => UiSession.RunAsync(async () =>
    {
        var dispatcherErrors = new List<Exception>();
        void Handle(object? sender, DispatcherUnhandledExceptionEventArgs e)
        { dispatcherErrors.Add(e.Exception); e.Handled = true; }
        Dispatcher.UIThread.UnhandledException += Handle;
        var failLoad = action == "opened";
        var services = new TopicRoutingServices(
            _ => failLoad ? Task.FromException<IReadOnlyList<SubscriptionRules>>(new TaskCanceledException("management timeout"))
                : Task.FromResult<IReadOnlyList<SubscriptionRules>>([new("sub", [new("all", RuleFilterKind.True)])]),
            (_, _, _, _) => Task.FromException(new TaskCanceledException("management timeout")),
            (_, _, _) => Task.FromException(new TaskCanceledException("management timeout")),
            _ => Task.FromResult<SubscriptionRule?>(new("all", RuleFilterKind.True)), (_, _, _) => Task.FromResult(true));
        var vm = new TopicRoutingViewModel("events", "Fake", true, "", services);
        var window = new TopicRoutingWindow(vm);
        try
        {
            window.Show();
            Dispatcher.UIThread.RunJobs();
            if (action == "refresh") { failLoad = true; vm.RefreshCommand.Execute(null); }
            if (action == "save") vm.AddRuleCommand.Execute(null);
            if (action == "delete") vm.DeleteRuleCommand.Execute(vm.Selected!.Rules[0]);
            await Task.Yield();
            Dispatcher.UIThread.RunJobs();
            Assert.Empty(dispatcherErrors);
            Assert.False(vm.IsBusy);
            Assert.Contains("management timeout", vm.Error, StringComparison.Ordinal);
        }
        finally { window.Close(); Dispatcher.UIThread.RunJobs(); Dispatcher.UIThread.UnhandledException -= Handle; }
    });
}
