using Microsoft.Extensions.Logging;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(MessagingProvider.AmazonSqsSns)]
    [InlineData(MessagingProvider.GooglePubSub)]
    [InlineData(MessagingProvider.RabbitMq)]
    public async Task CycleTwoBrowse_NonPositionalPagingStopsAtRequestLimitWithAccurateStatus(MessagingProvider provider)
    {
        var profile = CreateProfile("isolated", EnvironmentKind.Development) with { Provider = provider };
        var source = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace
        {
            BrowseMessages = Enumerable.Range(1, 1100).Select(i => new BrowsedMessage(source, ServiceBusSubQueue.Active,
                i, "body"u8.ToArray(), new EditableMessageProperties(MessageId: "id-" + i)) { HasSequenceNumber = false }).ToArray()
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        vm.SelectedEntity = new EntityItemViewModel(source, new ServiceBusEntityRuntime(new ServiceBusMessageCounts()), ServiceBusEntityStatus.Active, false, 0);
        await vm.BrowseSelectedActiveCommand.ExecuteAsync();
        for (var i = 0; i < 9; i++) await vm.LoadMoreMessagesCommand.ExecuteAsync();
        Assert.Equal(1000, vm.Messages.Count);
        Assert.False(vm.CanLoadMoreMessages);
        Assert.Contains("1,000", vm.BrowsePageStatus, StringComparison.Ordinal);
        Assert.DoesNotContain("End of available", vm.BrowsePageStatus, StringComparison.Ordinal);
        Assert.Equal(10, workspace.BrowseRequests.Count);
        Assert.Equal(1000, workspace.BrowseRequests[^1].MaxMessages);
    }

    [Fact]
    public async Task CycleTwoLogging_OperationRetainsOriginalErrorWhenLoggerThrows()
    {
        var profile = CreateProfile("isolated", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { FailNextConnection = true };
        var logger = new FailingOperationLogger();
        await using var vm = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(), workspace, new FakeDialogService(), logger: logger);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        Assert.Contains("Connection failed", vm.ErrorText, StringComparison.Ordinal);
        Assert.False(vm.IsBusy);
        Assert.Equal("Connecting failed", vm.StatusText);
        Assert.Contains("Failed", vm.Diagnostics.Capture().Json, StringComparison.Ordinal);
        Assert.NotEmpty(vm.Activity);
        logger.Fail = false;
        await vm.ConnectCommand.ExecuteAsync();
        Assert.True(vm.IsConnected);
    }

    private sealed class FailingOperationLogger : ILogger<MainWindowViewModel>
    {
        public bool Fail { get; set; } = true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        { if (Fail) throw new System.Text.RegularExpressions.RegexMatchTimeoutException(); }
    }
}
