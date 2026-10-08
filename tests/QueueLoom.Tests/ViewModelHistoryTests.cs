using QueueLoom.App.Models;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task History_ScansAreRecordedAndDrawnOnMonitors()
    {
        var directory = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "history", Guid.NewGuid().ToString("N"));
        var profile = CreateProfile("Orders", EnvironmentKind.Development);
        var orders = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 4)))])
        };
        workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow, [new DeadLetterEntitySnapshot(orders, 4)]);
        try
        {
            var store = new JsonLinesDeadLetterHistoryStore(Path.Combine(directory, "dlq-history.jsonl"));
            await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, history: store);
            await viewModel.InitializeAsync();
            await viewModel.ConnectCommand.ExecuteAsync();

            await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
            viewModel.SelectedNavigation = viewModel.Navigation.Single(item => item.Key == nameof(NavigationPage.Monitors));
            await viewModel.HistoryRefresh;

            Assert.True(viewModel.HasHistory);
            Assert.Equal(profile.Id, viewModel.HistoryProfile?.Id);
            Assert.Equal("4", viewModel.HistoryNowText);
            Assert.Equal("orders", Assert.Single(viewModel.HistorySources).Name);

            viewModel.HistoryRange = "Last 6 hours";
            await viewModel.HistoryRefresh;
            Assert.Single(viewModel.HistoryPoints);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }
}
