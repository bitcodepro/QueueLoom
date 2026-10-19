using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// A notice a workspace raises during an operation (a Schema Registry outage, for example) reaches the operator: in the
// app's activity list once the operation ends, and through MCP in Activity with the tool's call.
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task ANoticeRaisedWhileBrowsingIsShownInActivity()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var source = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace
        {
            Snapshots = { [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(source, 1)) },
            BrowseMessages = [SearchMessage(source, 1, "2026-08-12T10:00:00Z")],
            BrowseNotice = new WorkspaceNotice("Schema Registry unavailable", "The Schema Registry answered 503 Service Unavailable.")
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        viewModel.SelectedDlqSource = Assert.Single(viewModel.FilteredDeadLetterSources);

        await viewModel.BrowseDlqSourceCommand.ExecuteAsync();

        var entry = Assert.Single(viewModel.Activity, item => item.Action == "Schema Registry unavailable");
        Assert.Equal("Warning", entry.Level);
        Assert.Contains("503", entry.Details, StringComparison.Ordinal);
    }
}

public sealed partial class McpServerTests
{
    [Fact]
    public async Task ANoticeRaisedDuringAToolIsRecordedInActivity()
    {
        await using var server = await McpTestServer.StartAsync();
        server.Workspace.BrowseNotice = new WorkspaceNotice("Schema Registry unavailable", "The Schema Registry answered 503 Service Unavailable.");

        await server.CallAsync("peek_messages", new() { ["entity"] = "orders" });

        var record = Assert.Single(server.Journal.Records, record => record.Action.EndsWith("Schema Registry unavailable", StringComparison.Ordinal));
        Assert.Equal("Warning", record.Level);
        Assert.Contains("503", record.Details, StringComparison.Ordinal);
    }
}
