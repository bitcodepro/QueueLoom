using Azure.Messaging.ServiceBus;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

// The Service Bus emulator reports no counts, so QueueLoom samples them by peeking. The active messages of a
// session-enabled entity can only be peeked session by session, so they were not sampled, and the count was set to
// zero: an entity full of session messages showed 0 active, exactly like an empty one. It is now unknown: the app
// shows a dash and get_entities says the active count is unknown. An auto-forwarding entity really holds nothing, so
// its zero stays.
public sealed class EmulatorSessionActiveCountTests
{
    [Theory]
    [InlineData(true, null, true)]
    [InlineData(false, "elsewhere", false)]
    [InlineData(false, null, false)]
    public async Task OnlyASessionEntitysActiveCountIsUnknown(bool requiresSession, string? forwardTo, bool unknown)
    {
        var sampled = new List<SubQueue>();

        var runtime = await AzureServiceBusWorkspace.SampleEmulatorRuntimeAsync(ServiceBusEntityReference.Queue("orders"), requiresSession, forwardTo, null,
            (_, subQueue, _) => { sampled.Add(subQueue); return Task.FromResult(3L); }, CancellationToken.None);

        Assert.Equal(unknown, runtime.ActiveCountUnavailable);
        Assert.Equal(!requiresSession && forwardTo is null, sampled.Contains(SubQueue.None));
    }

    [Fact]
    public async Task TheAppShowsADashAndMcpSaysUnknown()
    {
        var runtime = await AzureServiceBusWorkspace.SampleEmulatorRuntimeAsync(ServiceBusEntityReference.Queue("orders"), true, null, null,
            (_, _, _) => Task.FromResult(2L), CancellationToken.None);

        var item = new EntityItemViewModel(ServiceBusEntityReference.Queue("orders"), runtime, ServiceBusEntityStatus.Active, requiresSession: true, indent: 0);
        var info = QueueLoom.Mcp.McpMapping.ToInfo(ServiceBusEntityReference.Queue("orders"), runtime, ServiceBusEntityStatus.Active);

        Assert.Equal("—", item.ActiveDisplay);
        Assert.Equal("2", item.DeadLettersDisplay);
        Assert.True(info.ActiveCountUnknown);
    }
}
