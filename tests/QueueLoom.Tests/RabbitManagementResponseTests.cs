using System.Net;
using System.Reflection;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

public sealed class RabbitManagementResponseTests
{
    // The queue list asks only for the fields QueueLoom reads; without a column list the management API returns every
    // queue's full statistics, which on thousands of queues is heavy for the broker and for each refresh.
    [Fact]
    public async Task TheQueueListAsksOnlyForTheColumnsItUses()
    {
        var handler = new Recording(HttpStatusCode.OK, "[]");

        await ReadTopologyAsync(handler);

        var queues = Assert.Single(handler.Requests, uri => uri.AbsolutePath.StartsWith("/api/queues/", StringComparison.Ordinal));
        var columns = System.Web.HttpUtility.ParseQueryString(queues.Query)["columns"]!.Split(',');
        Assert.Equal(["arguments", "consumers", "effective_policy_definition", "messages_ready", "messages_unacknowledged", "name", "state", "type"],
            columns.Order());
    }

    // A proxy in front of the management API answers with a large HTML page: the error says what happened in one
    // bounded line instead of carrying the whole page into the status line and Activity.
    [Fact]
    public async Task AnErrorPageIsCutToOneShortLine()
    {
        var page = "<html>\n<body>\n" + string.Concat(Enumerable.Repeat("<p>Bad gateway</p>\n", 50_000)) + "</body></html>";
        var handler = new Recording(HttpStatusCode.BadGateway, page);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ReadTopologyAsync(handler));

        Assert.StartsWith("RabbitMQ the management API answered 502", error.Message, StringComparison.Ordinal);
        Assert.True(error.Message.Length < 1_200, $"{error.Message.Length} characters.");
        Assert.DoesNotContain('\n', error.Message);
        Assert.EndsWith("…", error.Message, StringComparison.Ordinal);
    }

    private static async Task ReadTopologyAsync(HttpMessageHandler handler)
    {
        await using var workspace = new RabbitMqWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(RabbitMqWorkspace).GetField("_management", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(workspace, new HttpClient(handler) { BaseAddress = new Uri("http://broker.invalid/") });
        var read = typeof(RabbitMqWorkspace).GetMethod("ReadTopologyAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<ServiceBusTopology>)read.Invoke(workspace, [CancellationToken.None])!;
    }

    private sealed class Recording(HttpStatusCode status, string body) : HttpMessageHandler
    {
        public List<Uri> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request.RequestUri!);
            return Task.FromResult(new HttpResponseMessage(status) { Content = new StringContent(body) });
        }
    }
}
