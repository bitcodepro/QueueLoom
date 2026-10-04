using System.IO.Pipelines;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Mcp;
using static QueueLoom.Tests.ViewModelStateTests;

namespace QueueLoom.Tests;

public sealed class McpSendApprovalDetailsTests
{
    [Fact]
    public async Task SendMessage_ApprovalShowsTheApplicationPropertiesAndContentTypeThatWillBeSent()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        var orders = new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 1)), ServiceBusEntityStatus.Active);
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [orders]) };
        var approver = new CapturingApprover();
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        using var stop = new CancellationTokenSource();
        var run = QueueLoomMcpServer.RunAsync(
            new McpServerSettings(false, Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "mcp-hunt", Guid.NewGuid().ToString("N"))),
            services =>
            {
                services.AddSingleton<IProfileRepository>(new FakeProfileRepository([profile], profile.Id));
                services.AddSingleton<IServiceBusWorkspace>(workspace);
                services.AddSingleton<IOperationApprover>(approver);
            },
            input: clientToServer.Reader.AsStream(),
            output: serverToClient.Writer.AsStream(),
            cancellationToken: stop.Token);
        var client = await McpClient.CreateAsync(
            new StreamClientTransport(clientToServer.Writer.AsStream(), serverToClient.Reader.AsStream()), new McpClientOptions());

        var result = await client.CallToolAsync("send_message", new Dictionary<string, object?>
        {
            ["destination"] = "orders",
            ["body"] = "{\"orderId\":42}",
            ["reason"] = "Replay order 42",
            ["contentType"] = "application/x-hidden",
            ["applicationProperties"] = new Dictionary<string, string> { ["tenant"] = "everyone-else" }
        });
        await client.DisposeAsync();
        stop.Cancel();
        try { await run; } catch (OperationCanceledException) { }

        Assert.True(result.IsError != true, string.Concat(result.Content.OfType<TextContentBlock>().Select(block => block.Text)));
        var sent = Assert.Single(workspace.SentMessages);
        Assert.Equal("everyone-else", Assert.Single(sent.Message.ApplicationProperties).Value);
        var request = Assert.Single(approver.Requests);
        // The approver must see everything that is sent; properties steer subscription filters and consumers.
        Assert.Contains("tenant", request.Details, StringComparison.Ordinal);
        Assert.Contains("everyone-else", request.Details, StringComparison.Ordinal);
        Assert.Contains("application/x-hidden", request.Details, StringComparison.Ordinal);
    }

    private sealed class CapturingApprover : IOperationApprover
    {
        public List<ApprovalRequest> Requests { get; } = [];

        public Task<ApprovalDecision> RequestAsync(ApprovalRequest request, McpServer server, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            return Task.FromResult(ApprovalDecision.Approve("Approved in test."));
        }
    }
}
