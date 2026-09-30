using System.IO.Pipelines;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Mcp;
using static QueueLoom.Tests.ViewModelStateTests;

namespace QueueLoom.Tests;

public sealed class McpServerTests
{
    private static readonly ServiceBusQueue Orders = new(
        "orders",
        new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 4, deadLetter: 2)),
        ServiceBusEntityStatus.Active);

    [Fact]
    public async Task ListsReadAndChangeToolsWithHonestAnnotations()
    {
        await using var server = await McpTestServer.StartAsync();

        var tools = await server.Client.ListToolsAsync();

        Assert.Equal(
            ["delete_dead_letter_messages", "export_messages", "get_dead_letter_history", "get_entities", "list_environments", "peek_messages",
             "purge_dead_letters", "resend_dead_letters", "scan_dead_letters", "search_dead_letters", "send_message"],
            tools.Select(tool => tool.Name).Order());
        foreach (var name in new[] { "list_environments", "get_entities", "scan_dead_letters", "peek_messages", "search_dead_letters", "export_messages", "get_dead_letter_history" })
        {
            Assert.True(tools.Single(tool => tool.Name == name).ProtocolTool.Annotations?.ReadOnlyHint);
        }
        Assert.True(tools.Single(tool => tool.Name == "delete_dead_letter_messages").ProtocolTool.Annotations?.DestructiveHint);
        Assert.False(tools.Single(tool => tool.Name == "send_message").ProtocolTool.Annotations?.ReadOnlyHint ?? false);
    }

    [Fact]
    public async Task DeadLetterHistory_ComesFromScansAndMonitorChecks()
    {
        await using var server = await McpTestServer.StartAsync();

        var empty = await server.CallAsync("get_dead_letter_history");
        Assert.Equal(0, empty.GetProperty("sampleCount").GetInt32());
        Assert.Contains("Nothing was recorded", empty.GetProperty("note").GetString(), StringComparison.Ordinal);

        await server.CallAsync("scan_dead_letters");
        var history = await server.CallAsync("get_dead_letter_history", new() { ["hours"] = 6 });

        Assert.Equal(1, history.GetProperty("sampleCount").GetInt32());
        Assert.Equal(2, history.GetProperty("now").GetInt64());
        Assert.Equal("orders", history.GetProperty("sources")[0].GetProperty("source").GetString());
        Assert.Contains("1-720", await server.CallForErrorAsync("get_dead_letter_history", new() { ["hours"] = 5000 }), StringComparison.Ordinal);
    }

    public sealed class MemoryHistoryStore : IDeadLetterHistoryStore
    {
        private readonly List<DeadLetterHistorySample> _samples = [];

        public void Append(DeadLetterHistorySample sample)
        {
            lock (_samples)
            {
                _samples.Add(sample);
            }
        }

        public IReadOnlyList<DeadLetterHistorySample> Read(Guid profileId, DateTimeOffset since)
        {
            lock (_samples)
            {
                return _samples.Where(sample => sample.ProfileId == profileId && sample.At >= since).ToArray();
            }
        }
    }

    [Fact]
    public async Task ReadOnlyMode_OffersNoChangeTools()
    {
        await using var server = await McpTestServer.StartAsync(readOnly: true);

        var tools = await server.Client.ListToolsAsync();

        Assert.DoesNotContain(tools, tool => tool.ProtocolTool.Annotations?.ReadOnlyHint != true);
        Assert.Equal(7, tools.Count);
    }

    [Fact]
    public async Task ReadTools_WorkWithoutApprovalAndUseAReadOnlyConnection()
    {
        await using var server = await McpTestServer.StartAsync();

        var environments = await server.CallAsync("list_environments");
        var entities = await server.CallAsync("get_entities");
        var peek = await server.CallAsync("peek_messages", new() { ["entity"] = "orders" });

        Assert.Equal("Development", environments[0].GetProperty("name").GetString());
        Assert.Equal(2, entities.GetProperty("entities")[0].GetProperty("deadLetter").GetInt64());
        Assert.Equal(2, peek.GetProperty("messages").GetArrayLength());
        Assert.Empty(server.Approver.Requests);
        Assert.Equal(ProfileAccessMode.ReadOnly, server.Workspace.ConnectedAccessMode);
    }

    [Fact]
    public async Task Search_ReturnsMatchesTheModelCanDelete()
    {
        await using var server = await McpTestServer.StartAsync();

        var search = await server.CallAsync("search_dead_letters", new() { ["query"] = "correlation-42" });

        var match = search.GetProperty("messages")[0];
        Assert.Equal("orders", match.GetProperty("entity").GetString());
        Assert.Equal("dlq", match.GetProperty("subQueue").GetString());
        Assert.Equal(2, match.GetProperty("sequenceNumber").GetInt64());
    }

    [Fact]
    public async Task Delete_RunsOnlyAfterApprovalAndOnlyForTheListedMessages()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);

        var result = await server.CallAsync("delete_dead_letter_messages", new()
        {
            ["messages"] = new[] { new { entity = "orders", subQueue = "dlq", sequenceNumber = 2L, messageId = (string?)null } },
            ["reason"] = "Poison messages from the failed deployment"
        });

        var request = Assert.Single(server.Approver.Requests);
        Assert.Equal("Delete dead-letter messages", request.Action);
        Assert.Contains("Poison messages from the failed deployment", request.Details, StringComparison.Ordinal);
        Assert.True(result.GetProperty("approved").GetBoolean());
        var delete = Assert.Single(server.Workspace.DeleteRequests);
        Assert.Equal([2L], delete.Messages.Select(message => message.SequenceNumber));
        Assert.Equal([ProfileAccessMode.ReadWrite, ProfileAccessMode.ReadOnly], server.Workspace.AccessModeChanges);
    }

    [Fact]
    public async Task ResendMove_SendsTheOriginalsBackAndThenRemovesThem()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);

        var result = await server.CallAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { new { entity = "orders", subQueue = "dlq", sequenceNumber = 3L, messageId = (string?)null } },
            ["mode"] = "move",
            ["reason"] = "The consumer bug is fixed"
        });

        var request = Assert.Single(server.Approver.Requests);
        Assert.Equal("Move dead-letter messages", request.Action);
        Assert.Contains("to orders: 1", request.Details, StringComparison.Ordinal);
        Assert.True(result.GetProperty("approved").GetBoolean());
        var sent = Assert.Single(server.Workspace.SentMessages);
        Assert.Equal("orders", sent.Destination.Name);
        Assert.Equal("correlation-42", sent.Message.Properties.CorrelationId);
        Assert.Equal([3L], Assert.Single(server.Workspace.DeleteRequests).Messages.Select(message => message.SequenceNumber));
        Assert.Equal([ProfileAccessMode.ReadWrite, ProfileAccessMode.ReadOnly], server.Workspace.AccessModeChanges);
    }

    [Fact]
    public async Task ResendCopy_KeepsTheOriginalsAndReportsMissingMessages()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);

        var result = await server.CallAsync("resend_dead_letters", new()
        {
            ["messages"] = new[]
            {
                new { entity = "orders", subQueue = "dlq", sequenceNumber = 2L, messageId = (string?)null },
                new { entity = "orders", subQueue = "dlq", sequenceNumber = 99L, messageId = (string?)null }
            },
            ["mode"] = "copy",
            ["reason"] = "Replay for the new consumer"
        });

        Assert.Contains("1 listed message(s) were not found", Assert.Single(server.Approver.Requests).Details, StringComparison.Ordinal);
        Assert.Single(server.Workspace.SentMessages);
        Assert.Empty(server.Workspace.DeleteRequests);
        Assert.Contains("1 not found", result.GetProperty("summary").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResendWithUnknownMode_IsRejectedBeforeAskingTheUser()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);

        var error = await server.CallForErrorAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { new { entity = "orders", subQueue = "dlq", sequenceNumber = 2L, messageId = (string?)null } },
            ["mode"] = "teleport",
            ["reason"] = "Retry"
        });

        Assert.Contains("'copy' or 'move'", error, StringComparison.Ordinal);
        Assert.Empty(server.Approver.Requests);
    }

    [Fact]
    public async Task Export_WritesTheMessagesToTheExportFolderWithoutApproval()
    {
        await using var server = await McpTestServer.StartAsync();

        var result = await server.CallAsync("export_messages", new()
        {
            ["entity"] = "orders",
            ["format"] = "csv",
            ["fileName"] = "../../outside/orders.csv"
        });

        var path = result.GetProperty("path").GetString()!;
        Assert.Equal(server.ExportDirectory, Path.GetDirectoryName(path));
        Assert.Equal("orders.csv", Path.GetFileName(path));
        Assert.Equal(2, result.GetProperty("count").GetInt32());
        Assert.Equal(3, File.ReadAllLines(path).Length);
        Assert.Empty(server.Approver.Requests);
        Assert.Empty(server.Workspace.AccessModeChanges);
    }

    [Fact]
    public async Task Export_OfASearchNeverOverwritesAnEarlierFile()
    {
        await using var server = await McpTestServer.StartAsync();

        var first = await server.CallAsync("export_messages", new() { ["query"] = "correlation-42", ["fileName"] = "found" });
        var second = await server.CallAsync("export_messages", new() { ["query"] = "correlation-42", ["fileName"] = "found" });

        Assert.EndsWith("found.json", first.GetProperty("path").GetString(), StringComparison.Ordinal);
        Assert.EndsWith("found (2).json", second.GetProperty("path").GetString(), StringComparison.Ordinal);
        using var json = JsonDocument.Parse(File.ReadAllText(second.GetProperty("path").GetString()!));
        Assert.Equal(1, json.RootElement.GetArrayLength());
    }

    [Fact]
    public async Task DeclinedChange_TouchesNothing()
    {
        await using var server = await McpTestServer.StartAsync(approve: false);

        var result = await server.CallAsync("purge_dead_letters", new()
        {
            ["entity"] = "orders",
            ["maxMessages"] = 100,
            ["reason"] = "Clean up"
        });

        Assert.False(result.GetProperty("approved").GetBoolean());
        Assert.StartsWith("Not done", result.GetProperty("summary").GetString(), StringComparison.Ordinal);
        Assert.Empty(server.Workspace.PurgeRequests);
        Assert.Empty(server.Workspace.AccessModeChanges);
    }

    [Fact]
    public async Task ChangeWithoutReason_IsRejectedBeforeAskingTheUser()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);

        var error = await server.CallForErrorAsync("send_message", new()
        {
            ["destination"] = "orders",
            ["body"] = "hello",
            ["reason"] = " "
        });

        Assert.Contains("reason", error, StringComparison.Ordinal);
        Assert.Empty(server.Approver.Requests);
        Assert.Empty(server.Workspace.SentMessages);
    }

    [Fact]
    public async Task UnknownEntity_ReturnsAHelpfulError()
    {
        await using var server = await McpTestServer.StartAsync();

        var error = await server.CallForErrorAsync("peek_messages", new() { ["entity"] = "missing" });

        Assert.Contains("get_entities", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ElicitationApprover_AsksTheClientUserAndChecksTheProductionName()
    {
        string? shownMessage = null;
        await using var server = await McpTestServer.StartAsync(
            approver: new ElicitationApprover(),
            environment: EnvironmentKind.Production,
            elicit: request =>
            {
                shownMessage = request.Message;
                return new ElicitResult
                {
                    Action = "accept",
                    Content = new Dictionary<string, JsonElement>
                    {
                        ["approve"] = JsonSerializer.SerializeToElement(true),
                        ["confirmEnvironmentName"] = JsonSerializer.SerializeToElement("wrong")
                    }
                };
            });

        var result = await server.CallAsync("send_message", new()
        {
            ["destination"] = "orders",
            ["body"] = "hello",
            ["reason"] = "Retry order 42"
        });

        Assert.Contains("production environment", shownMessage, StringComparison.Ordinal);
        Assert.False(result.GetProperty("approved").GetBoolean());
        Assert.Empty(server.Workspace.SentMessages);
    }

    private sealed class RecordingApprover(bool approve) : IOperationApprover
    {
        public List<ApprovalRequest> Requests { get; } = [];

        public Task<ApprovalDecision> RequestAsync(ApprovalRequest request, McpServer server, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            return Task.FromResult(approve
                ? ApprovalDecision.Approve("Approved in test.")
                : ApprovalDecision.Deny("The user declined the change."));
        }
    }

    private sealed class McpTestServer : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stop = new();
        private Task _run = Task.CompletedTask;

        public required McpClient Client { get; init; }
        public required FakeWorkspace Workspace { get; init; }
        public RecordingApprover Approver { get; private init; } = new(false);
        public required string ExportDirectory { get; init; }
        private Task Run { init => _run = value; }
        private CancellationTokenSource Stop { init => _stop = value; }

        public static async Task<McpTestServer> StartAsync(
            bool readOnly = false,
            bool approve = false,
            IOperationApprover? approver = null,
            EnvironmentKind environment = EnvironmentKind.Development,
            Func<ElicitRequestParams, ElicitResult>? elicit = null)
        {
            var profile = CreateProfile(environment == EnvironmentKind.Production ? "Orders" : "Development", environment);
            var workspace = new FakeWorkspace
            {
                Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]),
                BrowseMessages =
                [
                    SearchMessage(Orders.Reference, 2, "2026-08-12T10:00:00Z"),
                    SearchMessage(Orders.Reference, 3, "2026-08-12T10:01:00Z")
                ],
                SearchMatches = { [profile.Id] = [SearchMessage(Orders.Reference, 2, "2026-08-12T10:00:00Z")] }
            };
            workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow,
                [new DeadLetterEntitySnapshot(Orders.Reference, 2)]);
            var recording = new RecordingApprover(approve);
            var history = new MemoryHistoryStore();

            var clientToServer = new Pipe();
            var serverToClient = new Pipe();
            var stop = new CancellationTokenSource();
            var exportDirectory = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "mcp-exports", Guid.NewGuid().ToString("N"));
            var run = QueueLoomMcpServer.RunAsync(
                new McpServerSettings(readOnly, exportDirectory),
                services =>
                {
                    services.AddSingleton<IProfileRepository>(new FakeProfileRepository([profile], profile.Id));
                    services.AddSingleton<IServiceBusWorkspace>(workspace);
                    services.AddSingleton<IDeadLetterHistoryStore>(history);
                    services.AddSingleton(approver ?? recording);
                },
                input: clientToServer.Reader.AsStream(),
                output: serverToClient.Writer.AsStream(),
                cancellationToken: stop.Token);

            var options = new McpClientOptions();
            if (elicit is not null)
            {
                options.Handlers.ElicitationHandler = (request, _) => ValueTask.FromResult(elicit(request!));
            }
            var client = await McpClient.CreateAsync(
                new StreamClientTransport(clientToServer.Writer.AsStream(), serverToClient.Reader.AsStream()),
                options);
            return new McpTestServer
            {
                History = history,
                Client = client,
                Workspace = workspace,
                Approver = recording,
                ExportDirectory = exportDirectory,
                Run = run,
                Stop = stop
            };
        }

        public required MemoryHistoryStore History { get; init; }

        public async Task<JsonElement> CallAsync(string tool, Dictionary<string, object?>? arguments = null)
        {
            var result = await Client.CallToolAsync(tool, arguments ?? []);
            Assert.True(result.IsError != true, Text(result));
            return JsonDocument.Parse(Text(result)).RootElement.Clone();
        }

        public async Task<string> CallForErrorAsync(string tool, Dictionary<string, object?> arguments)
        {
            var result = await Client.CallToolAsync(tool, arguments);
            Assert.True(result.IsError);
            return Text(result);
        }

        public async ValueTask DisposeAsync()
        {
            await Client.DisposeAsync();
            _stop.Cancel();
            try
            {
                await _run;
            }
            catch (OperationCanceledException)
            {
            }
            _stop.Dispose();
            if (Directory.Exists(ExportDirectory))
            {
                Directory.Delete(ExportDirectory, recursive: true);
            }
        }

        private static string Text(CallToolResult result) =>
            string.Concat(result.Content.OfType<TextContentBlock>().Select(block => block.Text));
    }
}
