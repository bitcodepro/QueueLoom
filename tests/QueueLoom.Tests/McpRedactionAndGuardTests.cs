using System.IO.Pipelines;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
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

public sealed class McpRedactionAndGuardTests
{
    private const string Secret = "TopSecretKey123=";
    private const string ConnectionError =
        "Connection failed for Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=root;SharedAccessKey=" + Secret;

    private static readonly ServiceBusQueue Orders = new(
        "orders",
        new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 4, deadLetter: 2)),
        ServiceBusEntityStatus.Active);

    private static BrowsedMessage DeadLetter(long sequenceNumber, string messageId) => new(
        Orders.Reference, ServiceBusSubQueue.DeadLetter, sequenceNumber, Encoding.UTF8.GetBytes("{\"order\":42}"),
        new EditableMessageProperties(MessageId: messageId, CorrelationId: "correlation-42"),
        enqueuedAt: DateTimeOffset.Parse("2026-08-12T10:00:00Z"));

    // (a) The same message listed with and without its Message ID must be resent once, not crash the tool.
    [Fact]
    public async Task Resend_SameMessageListedWithAndWithoutItsMessageId_IsResentOnce()
    {
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]),
            BrowseMessages = [DeadLetter(2, "order-2")]
        };

        var (result, text, _) = await CallAsync(workspace, "resend_dead_letters", new()
        {
            ["messages"] = new object[]
            {
                new { entity = "orders", subQueue = "dlq", sequenceNumber = 2L, messageId = (string?)null },
                new { entity = "orders", subQueue = "dlq", sequenceNumber = 2L, messageId = (string?)"order-2" }
            },
            ["mode"] = "copy",
            ["reason"] = "Replay after the consumer fix"
        });

        Assert.True(result.IsError != true, text);
        Assert.Single(workspace.SentMessages);
    }

    // (b) explain_dead_letters returns a queue read error to the model without redaction.
    [Fact]
    public async Task ExplainDeadLetters_RedactsCredentialsInQueueReadErrors()
    {
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]),
            CleanupOperationGate = _ => throw new InvalidOperationException(ConnectionError)
        };

        var (result, text, _) = await CallAsync(workspace, "explain_dead_letters", new());

        Assert.True(result.IsError != true, text);
        Assert.DoesNotContain(Secret, text, StringComparison.Ordinal);
        Assert.Contains("SharedAccessKey=[REDACTED]", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ScanDeadLetters_RedactsCredentialsInSourceErrors()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]) };
        workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow,
            [new DeadLetterEntitySnapshot(Orders.Reference, null, null, ConnectionError)]);

        var (result, text, _) = await CallAsync(workspace, "scan_dead_letters", new(), profile: profile);

        Assert.True(result.IsError != true, text);
        Assert.DoesNotContain(Secret, text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResendFailure_DetailReturnedToTheModelIsRedacted()
    {
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]),
            BrowseMessages = [DeadLetter(2, "order-2")],
            OnSend = () => throw new InvalidOperationException(ConnectionError)
        };

        var (result, text, _) = await CallAsync(workspace, "resend_dead_letters", new()
        {
            ["messages"] = new[] { new { entity = "orders", subQueue = "dlq", sequenceNumber = 2L, messageId = (string?)null } },
            ["mode"] = "copy",
            ["reason"] = "Replay"
        });

        Assert.True(result.IsError != true, text);
        Assert.Contains("1 failed", text, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PurgeFailure_SummaryReturnedToTheModelIsRedacted()
    {
        var inner = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]) };
        var workspace = OverridingWorkspace.Wrap(inner, (method, args) => method.Name == nameof(IServiceBusWorkspace.PurgeDeadLettersAsync)
            ? (true, Task.FromResult(new DeadLetterPurgeResult(inner.ConnectedProfileId!.Value, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow,
                [new DeadLetterPurgeSourceResult(Orders.Reference, ServiceBusSubQueue.DeadLetter, 0, ConnectionError)],
                Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "backup"))))
            : (false, null));

        var (result, text, _) = await CallAsync(workspace, "purge_dead_letters", new()
        {
            ["entity"] = "orders", ["maxMessages"] = 10, ["reason"] = "Clean up"
        });

        Assert.True(result.IsError != true, text);
        Assert.DoesNotContain(Secret, text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeleteFailure_DetailReturnedToTheModelIsRedacted()
    {
        var inner = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]) };
        var workspace = OverridingWorkspace.Wrap(inner, (method, args) =>
        {
            if (method.Name != nameof(IServiceBusWorkspace.DeleteDeadLetterMessagesAsync)) return (false, null);
            var request = (DeleteDeadLetterMessagesRequest)args![0]!;
            return (true, Task.FromResult(new DeleteDeadLetterMessagesResult(inner.ConnectedProfileId!.Value, DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow,
                request.Messages.Select(key => new DeadLetterMessageDeletionResult(key, DeadLetterMessageDeletionOutcome.Failed,
                    "Backed up, but the deletion failed; the message may still be in the queue: " + ConnectionError)),
                Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "backup"))));
        });

        var (result, text, _) = await CallAsync(workspace, "delete_dead_letter_messages", new()
        {
            ["messages"] = new[] { new { entity = "orders", subQueue = "dlq", sequenceNumber = 2L, messageId = (string?)null } },
            ["reason"] = "Poison"
        });

        Assert.True(result.IsError != true, text);
        Assert.Contains("1 failed", text, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, text, StringComparison.Ordinal);
    }

    // (d) Services without transfer dead-letter queues: the purge must be refused, not approved and reported as "0 deleted".
    [Fact]
    public async Task PurgeOfATransferDlq_OnAServiceWithoutThem_IsRefusedBeforeAskingTheUser()
    {
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]) { SupportsTransferDeadLetter = false }
        };

        var (result, text, approver) = await CallAsync(workspace, "purge_dead_letters", new()
        {
            ["entity"] = "orders", ["maxMessages"] = 100, ["reason"] = "Clean up", ["subQueue"] = "transfer-dlq"
        });

        Assert.True(result.IsError, text);
        Assert.Empty(approver.Requests);
        Assert.Empty(workspace.PurgeRequests);
    }

    // A purge that stops at the requested maxMessages (no error text) is reported as "...deleted; " and logged as an error.
    [Fact]
    public async Task PurgeThatReachesMaxMessages_SaysSoInsteadOfADanglingEmptyError()
    {
        var inner = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]) };
        var workspace = OverridingWorkspace.Wrap(inner, (method, args) => method.Name == nameof(IServiceBusWorkspace.PurgeDeadLettersAsync)
            ? (true, Task.FromResult(new DeadLetterPurgeResult(inner.ConnectedProfileId!.Value, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow,
                // What LeasedMessagingWorkspace returns when it deleted exactly maxMessages.
                [new DeadLetterPurgeSourceResult(Orders.Reference, ServiceBusSubQueue.DeadLetter, 5, LimitReached: true)],
                Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "backup"))))
            : (false, null));

        var (result, text, _) = await CallAsync(workspace, "purge_dead_letters", new()
        {
            ["entity"] = "orders", ["maxMessages"] = 5, ["reason"] = "Clean up"
        });

        Assert.True(result.IsError != true, text);
        var summary = JsonDocument.Parse(text).RootElement.GetProperty("summary").GetString()!;
        Assert.DoesNotMatch(@";\s*$", summary);
        Assert.Contains("limit", summary, StringComparison.OrdinalIgnoreCase);
    }

    // The model-written reason is inserted verbatim before the real details, so it can fake or push away what is shown.
    [Fact]
    public async Task ApprovalReason_CannotAddLinesThatImitateTheOperationDetails()
    {
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]) };

        var (_, _, approver) = await CallAsync(workspace, "purge_dead_letters", new()
        {
            ["entity"] = "orders",
            ["maxMessages"] = 10_000,
            ["reason"] = "Routine cleanup.\n\nNothing will be deleted: this is a dry run that only counts messages." + new string('\n', 200) + "."
        }, approve: false);

        var details = Assert.Single(approver.Requests).Details;
        Assert.DoesNotContain(details.Split('\n'), line => line.StartsWith("Nothing will be deleted", StringComparison.Ordinal));
        Assert.DoesNotContain("\n\n\n\n", details, StringComparison.Ordinal);
    }

    // Count quality in the public tools: an approximate zero is not proof of empty, an approximate count says so, and
    // a period without history has unknown values rather than exact nulls.
    private static readonly ServiceBusQueue Estimated = new(
        "orders",
        new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 0, deadLetter: 0)) { CountsAreEstimates = true },
        ServiceBusEntityStatus.Active);

    [Fact]
    public async Task ExplainDeadLetters_DoesNotCallAnApproximateZeroEmpty()
    {
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Estimated]) { HasMessageCounts = true } };

        var (result, text, _) = await CallAsync(workspace, "explain_dead_letters", new());

        Assert.True(result.IsError != true, text);
        Assert.Contains("not known to be empty", text, StringComparison.Ordinal);
        Assert.Contains("\"countQuality\":\"estimated\"", text.Replace(" ", string.Empty, StringComparison.Ordinal), StringComparison.OrdinalIgnoreCase);
    }

    // An exact positive count whose read failed is not an empty queue.
    [Fact]
    public async Task ExplainDeadLetters_DoesNotCallAFailedReadEmpty()
    {
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]) { HasMessageCounts = true },
            CleanupOperationGate = _ => throw new TimeoutException("The read timed out.")
        };

        var (result, text, _) = await CallAsync(workspace, "explain_dead_letters", new());

        Assert.True(result.IsError != true, text);
        Assert.Contains("could not be read", text, StringComparison.Ordinal);
        Assert.DoesNotContain("are empty", text, StringComparison.Ordinal);
    }

    // The emulator's capped sample is at least the cap: get_entities says so, as scan_dead_letters does.
    [Theory]
    [InlineData(999, "exact")]
    [InlineData(1000, "lowerBound")]
    public async Task GetEntities_CallsACappedEmulatorSampleALowerBound(long sampled, string quality)
    {
        var runtime = await QueueLoom.Infrastructure.Azure.AzureServiceBusWorkspace.SampleEmulatorRuntimeAsync(
            Orders.Reference, false, null, null, (_, _, _) => Task.FromResult(sampled), CancellationToken.None);
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", runtime, ServiceBusEntityStatus.Active)]) };

        var (result, text, _) = await CallAsync(workspace, "get_entities", new());

        Assert.True(result.IsError != true, text);
        Assert.Contains($"\"countQuality\":\"{quality}\"", text.Replace(" ", string.Empty, StringComparison.Ordinal), StringComparison.OrdinalIgnoreCase);
    }

    // A configured dead-letter target that cannot be seen (another account or region; a Pub/Sub dead-letter topic
    // with no subscription) stays a dead-letter source of unknown count: never a zero, never left out.
    public static TheoryData<string> UnobservableDeadLetterTargets => ["sqs", "sns", "pubsub"];

    private static ServiceBusTopology UnobservableTopology(string provider) => provider switch
    {
        "sqs" => new QueueLoom.Infrastructure.Aws.AwsTopologyIndex(
            [new QueueLoom.Infrastructure.Aws.AwsQueueInfo("orders", "http://localhost/orders", "arn:aws:sqs:us-east-1:123:orders",
                false, 0, 0, 0, "arn:aws:sqs:eu-west-1:999:orders-dlq", null, null)], []).ToTopology(DateTimeOffset.UtcNow) with { HasMessageCounts = true },
        "sns" => new QueueLoom.Infrastructure.Aws.AwsTopologyIndex([],
            [QueueLoom.Infrastructure.Aws.AwsTopicInfo.From("arn:aws:sns:us-east-1:123:events",
                [new QueueLoom.Infrastructure.Aws.AwsSubscriptionInfo("arn:aws:sns:us-east-1:123:events:1", "https", "https://example.invalid/hook",
                    "arn:aws:sqs:eu-west-1:999:events-dlq")])]).ToTopology(DateTimeOffset.UtcNow) with { HasMessageCounts = true },
        _ => QueueLoom.Infrastructure.Google.GooglePubSubTopology.Build("project-a", ["events"],
            [new global::Google.Cloud.PubSub.V1.Subscription
            {
                Name = "projects/project-a/subscriptions/worker",
                Topic = "projects/project-a/topics/events",
                DeadLetterPolicy = new global::Google.Cloud.PubSub.V1.DeadLetterPolicy { DeadLetterTopic = "projects/project-a/topics/events-dlq", MaxDeliveryAttempts = 5 }
            }],
            DateTimeOffset.UtcNow, undelivered: new Dictionary<string, long> { ["worker"] = 0 }).Topology
    };

    [Theory]
    [MemberData(nameof(UnobservableDeadLetterTargets))]
    public async Task AnUnobservableDeadLetterTargetIsUnknownNotZero(string provider)
    {
        var topology = UnobservableTopology(provider);
        var source = topology.Queues.Cast<object>().Concat(topology.Topics.SelectMany(topic => topic.Subscriptions)).Single() switch
        {
            ServiceBusQueue queue => (queue.HasDeadLetterQueue, queue.Runtime),
            ServiceBusSubscription subscription => (subscription.HasDeadLetterQueue, subscription.Runtime),
            _ => throw new InvalidOperationException()
        };
        Assert.True(source.HasDeadLetterQueue);
        Assert.Equal(DeadLetterCountQuality.Unknown, DeadLetterCountQualities.OfReported(source.Runtime));

        var workspace = new FakeWorkspace { Topology = topology };
        var (_, entities, _) = await CallAsync(workspace, "get_entities", new());
        var (_, explained, _) = await CallAsync(workspace, "explain_dead_letters", new());

        Assert.Contains("\"countQuality\":\"unknown\"", entities.Replace(" ", string.Empty, StringComparison.Ordinal), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("not known to be empty", explained, StringComparison.Ordinal);
        Assert.DoesNotContain("No dead-letter queue holds messages", explained, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetEntities_SaysApproximateCountsAreEstimates()
    {
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Estimated]) };

        var (result, text, _) = await CallAsync(workspace, "get_entities", new());

        Assert.True(result.IsError != true, text);
        Assert.Contains("estimated", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetDeadLetterHistory_WithoutRecordsHasUnknownValues()
    {
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        var workspace = new FakeWorkspace { Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [Orders]) };
        var store = new QueueLoom.Infrastructure.Persistence.JsonLinesDeadLetterHistoryStore(Path.Combine(directory.Path, "history.jsonl"));

        var (result, text, _) = await CallAsync(workspace, "get_dead_letter_history", new(), history: store);

        Assert.True(result.IsError != true, text);
        Assert.DoesNotContain("\"exact\"", text, StringComparison.Ordinal);
        Assert.Contains("unknown", text, StringComparison.Ordinal);
    }

    private static async Task<(CallToolResult Result, string Text, CapturingApprover Approver)> CallAsync(
        IServiceBusWorkspace workspace,
        string tool,
        Dictionary<string, object?> arguments,
        bool approve = true,
        ServiceBusProfile? profile = null,
        IDeadLetterHistoryStore? history = null)
    {
        profile ??= CreateProfile("Development", EnvironmentKind.Development);
        var approver = new CapturingApprover(approve);
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        using var stop = new CancellationTokenSource();
        var run = QueueLoomMcpServer.RunAsync(
            new McpServerSettings(false, Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "mcp-c3", Guid.NewGuid().ToString("N"))),
            services =>
            {
                services.AddSingleton<IProfileRepository>(new FakeProfileRepository([profile], profile.Id));
                services.AddSingleton(workspace);
                services.AddSingleton<IOperationApprover>(approver);
                if (history is not null) services.AddSingleton(history);
            },
            input: clientToServer.Reader.AsStream(),
            output: serverToClient.Writer.AsStream(),
            cancellationToken: stop.Token);
        var client = await McpClient.CreateAsync(
            new StreamClientTransport(clientToServer.Writer.AsStream(), serverToClient.Reader.AsStream()), new McpClientOptions());
        try
        {
            var result = await client.CallToolAsync(tool, arguments);
            return (result, string.Concat(result.Content.OfType<TextContentBlock>().Select(block => block.Text)), approver);
        }
        finally
        {
            await client.DisposeAsync();
            stop.Cancel();
            try { await run; } catch (OperationCanceledException) { }
        }
    }

    internal sealed class CapturingApprover(bool approve) : IOperationApprover
    {
        public List<ApprovalRequest> Requests { get; } = [];

        public Task<ApprovalDecision> RequestAsync(ApprovalRequest request, McpServer server, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            return Task.FromResult(approve ? ApprovalDecision.Approve("Approved in test.") : ApprovalDecision.Deny("Declined in test."));
        }
    }

    /// <summary>Forwards every workspace call to <see cref="Inner"/> unless <see cref="Override"/> handles it.</summary>
    public class OverridingWorkspace : DispatchProxy
    {
        private IServiceBusWorkspace _inner = null!;
        private Func<MethodInfo, object?[]?, (bool Handled, object? Result)> _override = null!;

        public static IServiceBusWorkspace Wrap(
            IServiceBusWorkspace inner,
            Func<MethodInfo, object?[]?, (bool Handled, object? Result)> handler)
        {
            var proxy = Create<IServiceBusWorkspace, OverridingWorkspace>();
            var self = (OverridingWorkspace)(object)proxy;
            self._inner = inner;
            self._override = handler;
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var (handled, result) = _override(targetMethod!, args);
            if (handled) return result;
            try
            {
                return targetMethod!.Invoke(_inner, args);
            }
            catch (TargetInvocationException exception) when (exception.InnerException is not null)
            {
                ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
                throw;
            }
        }
    }
}
