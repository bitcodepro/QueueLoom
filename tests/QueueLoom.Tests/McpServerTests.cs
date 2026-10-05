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
            ["check_topic_routing", "delete_dead_letter_messages", "explain_dead_letters", "export_messages", "get_dead_letter_history", "get_entities",
             "list_environments", "peek_messages", "purge_dead_letters", "resend_dead_letters", "scan_dead_letters", "search_dead_letters", "send_message",
             "trace_forwarding"],
            tools.Select(tool => tool.Name).Order());
        foreach (var name in new[] { "list_environments", "get_entities", "scan_dead_letters", "peek_messages", "search_dead_letters", "export_messages",
                     "get_dead_letter_history", "check_topic_routing", "explain_dead_letters", "trace_forwarding" })
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

    [Fact]
    public async Task DeadLetterHistory_LeavesOutScansWithUnreadableQueues()
    {
        await using var server = await McpTestServer.StartAsync();
        var profile = (await server.Profiles.ListAsync())[0];
        server.Workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow,
        [
            new DeadLetterEntitySnapshot(Orders.Reference, 2),
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("payments"), null, error: "Unauthorized")
        ]);

        await server.CallAsync("scan_dead_letters");
        var history = await server.CallAsync("get_dead_letter_history", new() { ["hours"] = 6 });

        Assert.Equal(0, history.GetProperty("sampleCount").GetInt32());
    }

    [Fact]
    public async Task Environments_ExactNameWinsAndSameNamesAreNeverGuessed()
    {
        await using var server = await McpTestServer.StartAsync();
        var upper = CreateProfile("DEVELOPMENT", EnvironmentKind.Test);
        await server.Profiles.UpsertAsync(upper);
        server.Workspace.Snapshots[upper.Id] = new DeadLetterSnapshot(upper.Id, DateTimeOffset.UtcNow, []);

        var scan = await server.CallAsync("scan_dead_letters", new() { ["environment"] = "DEVELOPMENT" });
        Assert.Equal("DEVELOPMENT", scan.GetProperty("environment").GetString());

        await server.Profiles.UpsertAsync(CreateProfile("DEVELOPMENT", EnvironmentKind.Test));
        Assert.Contains("pass the id", await server.CallForErrorAsync("scan_dead_letters", new() { ["environment"] = "DEVELOPMENT" }),
            StringComparison.Ordinal);
        Assert.Equal("DEVELOPMENT", (await server.CallAsync("scan_dead_letters", new() { ["environment"] = upper.Id.ToString() }))
            .GetProperty("environment").GetString());
    }

    [Fact]
    public async Task TopicRouting_ExplainsWhichSubscriptionsReceiveAMessage()
    {
        await using var server = await McpTestServer.StartAsync();
        server.Workspace.SupportsSubscriptionRules = true;
        server.Workspace.TopicRules["orders"] =
        [
            new QueueLoom.Core.Routing.SubscriptionRules("billing",
                [new QueueLoom.Core.Routing.SubscriptionRule("eu", QueueLoom.Core.Routing.RuleFilterKind.Sql, "region = 'EU' AND amount > 100")]),
            new QueueLoom.Core.Routing.SubscriptionRules("legacy", [])
        ];

        var rulesOnly = await server.CallAsync("check_topic_routing", new() { ["topic"] = "orders" });
        Assert.False(rulesOnly.TryGetProperty("headline", out _));
        Assert.Equal("region = 'EU' AND amount > 100", rulesOnly.GetProperty("subscriptions")[0].GetProperty("rules")[0].GetProperty("filter").GetString());

        var routed = await server.CallAsync("check_topic_routing", new()
        {
            ["topic"] = "orders",
            ["properties"] = new Dictionary<string, object> { ["region"] = "EU", ["amount"] = 250 }
        });
        Assert.Equal("1 of 2 subscriptions receive it.", routed.GetProperty("headline").GetString());
        Assert.Equal("Receives", routed.GetProperty("subscriptions")[0].GetProperty("outcome").GetString());
        Assert.Equal("Has no rules, so it receives no messages at all.", routed.GetProperty("subscriptions")[1].GetProperty("warning").GetString());
    }

    [Fact]
    public async Task TopicRouting_KeepsASameNamedQueueAndExchangeApart()
    {
        await using var server = await McpTestServer.StartAsync();
        server.Workspace.SupportsSubscriptionRules = true;
        server.Workspace.RoutingService = QueueLoom.Core.Routing.RoutingService.RabbitMq;
        server.Workspace.TopicRules["source"] =
        [
            new QueueLoom.Core.Routing.SubscriptionRules("dest",
                [new QueueLoom.Core.Routing.SubscriptionRule("q", QueueLoom.Core.Routing.RuleFilterKind.DirectBinding) { Expression = "q", Title = "'q'" }])
                { Service = QueueLoom.Core.Routing.RoutingService.RabbitMq },
            new QueueLoom.Core.Routing.SubscriptionRules("dest",
                [new QueueLoom.Core.Routing.SubscriptionRule("e", QueueLoom.Core.Routing.RuleFilterKind.DirectBinding) { Expression = "e", Title = "'e'" }])
                { Service = QueueLoom.Core.Routing.RoutingService.RabbitMq, IsExchange = true }
        ];

        var routed = await server.CallAsync("check_topic_routing", new() { ["topic"] = "source", ["subject"] = "q" });

        var rows = routed.GetProperty("subscriptions");
        Assert.Equal("Receives", rows[0].GetProperty("outcome").GetString());
        Assert.False(rows[0].GetProperty("isExchange").GetBoolean());
        Assert.Equal("Skips", rows[1].GetProperty("outcome").GetString());
        Assert.True(rows[1].GetProperty("isExchange").GetBoolean());
        Assert.Contains("is not 'e'", rows[1].GetProperty("explanation").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("en-US", "66.7")]
    [InlineData("uk-UA", "66,7")]
    public async Task ExplainDeadLetters_GroupsTheDeadLettersByCause(string cultureName, string formattedShare)
    {
        using var culture = new TestCulture(cultureName);
        await using var server = await McpTestServer.StartAsync();
        BrowsedMessage Dead(long number, string reason, string description) => new(
            Orders.Reference, ServiceBusSubQueue.DeadLetter, number, System.Text.Encoding.UTF8.GetBytes("{}"),
            new EditableMessageProperties(MessageId: $"m-{number}"), enqueuedAt: DateTimeOffset.Parse("2026-08-12T10:00:00Z").AddMinutes(number),
            deadLetterReason: reason, deadLetterErrorDescription: description, deliveryCount: (int)number);
        server.Workspace.BrowseMessages =
        [
            Dead(1, "MaxDeliveryCountExceeded", "Order 17 was not found"),
            Dead(2, "MaxDeliveryCountExceeded", "Order 99 was not found"),
            Dead(3, "TTLExpiredException", "Expired")
        ];

        var explained = await server.CallAsync("explain_dead_letters");

        Assert.Equal(3, explained.GetProperty("readMessages").GetInt32());
        var top = explained.GetProperty("causes")[0];
        Assert.Equal("MaxDeliveryCountExceeded", top.GetProperty("reason").GetString());
        Assert.Equal("Order {n} was not found", top.GetProperty("pattern").GetString());
        Assert.Equal(2, top.GetProperty("count").GetInt32());
        Assert.Equal(66.7, top.GetProperty("share").GetDouble());
        Assert.Equal(2, top.GetProperty("sources").GetProperty("orders").GetInt32());
        Assert.Equal(["m-1", "m-2"], top.GetProperty("sampleMessageIds").EnumerateArray().Select(id => id.GetString()));
        Assert.Contains("never completed it", top.GetProperty("hint").GetString(), StringComparison.Ordinal);
        Assert.StartsWith("3 dead letter(s) read from 1 queue(s) fall into 2 cause(s); the largest is MaxDeliveryCountExceeded: Order {n} was not found (" + formattedShare + "%)",
            explained.GetProperty("summary").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TraceForwarding_FollowsTheChainAndFindsLoops()
    {
        await using var server = await McpTestServer.StartAsync();
        server.Workspace.Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
        [
            Orders with { ForwardTo = "archive" },
            new ServiceBusQueue("archive", ServiceBusEntityRuntime.Empty, ServiceBusEntityStatus.Active),
            new ServiceBusQueue("ping", ServiceBusEntityRuntime.Empty, ServiceBusEntityStatus.Active) { ForwardTo = "pong" },
            new ServiceBusQueue("pong", ServiceBusEntityRuntime.Empty, ServiceBusEntityStatus.Active) { ForwardTo = "ping" }
        ]);

        var chain = await server.CallAsync("trace_forwarding", new() { ["entity"] = "orders" });
        Assert.Equal(["archive"], chain.GetProperty("destinations").EnumerateArray().Select(item => item.GetString()));
        Assert.Equal("orders → archive", chain.GetProperty("paths")[0].GetString());

        var loop = await server.CallAsync("trace_forwarding", new() { ["entity"] = "ping" });
        Assert.Equal("Loop: ping → pong → ping. Service Bus dead-letters these messages after 4 forwards.", loop.GetProperty("summary").GetString());
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
        Assert.Equal(10, tools.Count);
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

    // Bug 3. Two different deliveries share a Message ID (so, on RabbitMQ, SQS and Pub/Sub, one derived sequence
    // number). A pick is bound to the content listed through its fingerprint: choosing B sends B and never A, listing
    // both sends each once, and a pick without its fingerprint is refused with nothing sent.
    private static BrowsedMessage Delivery(string? id, string body, string header) => new(
        Orders.Reference, ServiceBusSubQueue.DeadLetter, 77, System.Text.Encoding.UTF8.GetBytes(body),
        new EditableMessageProperties(MessageId: id),
        [new MessageApplicationProperty("tenant", ApplicationPropertyType.String, header)]) { HasSequenceNumber = false };

    private static object Pick(BrowsedMessage message, bool withFingerprint = true) => new
    {
        entity = "orders", subQueue = "dlq", sequenceNumber = 77L, messageId = message.Properties.MessageId,
        fingerprint = withFingerprint ? MessageFingerprint.Of(message) : null
    };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ResendCopy_SendsExactlyTheChosenOneOfMessagesSharingAKey(bool withoutId)
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        var id = withoutId ? null : "repeated-id";
        var a = withoutId ? Delivery(id, "same", "A") : Delivery(id, "A", "x");
        var b = withoutId ? Delivery(id, "same", "B") : Delivery(id, "B", "x");
        server.Workspace.BrowseMessages = [a, b];

        await server.CallAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { Pick(b) }, ["mode"] = "copy", ["reason"] = "retry after the fix"
        });

        var sent = Assert.Single(server.Workspace.SentMessages);
        Assert.Equal(b.Body.ToArray(), System.Text.Encoding.UTF8.GetBytes(sent.Message.Body.Content));
        Assert.Equal(b.ApplicationProperties[0].Value, Assert.Single(sent.Message.ApplicationProperties).Value);
    }

    [Fact]
    public async Task ResendCopy_ListingBothSendsEachOnce()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        var a = Delivery("repeated-id", "A", "x");
        var b = Delivery("repeated-id", "B", "x");
        server.Workspace.BrowseMessages = [a, b];

        var error = await server.CallForErrorAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { Pick(a), Pick(b) }, ["mode"] = "copy", ["reason"] = "retry after the fix"
        });

        // Both share one key; the tool lists each message once, so two fingerprints for one key are refused.
        Assert.Contains("different fingerprints", error, StringComparison.Ordinal);
        Assert.Empty(server.Workspace.SentMessages);
    }

    // The chosen B is gone (or held by another consumer) when the tool reads again; only A, with the same key, is
    // there. Nothing is approved or sent.
    [Fact]
    public async Task ResendCopy_NeverSendsAnotherMessageWhenTheChosenOneIsGone()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        var a = Delivery("repeated-id", "A", "x");
        var b = Delivery("repeated-id", "B", "x");
        server.Workspace.BrowseMessages = [a];

        var error = await server.CallForErrorAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { Pick(b) }, ["mode"] = "copy", ["reason"] = "retry after the fix"
        });

        Assert.Contains("None of the listed messages", error, StringComparison.Ordinal);
        Assert.Empty(server.Workspace.SentMessages);
        Assert.Empty(server.Approver.Requests);
    }

    [Fact]
    public async Task ResendCopy_RefusesAPickWithoutItsFingerprint()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        var a = Delivery("repeated-id", "A", "x");
        server.Workspace.BrowseMessages = [a, Delivery("repeated-id", "B", "x")];

        var error = await server.CallForErrorAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { Pick(a, withFingerprint: false) }, ["mode"] = "copy", ["reason"] = "retry after the fix"
        });

        Assert.Contains("Nothing was sent", error, StringComparison.Ordinal);
        Assert.Empty(server.Workspace.SentMessages);
        Assert.Empty(server.Approver.Requests);
    }

    // Review: on RabbitMQ 4.2 classic queues a producer may set x-acquired-count itself. Two messages that differ only
    // in that header: choosing B sends B, with B's value.
    // ...and when the chosen B is gone and only A (counter 1) remains, A is not sent in its place.
    [Fact]
    public async Task ResendCopy_DoesNotSubstituteAProducerCounterTwin()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        BrowsedMessage Counted(string count) => new(Orders.Reference, ServiceBusSubQueue.DeadLetter, 77, "same"u8.ToArray(),
            new EditableMessageProperties(MessageId: "repeated-id"),
            [new MessageApplicationProperty("x-acquired-count", ApplicationPropertyType.Int64, count)]) { HasSequenceNumber = false };
        var b = Counted("2");
        server.Workspace.BrowseMessages = [Counted("1")];

        var error = await server.CallForErrorAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { Pick(b) }, ["mode"] = "copy", ["reason"] = "retry after the fix"
        });

        Assert.Contains("None of the listed messages", error, StringComparison.Ordinal);
        Assert.Empty(server.Workspace.SentMessages);
        Assert.Empty(server.Approver.Requests);
    }

    // The MCP path with the real classifier: B was chosen and is gone; only its counter twin A is there. On classic
    // queues (4.2 and 4.3) the counter is the producer's, so A is not sent. On a quorum queue the broker raised the
    // counter of the same message, which is found and sent.
    [Theory]
    [InlineData(false, "4.2.0", false)]
    [InlineData(false, "4.3.0", false)]
    [InlineData(true, "4.2.0", true)]
    [InlineData(true, "4.3.0", true)]
    public async Task ResendCopy_FollowsTheQueueTypeForCounterHeaders(bool quorum, string version, bool sent)
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        var owned = QueueLoom.Infrastructure.RabbitMq.RabbitMqWorkspace.BrokerOwnedHeaders(quorum, Version.Parse(version));
        var header = quorum ? "x-delivery-count" : "x-acquired-count";
        BrowsedMessage Counted(string count) => new BrowsedMessage(Orders.Reference, ServiceBusSubQueue.DeadLetter, 77, "same"u8.ToArray(),
            new EditableMessageProperties(MessageId: "repeated-id"),
            [new MessageApplicationProperty(header, ApplicationPropertyType.Int64, count)]) { HasSequenceNumber = false } with { BrokerOwnedHeaders = owned };
        var chosen = Counted("2");
        server.Workspace.BrowseMessages = [Counted("1")];

        if (sent)
        {
            await server.CallAsync("resend_dead_letters", new()
            {
                ["messages"] = new[] { Pick(chosen) }, ["mode"] = "copy", ["reason"] = "retry after the fix"
            });
            Assert.Single(server.Workspace.SentMessages);
        }
        else
        {
            var error = await server.CallForErrorAsync("resend_dead_letters", new()
            {
                ["messages"] = new[] { Pick(chosen) }, ["mode"] = "copy", ["reason"] = "retry after the fix"
            });
            Assert.Contains("None of the listed messages", error, StringComparison.Ordinal);
            Assert.Empty(server.Workspace.SentMessages);
        }
    }

    [Fact]
    public async Task ResendCopy_KeepsAProducerSetCounterHeaderApart()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        BrowsedMessage Counted(string count) => new(Orders.Reference, ServiceBusSubQueue.DeadLetter, 77, "same"u8.ToArray(),
            new EditableMessageProperties(MessageId: "repeated-id"),
            [new MessageApplicationProperty("x-acquired-count", ApplicationPropertyType.Int64, count)]) { HasSequenceNumber = false };
        var a = Counted("1");
        var b = Counted("2");
        server.Workspace.BrowseMessages = [a, b];

        await server.CallAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { Pick(b) }, ["mode"] = "copy", ["reason"] = "retry after the fix"
        });

        Assert.Equal("2", Assert.Single(Assert.Single(server.Workspace.SentMessages).Message.ApplicationProperties).Value);
    }

    // Exact duplicates (same body and metadata) are interchangeable: copying one of them is still allowed.
    [Fact]
    public async Task ResendCopy_AllowsExactDuplicatesOfTheChosenMessage()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        var duplicate = Delivery("repeated-id", "same", "x");
        server.Workspace.BrowseMessages = [duplicate, Delivery("repeated-id", "same", "x")];

        await server.CallAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { Pick(duplicate) }, ["mode"] = "copy", ["reason"] = "retry after the fix"
        });

        Assert.Equal("same", Assert.Single(server.Workspace.SentMessages).Message.Body.Content);
    }

    // Peek returns the fingerprint for messages of services without real sequence numbers.
    [Fact]
    public async Task Peek_ReturnsTheFingerprintWhereSequenceNumbersAreDerived()
    {
        await using var server = await McpTestServer.StartAsync();
        var b = Delivery("repeated-id", "B", "x");
        server.Workspace.BrowseMessages = [b];

        var peeked = await server.CallAsync("peek_messages", new() { ["entity"] = "orders", ["subQueue"] = "dlq" });

        Assert.Equal(MessageFingerprint.Of(b), peeked.GetProperty("messages")[0].GetProperty("fingerprint").GetString());
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
        Assert.False(string.IsNullOrWhiteSpace(sent.Message.Properties.MessageId));
        Assert.Contains("distinct new Message IDs", request.Details, StringComparison.Ordinal);
        Assert.Equal([3L], Assert.Single(server.Workspace.DeleteRequests).Messages.Select(message => message.SequenceNumber));
        Assert.Equal([ProfileAccessMode.ReadWrite, ProfileAccessMode.ReadOnly], server.Workspace.AccessModeChanges);
    }

    [Fact]
    public async Task SendRejectsConfigurationChangedDuringApproval()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        var profile = Assert.Single(await server.Profiles.ListAsync());
        server.Approver.OnRequest = () => server.Profiles.UpsertAsync(profile with { ConfigurationRevision = Guid.NewGuid() }).GetAwaiter().GetResult();
        var error = await server.CallForErrorAsync("send_message", new() { ["destination"] = "orders", ["body"] = "{}", ["reason"] = "test" });
        Assert.Contains("configuration changed", error, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(server.Workspace.SentMessages);
        Assert.DoesNotContain(ProfileAccessMode.ReadWrite, server.Workspace.AccessModeChanges);
    }

    [Theory]
    [InlineData(MessagingProvider.AzureServiceBus)]
    [InlineData(MessagingProvider.AmazonSqsSns)]
    public async Task DeduplicatingProviderMcpMoveCannotPreserveIdsButCopyWarns(MessagingProvider provider)
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        var profile = Assert.Single(await server.Profiles.ListAsync());
        await server.Profiles.UpsertAsync(profile with { Provider = provider });
        var args = new Dictionary<string, object?>
        {
            ["messages"] = new[] { new { entity = "orders", subQueue = "dlq", sequenceNumber = 3L, messageId = (string?)null } },
            ["mode"] = "move", ["reason"] = "test", ["preserveMessageIds"] = true
        };
        Assert.Contains("distinct new Message IDs", await server.CallForErrorAsync("resend_dead_letters", args), StringComparison.Ordinal);
        Assert.Empty(server.Workspace.SentMessages);
        Assert.Empty(server.Workspace.DeleteRequests);
        Assert.Empty(server.Approver.Requests);
        args["mode"] = "copy";
        await server.CallAsync("resend_dead_letters", args);
        Assert.Contains("suppress delivery", Assert.Single(server.Approver.Requests).Details, StringComparison.Ordinal);
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

    // Bug 4 / review: the export's reserved name is claimed private, so the completed export (JSON and CSV) is
    // owner-only even in a folder others can read, and whatever the umask.
    [Theory]
    [InlineData("json")]
    [InlineData("csv")]
    public async Task Export_ThroughMcpIsPrivate(string format)
    {
        if (OperatingSystem.IsWindows()) return;
        await using var server = await McpTestServer.StartAsync();
        Directory.CreateDirectory(server.ExportDirectory);
#pragma warning disable CA1416 // not Windows: returned above
        File.SetUnixFileMode(server.ExportDirectory, (UnixFileMode)0b111_101_101);

        var result = await server.CallAsync("export_messages", new() { ["entity"] = "orders", ["format"] = format });

        var path = result.GetProperty("path").GetString()!;
        Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(path));
#pragma warning restore CA1416
        Assert.Equal(2, result.GetProperty("count").GetInt32());
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
        public Action? OnRequest { get; set; }

        public Task<ApprovalDecision> RequestAsync(ApprovalRequest request, McpServer server, CancellationToken cancellationToken)
        {
            Requests.Add(request);
            OnRequest?.Invoke();
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
        public required FakeProfileRepository Profiles { get; init; }
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
            var profiles = new FakeProfileRepository([profile], profile.Id);
            var history = new MemoryHistoryStore();

            var clientToServer = new Pipe();
            var serverToClient = new Pipe();
            var stop = new CancellationTokenSource();
            var exportDirectory = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "mcp-exports", Guid.NewGuid().ToString("N"));
            var run = QueueLoomMcpServer.RunAsync(
                new McpServerSettings(readOnly, exportDirectory),
                services =>
                {
                    services.AddSingleton<IProfileRepository>(profiles);
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
                Profiles = profiles,
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
