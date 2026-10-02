using System.Net.Http.Headers;
using System.Text;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using RabbitMQ.Client;

namespace QueueLoom.IntegrationTests;

/// <summary>
/// Runs the RabbitMQ workspace against a real broker (rabbitmq:4-management) in a virtual host of its own:
/// "orders" and "payments" dead-letter through the direct exchange "dlx" into the shared queue "dead-letters";
/// "invoices" is a quorum queue with a delivery limit that dead-letters into "invoices.dlq" by the default exchange.
/// </summary>
public sealed class RabbitMqWorkspaceTests : IAsyncLifetime
{
    private readonly TemporaryDirectory _directory = new();
    private readonly InMemorySecretVault _vault = new();
    private readonly string _vhost = Emulators.Unique("queueloom");
    private HttpClient _management = null!;
    private IConnection _connection = null!;
    private IChannel _setup = null!;
    private RabbitMqWorkspace _workspace = null!;

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.RabbitMq)))
        {
            return;
        }

        _management = new HttpClient(new HttpClientHandler { UseProxy = false })
        {
            BaseAddress = new Uri($"http://{Emulators.RabbitMqHost}:{Emulators.RabbitMqPort + 10000}/")
        };
        _management.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String("guest:guest"u8.ToArray()));
        (await _management.PutAsync($"api/vhosts/{_vhost}", null)).EnsureSuccessStatusCode();
        (await _management.PutAsync($"api/permissions/{_vhost}/guest",
            new StringContent("""{"configure":".*","write":".*","read":".*"}""", Encoding.UTF8, "application/json"))).EnsureSuccessStatusCode();

        _connection = await new ConnectionFactory
        {
            HostName = Emulators.RabbitMqHost, Port = Emulators.RabbitMqPort, VirtualHost = _vhost
        }.CreateConnectionAsync();
        _setup = await _connection.CreateChannelAsync();
        await _setup.ExchangeDeclareAsync("dlx", ExchangeType.Direct, durable: true);
        await _setup.ExchangeDeclareAsync("events", ExchangeType.Topic, durable: true);
        await _setup.QueueDeclareAsync("dead-letters", durable: true, exclusive: false, autoDelete: false);
        await _setup.QueueBindAsync("dead-letters", "dlx", "failed");
        foreach (var queue in new[] { "orders", "payments" })
        {
            await _setup.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = "dlx",
                ["x-dead-letter-routing-key"] = "failed"
            });
        }
        await _setup.QueueBindAsync("orders", "events", "order.*");
        await _setup.QueueDeclareAsync("invoices.dlq", durable: true, exclusive: false, autoDelete: false);
        await _setup.QueueDeclareAsync("invoices", durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum",
            ["x-delivery-limit"] = 2,
            ["x-dead-letter-exchange"] = string.Empty,
            ["x-dead-letter-routing-key"] = "invoices.dlq"
        });

        var profile = ServiceBusProfile.CreateNew("RabbitMQ", EnvironmentKind.Development,
                new AuthenticationSettings(AuthenticationKind.RabbitMqPassword), accessMode: ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.RabbitMq,
            RabbitMq = new RabbitMqSettings(Emulators.RabbitMqHost, "guest", _vhost, Emulators.RabbitMqPort, Emulators.RabbitMqPort + 10000),
            AllowQueueManagement = true
        };
        await _vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), "guest");
        _workspace = new RabbitMqWorkspace(_vault, backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)),
            httpHandler: new HttpClientHandler { UseProxy = false });
        await _workspace.ConnectAsync(profile);
    }

    public async ValueTask DisposeAsync()
    {
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        if (_connection is not null)
        {
            await _setup.CloseAsync();
            await _connection.CloseAsync();
            await _management.DeleteAsync($"api/vhosts/{_vhost}");
            _management.Dispose();
        }
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task Topology_follows_dead_letter_exchanges_and_marks_the_shared_queue()
    {
        await PublishAsync("orders", "o-1");
        await RejectAsync("orders", 1);

        var topology = await WaitForAsync(t => t.Queues.Single(queue => queue.Name == "orders").Runtime.MessageCounts.DeadLetter == 1);

        var orders = topology.Queues.Single(queue => queue.Name == "orders");
        Assert.True(orders.HasDeadLetterQueue);
        Assert.Contains("Shares dead-letter queue dead-letters", orders.Note, StringComparison.Ordinal);
        Assert.Equal("Dead-letter queue of orders, payments", topology.Queues.Single(queue => queue.Name == "dead-letters").Note);
        Assert.Contains("Quorum queue, delivery limit 2", topology.Queues.Single(queue => queue.Name == "invoices").Note, StringComparison.Ordinal);
        Assert.False(topology.Queues.Single(queue => queue.Name == "dead-letters").HasDeadLetterQueue);
        Assert.Equal(["dlx", "events"], topology.Topics.Select(topic => topic.Name));
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public Task Purge_with_batch_size_one_deletes_both_messages_without_application_ids() => PurgeRepeatedIdsAsync(null);

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task CycleTwoRabbit_CollidingHeadersSurviveBrokerCopyAndBackedUpPurge()
    {
        await _setup.BasicPublishAsync(string.Empty, "orders", mandatory: false,
            new BasicProperties { MessageId = "metadata-fixture", Type = "basic-type", AppId = "basic-app",
                Headers = new Dictionary<string, object?> { ["amqp-type"] = "header-type"u8.ToArray(), ["amqp-app-id"] = "header-app"u8.ToArray() } }, "body"u8.ToArray());
        await RejectAsync("orders", 1);
        await WaitForAsync(t => t.Queues.Single(q => q.Name == "orders").Runtime.MessageCounts.DeadLetter == 1);
        var source = ServiceBusEntityReference.Queue("orders");
        var message = Assert.Single(await _workspace.BrowseMessagesAsync(new(source, ServiceBusSubQueue.DeadLetter)));
        await CopyAndInspect(message);
        Assert.Equal(1u, (await _setup.QueueDeclarePassiveAsync("dead-letters")).MessageCount);
        var purge = await _workspace.PurgeDeadLettersAsync(new([new DeadLetterPurgeTarget(source, ServiceBusSubQueue.DeadLetter)], maximumMessagesPerSubQueue: 10));
        Assert.Equal(1, purge.DeletedCount);
        var repository = new JsonDeadLetterBackupRepository(QueueLoomPaths.ForRoot(_directory.Path));
        await CopyAndInspect(await repository.LoadAsync(Assert.Single(await repository.ListAsync())));
        Assert.False((await _workspace.GetTopologyAsync(true)).CanDeleteSelectedMessages);

        async Task CopyAndInspect(BrowsedMessage original)
        {
            await _workspace.SendMessageAsync(new(ServiceBusEntityReference.Queue("payments"), original.CreateDraft()));
            var delivered = await _setup.BasicGetAsync("payments", autoAck: true);
            Assert.NotNull(delivered);
            Assert.Equal("basic-type", delivered.BasicProperties.Type);
            Assert.Equal("basic-app", delivered.BasicProperties.AppId);
            Assert.Equal("header-type"u8.ToArray(), Assert.IsType<byte[]>(delivered.BasicProperties.Headers!["amqp-type"]));
            Assert.Equal("header-app"u8.ToArray(), Assert.IsType<byte[]>(delivered.BasicProperties.Headers!["amqp-app-id"]));
            Assert.Equal("body"u8.ToArray(), delivered.Body.ToArray());
        }
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public Task Purge_with_batch_size_one_deletes_both_messages_with_the_same_application_id() => PurgeRepeatedIdsAsync("repeated-fixture-id");

    private async Task PurgeRepeatedIdsAsync(string? id)
    {
        for (var index = 0; index < 2; index++)
            await _setup.BasicPublishAsync(string.Empty, "orders", mandatory: false,
                new BasicProperties { MessageId = id }, Encoding.UTF8.GetBytes($"fixture-{index}"));
        await RejectAsync("orders", 2);
        await WaitForAsync(topology => topology.Queues.Single(queue => queue.Name == "orders").Runtime.MessageCounts.DeadLetter == 2);
        var result = await _workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest(
            [new DeadLetterPurgeTarget(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter)],
            batchSize: 1, maximumMessagesPerSubQueue: 10));
        Assert.Equal(2, result.DeletedCount);
        Assert.False(result.HasFailures);
        Assert.Equal(0u, (await _setup.QueueDeclarePassiveAsync("dead-letters")).MessageCount);
        Assert.Equal(2, (await new JsonDeadLetterBackupRepository(QueueLoomPaths.ForRoot(_directory.Path)).ListAsync()).Count);
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task Dead_letters_of_a_shared_queue_are_read_per_source_and_stay_in_place()
    {
        await PublishAsync("orders", "o-1", "o-2");
        await PublishAsync("payments", "p-1");
        await RejectAsync("orders", 2);
        await RejectAsync("payments", 1);

        var orders = ServiceBusEntityReference.Queue("orders");
        var first = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));
        var second = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));

        Assert.Equal(["o-1", "o-2"], first.Select(message => message.Properties.MessageId).Order());
        Assert.Equal(2, second.Count);
        var message = first[0];
        Assert.Equal("Rejected by a consumer", message.DeadLetterReason);
        Assert.StartsWith("From orders", message.DeadLetterErrorDescription, StringComparison.Ordinal);
        Assert.Equal("orders", message.Properties.Subject);
        Assert.Equal("{\"id\":\"o-1\"}", Encoding.UTF8.GetString(first.Single(item => item.Properties.MessageId == "o-1").Body.Span));
        Assert.Equal(3u, (await _setup.QueueDeclarePassiveAsync("dead-letters")).MessageCount);
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task Reviewed_dead_letters_require_copy_or_a_source_purge()
    {
        await PublishAsync("orders", "o-1", "o-2", "o-3");
        await RejectAsync("orders", 3);
        var orders = ServiceBusEntityReference.Queue("orders");
        var dead = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));

        Assert.False((await _workspace.GetTopologyAsync()).CanDeleteSelectedMessages);
        await Assert.ThrowsAsync<NotSupportedException>(() => _workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest(
            [new DeadLetterMessageKey(orders, ServiceBusSubQueue.DeadLetter, 0, "o-2")])));

        var move = dead.Single(message => message.Properties.MessageId == "o-3");
        var items = new[] { new ResendItem(move, DeadLetterResender.OriginalDestination(orders), move.CreateDraft()) };
        await Assert.ThrowsAsync<NotSupportedException>(() => DeadLetterResender.ResendAsync(_workspace, items, ResendMode.Move));
        var result = await DeadLetterResender.ResendAsync(_workspace, items, ResendMode.Copy);
        Assert.Equal(1, result.SentCount);
        Assert.Equal(0, result.MovedCount);

        var left = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));
        Assert.Equal(["o-1", "o-2", "o-3"], left.Select(message => message.Properties.MessageId).Order(StringComparer.Ordinal));
        var active = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders));
        var resent = Assert.Single(active);
        Assert.Equal("o-3", resent.Properties.MessageId);
        Assert.DoesNotContain(resent.ApplicationProperties, property => property.Name.StartsWith("x-death", StringComparison.Ordinal));
        var purge = await _workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([new DeadLetterPurgeTarget(orders, ServiceBusSubQueue.DeadLetter)]));
        Assert.Equal(3, purge.DeletedCount);
        Assert.Contains(Directory.EnumerateFiles(purge.BackupDirectory, "*.json", SearchOption.AllDirectories),
            file => File.ReadAllText(file).Contains("o-2", StringComparison.Ordinal));
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task Duplicate_application_ids_cannot_authorize_deletion_of_a_body_search_selection()
    {
        foreach (var body in new[] { "unreviewed-A", "review-only-B" })
            await _setup.BasicPublishAsync(string.Empty, "orders", false, new BasicProperties { MessageId = "repeated-id" }, Encoding.UTF8.GetBytes(body));
        await RejectAsync("orders", 2);
        var orders = ServiceBusEntityReference.Queue("orders");
        var found = await _workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest("review-only-B", [new(orders, ServiceBusSubQueue.DeadLetter, 2)]));
        var selected = Assert.Single(found.Matches);
        Assert.Equal("review-only-B", Encoding.UTF8.GetString(selected.Body.Span));
        await Assert.ThrowsAsync<NotSupportedException>(() => _workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest(
            [new(orders, selected.SubQueue, selected.SequenceNumber, selected.Properties.MessageId)])));
        await Assert.ThrowsAsync<NotSupportedException>(() => DeadLetterResender.ResendAsync(_workspace,
            [new(selected, orders, selected.CreateDraft())], ResendMode.Move));
        Assert.Empty(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders)));
        var left = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));
        Assert.Equal(["review-only-B", "unreviewed-A"], left.Select(m => Encoding.UTF8.GetString(m.Body.Span)).Order(StringComparer.Ordinal));
        await Assert.ThrowsAsync<NotSupportedException>(() => _workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest(
            left.Select(m => new DeadLetterMessageKey(orders, m.SubQueue, m.SequenceNumber, m.Properties.MessageId)))));
        Assert.Empty(await new JsonDeadLetterBackupRepository(QueueLoomPaths.ForRoot(_directory.Path)).ListAsync());
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task Reading_a_quorum_queue_does_not_use_up_its_delivery_limit()
    {
        await PublishAsync("invoices", "i-1");
        var invoices = ServiceBusEntityReference.Queue("invoices");

        for (var round = 0; round < 4; round++)
        {
            Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(invoices)));
        }

        // A quorum queue applies a requeue through Raft, so its ready count can trail the nack for a moment.
        var ready = 0u;
        for (var attempt = 0; attempt < 50 && ready == 0; attempt++)
        {
            await Task.Delay(100);
            ready = (await _setup.QueueDeclarePassiveAsync("invoices")).MessageCount;
        }
        Assert.Equal(1u, ready);
        Assert.Equal(0u, (await _setup.QueueDeclarePassiveAsync("invoices.dlq")).MessageCount);
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task Sending_to_an_exchange_uses_the_subject_as_routing_key_and_reports_unroutable_messages()
    {
        var events = ServiceBusEntityReference.Topic("events");
        MessageDraft Draft(string subject) => new(new EditableMessageBody("{}", MessageBodyFormat.Json),
            new EditableMessageProperties(MessageId: Guid.NewGuid().ToString("N"), Subject: subject, ContentType: "application/json"),
            [new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "acme")]);

        await _workspace.SendMessageAsync(new SendMessageRequest(events, Draft("order.created")));
        var error = await Assert.ThrowsAsync<DeliveryRejectedException>(() =>
            _workspace.SendMessageAsync(new SendMessageRequest(events, Draft("invoice.created"))));

        Assert.Contains("no queue bound for routing key 'invoice.created'", error.Message, StringComparison.Ordinal);
        var received = Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders"))));
        Assert.Equal("acme", received.ApplicationProperties.Single(property => property.Name == "tenant").Value);
        Assert.Equal("order.created", received.Properties.Subject);
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task ActivatedScheduledPublishCanBeSelectivelyRetriedAfterBindingRepairWithoutResendingConfirmedItem()
    {
        var original = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.Active, 7,
            "retry body"u8.ToArray(), new EditableMessageProperties(MessageId: "stable-returned-id", Subject: "retry.repaired"));
        var store = new BatchReplayStore(Path.Combine(_directory.Path, "operations"));
        var plan = await store.CreateResendAsync(_workspace.ConnectedProfileId!.Value,
            [new ResendItem(original, ServiceBusEntityReference.Topic("events"), original.CreateDraft())],
            ResendMode.Copy, 50, _workspace.ConnectedNamespace, _workspace.ConnectedConfigurationIdentity!, "Scheduled resend", default,
            deferActivation: true);

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0], false, _workspace, () => true, null, default));
        Assert.Equal(0u, (await _setup.QueueDeclarePassiveAsync("orders")).MessageCount);
        await store.ActivateScheduledAsync(plan, default);

        var rejected = await store.RunItemsAsync(plan, [0], false, _workspace, () => true, null, default);
        Assert.Equal(1, rejected.FailedCount);
        Assert.Equal("Rejected", Assert.Single(store.ReadHistory(plan).Items).State);
        Assert.Equal(0u, (await _setup.QueueDeclarePassiveAsync("orders")).MessageCount);

        await _setup.QueueBindAsync("orders", "events", "retry.repaired");
        var retried = await store.RunItemsAsync(plan, [0], true, _workspace, () => true, null, default);
        Assert.Equal(1, retried.SentCount);
        Assert.Equal("Sent", Assert.Single(store.ReadHistory(plan).Items).State);
        var received = await _setup.BasicGetAsync("orders", autoAck: true);
        Assert.NotNull(received);
        Assert.Equal("stable-returned-id", received.BasicProperties.MessageId);
        Assert.Equal("retry body", Encoding.UTF8.GetString(received.Body.Span));
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0], true, _workspace, () => true, null, default));
        Assert.Null(await _setup.BasicGetAsync("orders", autoAck: true));
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task ReplayExecutorReturnIsRetainedAcrossReopenAndRouteRepairUsesSavedIdOnce()
    {
        var store = new BatchReplayStore(Path.Combine(_directory.Path, "replay-operations"));
        var draft = new MessageDraft(new EditableMessageBody("replay body", MessageBodyFormat.Text),
            new EditableMessageProperties(MessageId: "source-id", Subject: "replay.repaired"));
        var plan = await store.CreateAsync(_workspace.ConnectedProfileId!.Value, ServiceBusEntityReference.Topic("events"),
            [(draft, "isolated replay")], false, 50, default, _workspace.ConnectedNamespace, _workspace.ConnectedConfigurationIdentity);
        var savedId = Assert.Single(store.ReadHistory(plan).Items).MessageId;
        Assert.NotEqual("source-id", savedId);
        await Assert.ThrowsAsync<DeliveryRejectedException>(() => store.RunAsync(plan, _workspace, () => true, null, default));
        var reopened = new BatchReplayStore(store.RootDirectory);
        plan = Assert.Single(reopened.List());
        Assert.Equal("Rejected", Assert.Single(reopened.ReadHistory(plan).Items).State);
        Assert.Equal(0u, (await _setup.QueueDeclarePassiveAsync("orders")).MessageCount);
        await _setup.QueueBindAsync("orders", "events", "replay.repaired");
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunAsync(plan, _workspace, () => true, null, default));
        Assert.Equal(0u, (await _setup.QueueDeclarePassiveAsync("orders")).MessageCount);
        var retried = await reopened.RunItemsAsync(plan, [0], true, _workspace, () => true, null, default);
        Assert.Equal(1, retried.SentCount);
        var received = await _setup.BasicGetAsync("orders", autoAck: true);
        Assert.NotNull(received);
        Assert.Equal(savedId, received.BasicProperties.MessageId);
        Assert.Equal("replay body", Encoding.UTF8.GetString(received.Body.Span));
        Assert.Equal(1, (await reopened.RunAsync(plan, _workspace, () => true, null, default)).Sent);
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunItemsAsync(plan, [0], true, _workspace, () => true, null, default));
        Assert.Null(await _setup.BasicGetAsync("orders", autoAck: true));
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task Snapshot_counts_dead_letters_per_queue()
    {
        await PublishAsync("orders", "o-1", "o-2");
        await RejectAsync("orders", 2);
        await WaitForAsync(t => t.Queues.Single(queue => queue.Name == "orders").Runtime.MessageCounts.DeadLetter == 2);

        var snapshot = await _workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All);

        Assert.Equal(2, snapshot.Entities.Single(entity => entity.Entity.Name == "orders").Count);
        Assert.DoesNotContain(snapshot.Entities, entity => entity.Entity.Name == "dead-letters");
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task Queues_are_created_as_quorum_queues_with_a_dead_letter_queue_and_deleted()
    {
        await _workspace.CreateQueueAsync(new QueueDefinition("refunds", new QueueSettings(TimeSpan.FromHours(6), MaxDeliveryCount: 3)));

        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
        var refunds = topology.Queues.Single(queue => queue.Name == "refunds");
        Assert.True(refunds.HasDeadLetterQueue);
        Assert.Contains("Quorum queue, delivery limit 3", refunds.Note, StringComparison.Ordinal);
        Assert.StartsWith("Dead-letter queue of refunds", topology.Queues.Single(queue => queue.Name == "refunds.dlq").Note, StringComparison.Ordinal);
        Assert.Equal(new QueueSettings(TimeSpan.FromHours(6), 3), await _workspace.GetQueueSettingsAsync("refunds"));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _workspace.CreateQueueAsync(new QueueDefinition("refunds", new QueueSettings())));
        await Assert.ThrowsAsync<NotSupportedException>(() => _workspace.UpdateQueueSettingsAsync("refunds", new QueueSettings(MaxDeliveryCount: 5)));

        await _workspace.DeleteQueueAsync("refunds");
        Assert.DoesNotContain((await _workspace.GetTopologyAsync(forceRefresh: true)).Queues, queue => queue.Name == "refunds");
    }

    private async Task PublishAsync(string queue, params string[] ids)
    {
        foreach (var id in ids)
        {
            await _setup.BasicPublishAsync(string.Empty, queue, true,
                new BasicProperties { MessageId = id, ContentType = "application/json", Persistent = true },
                Encoding.UTF8.GetBytes($$"""{"id":"{{id}}"}"""));
        }
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public Task DeletedSource_RetainedForeignDeadLettersStayUntouched() => RetainedForeignDeadLettersAsync(reconfigure: false);

    [EmulatorFact(Emulators.RabbitMq)]
    public Task ReconfiguredSource_RetainedForeignDeadLettersStayUntouched() => RetainedForeignDeadLettersAsync(reconfigure: true);

    private async Task RetainedForeignDeadLettersAsync(bool reconfigure)
    {
        await PublishAsync("orders", "own");
        await PublishAsync("payments", "foreign");
        await RejectAsync("orders", 1);
        await RejectAsync("payments", 1);
        await PublishAsync("dead-letters", "unattributed");
        await WaitForAsync(topology => topology.Queues.Single(queue => queue.Name == "dead-letters").Runtime.MessageCounts.Active == 3);
        await _setup.QueueDeleteAsync("payments");
        if (reconfigure) await _setup.QueueDeclareAsync("payments", durable: true, exclusive: false, autoDelete: false);
        await _workspace.GetTopologyAsync(forceRefresh: true);

        var orders = ServiceBusEntityReference.Queue("orders");
        var browsed = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));
        Assert.Equal(["own"], browsed.Select(message => message.Properties.MessageId));
        await Assert.ThrowsAsync<NotSupportedException>(() => _workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest(
            [new(orders, ServiceBusSubQueue.DeadLetter, 0, "foreign"), new(orders, ServiceBusSubQueue.DeadLetter, 0, "unattributed")])));
        var purged = await _workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest(
            [new(orders, ServiceBusSubQueue.DeadLetter)], batchSize: 1, maximumMessagesPerSubQueue: 10));
        Assert.False(purged.HasFailures);
        Assert.Equal(1, purged.DeletedCount);
        var remaining = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("dead-letters")));
        Assert.Equal(["foreign", "unattributed"], remaining.Select(message => message.Properties.MessageId).Order(StringComparer.Ordinal));
        Assert.Equal(2u, (await _setup.QueueDeclarePassiveAsync("dead-letters")).MessageCount);
        Assert.Single(await new JsonDeadLetterBackupRepository(QueueLoomPaths.ForRoot(_directory.Path)).ListAsync());
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task BinaryAndTextHeaders_SurviveUnchangedResendAndBackupRestore()
    {
        var binary = new byte[] { 0xff, 0xc3, 0x28, 0 };
        var text = Encoding.UTF8.GetBytes("Привет, RabbitMQ");
        await _setup.BasicPublishAsync(string.Empty, "orders", true,
            new BasicProperties { MessageId = "headers", Headers = new Dictionary<string, object?> { ["binary"] = binary, ["text"] = text } },
            "fixture"u8.ToArray());
        var original = Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders"))));
        await _workspace.SendMessageAsync(new SendMessageRequest(ServiceBusEntityReference.Queue("payments"), original.CreateDraft()));
        AssertHeaders((await _setup.BasicGetAsync("payments", autoAck: true))!);

        var paths = QueueLoomPaths.ForRoot(_directory.Path);
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword))
            with { Provider = MessagingProvider.RabbitMq };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
        await session.BackupAsync(original, default);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
        await _workspace.SendMessageAsync(new SendMessageRequest(ServiceBusEntityReference.Queue("payments"), restored.CreateDraft()));
        AssertHeaders((await _setup.BasicGetAsync("payments", autoAck: true))!);

        void AssertHeaders(BasicGetResult result)
        {
            Assert.NotNull(result);
            Assert.Equal(binary, Assert.IsType<byte[]>(result.BasicProperties.Headers!["binary"]));
            Assert.Equal(text, Assert.IsType<byte[]>(result.BasicProperties.Headers!["text"]));
        }
    }

    /// <summary>Rejects messages the way a failing consumer does, so RabbitMQ dead-letters them.</summary>
    private async Task RejectAsync(string queue, int count)
    {
        await using var channel = await _connection.CreateChannelAsync();
        for (var index = 0; index < count; index++)
        {
            var message = await channel.BasicGetAsync(queue, autoAck: false) ?? throw new InvalidOperationException("Nothing to reject.");
            await channel.BasicRejectAsync(message.DeliveryTag, requeue: false);
        }
    }

    /// <summary>The management API refreshes its counters every few seconds.</summary>
    private async Task<ServiceBusTopology> WaitForAsync(Func<ServiceBusTopology, bool> condition)
    {
        for (var attempt = 0; ; attempt++)
        {
            var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
            if (condition(topology) || attempt == 40)
            {
                return topology;
            }
            await Task.Delay(500);
        }
    }
}
