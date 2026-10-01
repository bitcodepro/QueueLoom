using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.IntegrationTests;

/// <summary>Runs the Pub/Sub workspace against the Google Cloud Pub/Sub emulator.</summary>
public sealed class GooglePubSubWorkspaceTests : IAsyncLifetime
{
    private const string Project = "queueloom-test";
    private readonly TemporaryDirectory _directory = new();
    private PublisherServiceApiClient _publisher = null!;
    private SubscriberServiceApiClient _subscriber = null!;
    private GooglePubSubWorkspace _workspace = null!;
    private string _events = null!;
    private string _deadLetterTopic = null!;
    private string _billing = null!;
    private string _shipping = null!;
    private string _audit = null!;
    private string _deadLetterReader = null!;

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.PubSub)))
        {
            return;
        }

        _publisher = await new PublisherServiceApiClientBuilder
        {
            Endpoint = Emulators.PubSubHost,
            ChannelCredentials = ChannelCredentials.Insecure
        }.BuildAsync();
        _subscriber = await new SubscriberServiceApiClientBuilder
        {
            Endpoint = Emulators.PubSubHost,
            ChannelCredentials = ChannelCredentials.Insecure
        }.BuildAsync();

        _events = Emulators.Unique("events");
        _deadLetterTopic = Emulators.Unique("events-dlq");
        _billing = Emulators.Unique("billing");
        _shipping = Emulators.Unique("shipping");
        _audit = Emulators.Unique("audit");
        _deadLetterReader = Emulators.Unique("dlq-reader");

        await _publisher.CreateTopicAsync(new TopicName(Project, _events));
        await _publisher.CreateTopicAsync(new TopicName(Project, _deadLetterTopic));
        foreach (var name in new[] { _billing, _shipping })
        {
            await _subscriber.CreateSubscriptionAsync(new Subscription
            {
                SubscriptionName = new SubscriptionName(Project, name),
                TopicAsTopicName = new TopicName(Project, _events),
                AckDeadlineSeconds = 10,
                DeadLetterPolicy = new DeadLetterPolicy
                {
                    DeadLetterTopic = new TopicName(Project, _deadLetterTopic).ToString(),
                    MaxDeliveryAttempts = 5
                }
            });
        }
        await _subscriber.CreateSubscriptionAsync(new SubscriptionName(Project, _audit), new TopicName(Project, _events), null, 10);
        await _subscriber.CreateSubscriptionAsync(
            new SubscriptionName(Project, _deadLetterReader), new TopicName(Project, _deadLetterTopic), null, 10);

        var profile = ServiceBusProfile.CreateNew(
                "Pub/Sub emulator",
                EnvironmentKind.Development,
                new AuthenticationSettings(AuthenticationKind.GoogleApplicationDefault),
                accessMode: ProfileAccessMode.ReadWrite)
            with
            {
                Provider = MessagingProvider.GooglePubSub,
                GooglePubSub = new GooglePubSubSettings(Project, Emulators.PubSubHost),
                AllowQueueManagement = true
            };
        _workspace = new GooglePubSubWorkspace(
            new InMemorySecretVault(),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)));
        await _workspace.ConnectAsync(profile);
    }

    public async ValueTask DisposeAsync()
    {
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.PubSub)]
    public async Task Subscriptions_are_created_with_a_dead_letter_topic_changed_and_deleted()
    {
        var name = Emulators.Unique("invoices");
        Assert.True(_workspace.QueueManagement!.ManagesSubscriptions);

        await _workspace.CreateQueueAsync(new QueueDefinition(name,
            new QueueSettings(TimeSpan.FromDays(2), MaxDeliveryCount: 7, LockDuration: TimeSpan.FromSeconds(30)), TopicName: _events));

        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
        var created = topology.Topics.Single(topic => topic.Name == _events).Subscriptions.Single(subscription => subscription.Name == name);
        Assert.True(created.HasDeadLetterQueue);
        Assert.Contains(topology.Topics, topic => topic.Name == name + "-dead-letter");
        Assert.Equal(new QueueSettings(TimeSpan.FromDays(2), 7, TimeSpan.FromSeconds(30)), await _workspace.GetQueueSettingsAsync(name));

        await _workspace.UpdateQueueSettingsAsync(name, new QueueSettings(TimeSpan.FromHours(12), 20, TimeSpan.FromSeconds(60)));
        Assert.Equal(new QueueSettings(TimeSpan.FromHours(12), 20, TimeSpan.FromSeconds(60)), await _workspace.GetQueueSettingsAsync(name));
        await Assert.ThrowsAsync<InvalidOperationException>(() => _workspace.UpdateQueueSettingsAsync(name, new QueueSettings(MaxDeliveryCount: 2)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => _workspace.UpdateQueueSettingsAsync(_audit, new QueueSettings(MaxDeliveryCount: 9)));

        await _workspace.DeleteQueueAsync(name);
        Assert.DoesNotContain((await _workspace.GetTopologyAsync(forceRefresh: true)).Topics.SelectMany(topic => topic.Subscriptions),
            subscription => subscription.Name == name);
    }

    [EmulatorFact(Emulators.PubSub)]
    public async Task Topology_shows_topics_subscriptions_and_their_dead_letter_topics()
    {
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);

        Assert.Empty(topology.Queues);
        Assert.False(topology.HasMessageCounts);
        var events = Assert.Single(topology.Topics, topic => topic.Name == _events);
        Assert.Equal(3, events.Subscriptions.Count);
        var billing = Assert.Single(events.Subscriptions, subscription => subscription.Name == _billing);
        Assert.True(billing.HasDeadLetterQueue);
        Assert.True(billing.Runtime.CountsUnavailable);
        Assert.Contains(_deadLetterTopic, billing.Note);
        Assert.False(Assert.Single(events.Subscriptions, subscription => subscription.Name == _audit).HasDeadLetterQueue);
        var reader = Assert.Single(Assert.Single(topology.Topics, topic => topic.Name == _deadLetterTopic).Subscriptions);
        Assert.Contains(_billing, reader.Note);

        var targets = DeadLetterSearchTargets.ForTopology(topology).Where(target => target.Source.TopicName == _events);
        Assert.Equal([_billing, _shipping], targets.Select(target => target.Source.Name).Order());
    }

    [EmulatorFact(Emulators.PubSub)]
    public async Task Published_messages_can_be_browsed_repeatedly_with_their_properties()
    {
        await _workspace.GetTopologyAsync(forceRefresh: true);
        var draft = new MessageDraft(
            new EditableMessageBody("""{"orderId":1042}""", MessageBodyFormat.Json),
            new EditableMessageProperties(CorrelationId: "c-1", Subject: "OrderPlaced"),
            [new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "contoso")]);

        await _workspace.SendMessageAsync(new SendMessageRequest(ServiceBusEntityReference.Topic(_events), draft));
        var billing = ServiceBusEntityReference.Subscription(_events, _billing);
        var first = Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(billing)));
        var second = Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(billing)));

        Assert.Equal(first.Properties.MessageId, second.Properties.MessageId);
        Assert.Equal("c-1", first.Properties.CorrelationId);
        Assert.Equal("OrderPlaced", first.Properties.Subject);
        Assert.Equal(draft.ApplicationProperties, first.ApplicationProperties);
        Assert.Equal("""{"orderId":1042}""", first.CreateDraft().Body.Content);
    }

    [EmulatorFact(Emulators.PubSub)]
    public async Task Dead_letters_are_shown_for_the_subscription_they_came_from()
    {
        await DeadLetterAsync(_billing, "invoice 7 failed");
        await DeadLetterAsync(_shipping, "parcel 9 failed");
        await _workspace.GetTopologyAsync(forceRefresh: true);

        var billing = ServiceBusEntityReference.Subscription(_events, _billing);
        var messages = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(billing, ServiceBusSubQueue.DeadLetter));

        var message = Assert.Single(messages);
        Assert.Equal("invoice 7 failed", message.CreateDraft().Body.Content);
        Assert.Equal(5, message.DeliveryCount);
        Assert.Contains(_billing, message.DeadLetterErrorDescription);
        Assert.DoesNotContain(message.ApplicationProperties, property => property.Name.StartsWith("CloudPubSub", StringComparison.Ordinal));
    }

    [EmulatorFact(Emulators.PubSub)]
    public async Task Search_delete_and_purge_work_on_dead_letters()
    {
        await DeadLetterAsync(_billing, "invoice 7 failed");
        await DeadLetterAsync(_billing, "invoice 8 failed");
        await DeadLetterAsync(_shipping, "invoice 7 not shipped");
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
        var billing = ServiceBusEntityReference.Subscription(_events, _billing);
        var shipping = ServiceBusEntityReference.Subscription(_events, _shipping);

        var search = await _workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest(
            "invoice 7",
            DeadLetterSearchTargets.ForTopology(topology).Where(target => target.Source.TopicName == _events)));
        Assert.Equal(2, search.MatchCount);

        var victim = search.Matches.Single(match => match.Source == billing);
        var deletion = await _workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest(
            [new DeadLetterMessageKey(billing, ServiceBusSubQueue.DeadLetter, victim.SequenceNumber, victim.Properties.MessageId)]));
        Assert.Equal(1, deletion.DeletedCount);
        Assert.Equal("invoice 8 failed", Assert.Single(await _workspace.BrowseMessagesAsync(
            new BrowseMessagesRequest(billing, ServiceBusSubQueue.DeadLetter))).CreateDraft().Body.Content);

        var purge = await _workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([billing], [ServiceBusSubQueue.DeadLetter]));
        Assert.False(purge.HasFailures);
        Assert.Equal(1, purge.DeletedCount);
        Assert.Empty(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(billing, ServiceBusSubQueue.DeadLetter)));
        Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(shipping, ServiceBusSubQueue.DeadLetter)));
        Assert.Equal(2, Directory.GetFiles(_directory.Path, "0*.json", SearchOption.AllDirectories).Length);
    }

    [EmulatorFact(Emulators.PubSub)]
    public async Task Monitoring_counts_dead_letters_by_reading_them_and_leaves_them_in_place()
    {
        await DeadLetterAsync(_billing, "a");
        await DeadLetterAsync(_billing, "b");
        await DeadLetterAsync(_shipping, "c");
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
        Assert.True(topology.UsesSampledCounts);
        var billing = ServiceBusEntityReference.Subscription(_events, _billing);

        var first = await _workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.ForEntity(billing));
        var second = await _workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.ForEntity(billing));

        Assert.Equal(2, Assert.Single(first.Entities).Count);
        var again = Assert.Single(second.Entities);
        Assert.Equal(2, again.Count);
        Assert.Equal(2, again.PreviousCount);
        Assert.Equal(2, (await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(billing, ServiceBusSubQueue.DeadLetter))).Count);
    }

    [EmulatorFact(Emulators.PubSub)]
    public async Task Queues_and_scheduling_are_rejected_with_a_clear_message()
    {
        await _workspace.GetTopologyAsync(forceRefresh: true);

        var browse = await Assert.ThrowsAsync<InvalidOperationException>(() => _workspace.BrowseMessagesAsync(
            new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders"))));
        Assert.Contains("subscriptions only", browse.Message);
        var schedule = await Assert.ThrowsAsync<InvalidOperationException>(() => _workspace.SendMessageAsync(new SendMessageRequest(
            ServiceBusEntityReference.Topic(_events),
            new MessageDraft(
                new EditableMessageBody("x", MessageBodyFormat.Text),
                new EditableMessageProperties(ScheduledEnqueueTime: DateTimeOffset.UtcNow.AddMinutes(5))))));
        Assert.Contains("cannot schedule", schedule.Message);
    }

    /// <summary>
    /// Puts a message on the dead-letter topic the way Pub/Sub does after too many delivery attempts. The
    /// emulator does not forward messages itself, so the forwarded copy is published directly.
    /// </summary>
    private async Task DeadLetterAsync(string subscriptionId, string body)
    {
        var message = new PubsubMessage { Data = ByteString.CopyFromUtf8(body) };
        message.Attributes["CloudPubSubDeadLetterSourceSubscription"] = subscriptionId;
        message.Attributes["CloudPubSubDeadLetterSourceSubscriptionProject"] = Project;
        message.Attributes["CloudPubSubDeadLetterSourceDeliveryCount"] = "5";
        await _publisher.PublishAsync(new TopicName(Project, _deadLetterTopic), [message]);
    }
}
