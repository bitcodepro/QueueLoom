using System.Text.Json;
using Amazon.SQS.Model;
using Google.Cloud.PubSub.V1;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class MultiProviderTests
{
    private const string ServiceAccountKey =
        """{"type":"service_account","project_id":"orders-prod-4821","private_key":"-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n","client_email":"queueloom@orders-prod-4821.iam.gserviceaccount.com"}""";

    [Fact]
    public void Aws_profile_needs_a_valid_region_and_endpoint()
    {
        var valid = AwsProfile(new AwsSettings("eu-west-1", "http://localhost:4566"));
        var badRegion = AwsProfile(new AwsSettings("Frankfurt"));
        var badEndpoint = AwsProfile(new AwsSettings("eu-west-1", "localhost:4566"));

        Assert.True(ProfileValidator.Validate(valid).IsValid);
        Assert.True(ProfileValidator.Validate(badRegion).HasError("profile.aws.region.invalid"));
        Assert.True(ProfileValidator.Validate(badEndpoint).HasError("profile.aws.service_url.invalid"));
        Assert.True(ProfileValidator.Validate(valid with { Aws = null }).HasError("profile.aws.region.required"));
    }

    [Fact]
    public void Google_profile_needs_a_project_id_and_matching_sign_in()
    {
        var valid = GoogleProfile(new GooglePubSubSettings("orders-prod-4821", "localhost:8085"));

        Assert.True(ProfileValidator.Validate(valid).IsValid);
        Assert.True(ProfileValidator.Validate(valid with { GooglePubSub = new GooglePubSubSettings("Orders Prod") })
            .HasError("profile.gcp.project.invalid"));
        Assert.True(ProfileValidator.Validate(valid with { GooglePubSub = new GooglePubSubSettings("orders-prod-4821", "localhost") })
            .HasError("profile.gcp.emulator.invalid"));
        Assert.True(ProfileValidator.Validate(valid with { Authentication = new AuthenticationSettings(AuthenticationKind.AwsAccessKey) })
            .HasError("profile.authentication.provider_mismatch"));
        Assert.True(ProfileValidator.Validate(valid with { Aws = new AwsSettings("eu-west-1") })
            .HasError("profile.provider.settings_unexpected"));
    }

    [Fact]
    public async Task Profiles_saved_before_other_clouds_load_as_Azure_and_new_ones_round_trip()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var legacyId = Guid.NewGuid();
        await File.WriteAllTextAsync(paths.ProfilesFile, $$"""
            {"schemaVersion":1,"selectedProfileId":null,"profiles":[{"id":"{{legacyId}}","name":"Old","environment":"Development",
            "customEnvironmentName":null,"fullyQualifiedNamespace":null,"authentication":{"kind":"ConnectionString","entraId":null},
            "accessMode":"ReadWrite","emulatorManagementPort":5300}]}
            """);
        using var repository = new JsonProfileRepository(paths);
        var aws = AwsProfile(new AwsSettings("us-east-1", ProfileName: "work"), AuthenticationKind.AwsDefaultCredentials);

        await repository.UpsertAsync(aws);
        var profiles = await repository.ListAsync();

        Assert.Equal(MessagingProvider.AzureServiceBus, profiles.Single(profile => profile.Id == legacyId).Provider);
        var loaded = profiles.Single(profile => profile.Id == aws.Id);
        Assert.Equal(MessagingProvider.AmazonSqsSns, loaded.Provider);
        Assert.Equal(aws.Aws, loaded.Aws);
        Assert.Equal("AWS profile · work", loaded.AuthenticationDisplayName);
        Assert.Equal("us-east-1", loaded.EndpointDisplay);
    }

    [Fact]
    public void Editor_builds_an_aws_environment_with_its_access_key_as_the_secret()
    {
        var editor = new ProfileEditorViewModel(null)
        {
            Name = "Orders · LocalStack",
            SelectedProvider = new ProviderOption(MessagingProvider.AmazonSqsSns, "", "")
        };
        editor.SelectedProvider = editor.ProviderOptions.Single(option => option.Provider == MessagingProvider.AmazonSqsSns);
        editor.AwsRegion = "EU-WEST-1";
        editor.AwsServiceUrl = "http://localhost:4566/";
        editor.AwsAccessKeyId = "test";

        Assert.False(editor.TryBuild(out _));
        Assert.Contains("secret access key", editor.Error);

        editor.AwsSecretAccessKey = "secret";
        Assert.True(editor.TryBuild(out var result));

        Assert.Equal(AuthenticationKind.AwsAccessKey, result!.Profile.Authentication.Kind);
        Assert.Equal(new AwsSettings("eu-west-1", "http://localhost:4566"), result.Profile.Aws);
        Assert.True(result.ReplacesConnectionString);
        Assert.Equal(new AwsAccessKey("test", "secret"), AwsAccessKey.Parse(result.ConnectionString));
    }

    [Fact]
    public void Editing_keeps_the_saved_secret_and_the_cloud()
    {
        var saved = GoogleProfile(new GooglePubSubSettings("orders-prod-4821"), AuthenticationKind.GoogleServiceAccountKey);
        var editor = new ProfileEditorViewModel(saved);

        editor.SelectedProvider = editor.ProviderOptions.Single(option => option.Provider == MessagingProvider.AzureServiceBus);
        Assert.True(editor.TryBuild(out var result));

        Assert.False(editor.CanChangeProvider);
        Assert.Equal(MessagingProvider.GooglePubSub, result!.Profile.Provider);
        Assert.Null(result.ConnectionString);
        Assert.False(result.ReplacesConnectionString);
    }

    [Fact]
    public void Editor_reads_the_project_from_a_service_account_key_and_rejects_other_json()
    {
        var editor = new ProfileEditorViewModel(null) { Name = "Orders" };
        editor.SelectedProvider = editor.ProviderOptions.Single(option => option.Provider == MessagingProvider.GooglePubSub);

        editor.GoogleServiceAccountKey = """{"type":"authorized_user"}""";
        Assert.False(editor.TryBuild(out _));
        Assert.Contains("not a service account key", editor.Error);

        editor.GoogleServiceAccountKey = ServiceAccountKey;
        Assert.Equal("orders-prod-4821", editor.GoogleProjectId);
        Assert.Contains("queueloom@orders-prod-4821", editor.GoogleKeySummary);
        Assert.True(editor.TryBuild(out var result));
        Assert.Equal(ServiceAccountKey, result!.ConnectionString);
    }

    [Theory]
    [InlineData(ApplicationPropertyType.String, "contoso")]
    [InlineData(ApplicationPropertyType.Int32, "42")]
    [InlineData(ApplicationPropertyType.Decimal, "1.25")]
    [InlineData(ApplicationPropertyType.Boolean, "True")]
    [InlineData(ApplicationPropertyType.Guid, "9f8c2c55-5d0b-4d9e-9a57-0a3c1a2b3c4d")]
    [InlineData(ApplicationPropertyType.Binary, "AQID")]
    public void Sqs_attributes_keep_the_property_type(ApplicationPropertyType type, string value)
    {
        var property = new MessageApplicationProperty("p", type, value);

        var (dataType, stringValue, binaryValue) = AwsMessageMapper.ToAttribute(property);
        var back = AwsMessageMapper.ToProperty("p", dataType, stringValue, binaryValue is null ? null : new MemoryStream(binaryValue));

        Assert.Equal(property, back);
    }

    [Fact]
    public void Plain_sqs_number_attributes_become_numbers()
    {
        Assert.Equal(ApplicationPropertyType.Int64, AwsMessageMapper.ToProperty("n", "Number", "7", null).Type);
        Assert.Equal(ApplicationPropertyType.Decimal, AwsMessageMapper.ToProperty("n", "Number", "7.5", null).Type);
        Assert.Equal(ApplicationPropertyType.String, AwsMessageMapper.ToProperty("s", "String.Custom", "x", null).Type);
    }

    [Fact]
    public void Sqs_messages_map_system_attributes_and_standard_properties()
    {
        var message = new Message
        {
            MessageId = "m-1",
            ReceiptHandle = "r-1",
            Body = "hello",
            Attributes = new Dictionary<string, string>
            {
                ["SentTimestamp"] = "1767225600000",
                ["ApproximateReceiveCount"] = "4",
                ["MessageGroupId"] = "customer-7",
                ["DeadLetterQueueSourceArn"] = "arn:aws:sqs:eu-west-1:000000000000:orders"
            },
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                ["CorrelationId"] = new() { DataType = "String", StringValue = "c-1" },
                ["tenant"] = new() { DataType = "String", StringValue = "contoso" }
            }
        };

        var browsed = AwsMessageMapper.FromSqs(message, ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter);

        Assert.Equal("m-1", browsed.Properties.MessageId);
        Assert.Equal("c-1", browsed.Properties.CorrelationId);
        Assert.Equal("customer-7", browsed.Properties.SessionId);
        Assert.Equal(4, browsed.DeliveryCount);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1767225600000), browsed.EnqueuedAt);
        Assert.Equal("From orders after 4 receives", browsed.DeadLetterErrorDescription);
        Assert.Equal(new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "contoso"),
            Assert.Single(browsed.ApplicationProperties));
        Assert.Equal(LeasedMessageIdentity.SequenceNumberFor("m-1"), browsed.SequenceNumber);
    }

    [Fact]
    public void Aws_topology_names_subscriptions_readably_and_explains_dead_letter_queues()
    {
        var dlq = Queue("dlq");
        var orders = Queue("orders", dlq.Arn, visible: 3);
        var payments = Queue("payments", dlq.Arn);
        var billing = Queue("billing");
        var topic = AwsTopicInfo.From("arn:aws:sns:eu-west-1:000000000000:events",
        [
            new AwsSubscriptionInfo("arn:aws:sns:eu-west-1:000000000000:events:1", "sqs", billing.Arn, dlq.Arn),
            new AwsSubscriptionInfo("arn:aws:sns:eu-west-1:000000000000:events:2", "https", "https://hooks.example.com/a/b", null),
            new AwsSubscriptionInfo("arn:aws:sns:eu-west-1:000000000000:events:3", "https", "https://hooks.example.com/c", null),
            new AwsSubscriptionInfo("PendingConfirmation", "email", "ops@example.com", null)
        ]);

        var topology = new AwsTopologyIndex([dlq with { Visible = 5 }, orders, payments, billing], [topic])
            .ToTopology(DateTimeOffset.UnixEpoch);

        var subscriptions = topology.Topics.Single().Subscriptions;
        Assert.Equal(["email:ops@example.com", "https:hooks.example.com", "https:hooks.example.com (2)", $"sqs:billing"],
            subscriptions.Select(subscription => subscription.Name));
        Assert.All(subscriptions, subscription => Assert.DoesNotContain('/', subscription.Name));
        Assert.Equal("Pending confirmation", subscriptions[0].Note);
        Assert.True(subscriptions.Single(subscription => subscription.Name == "sqs:billing").HasDeadLetterQueue);

        var ordersQueue = topology.Queues.Single(queue => queue.Name == "orders");
        Assert.Equal(5, ordersQueue.Runtime.MessageCounts.DeadLetter);
        Assert.Contains("Shares dead-letter queue dlq with 2 other source(s)", ordersQueue.Note);
        Assert.Equal("Dead-letter queue of orders, payments, events/sqs:billing",
            topology.Queues.Single(queue => queue.Name == "dlq").Note);
        Assert.False(topology.Queues.Single(queue => queue.Name == "billing").HasDeadLetterQueue);
    }

    [Fact]
    public void Pubsub_topology_reads_dead_letters_through_a_subscription_of_the_dead_letter_topic()
    {
        const string project = "orders-prod-4821";
        var deadLetterTopic = new TopicName(project, "events-dlq").ToString();
        var subscriptions = new[]
        {
            Subscription(project, "billing", "events", deadLetterTopic),
            Subscription(project, "shipping", "events", new TopicName(project, "unread-dlq").ToString()),
            Subscription(project, "dlq-reader", "events-dlq", null),
            Subscription(project, "orphan", "_deleted-topic_", null)
        };

        var result = GooglePubSubTopology.Build(project, ["events", "events-dlq", "unread-dlq"], subscriptions, DateTimeOffset.UnixEpoch);

        Assert.Equal("dlq-reader", result.DeadLetterReaders[deadLetterTopic].SubscriptionName.SubscriptionId);
        var events = result.Topology.Topics.Single(topic => topic.Name == "events");
        Assert.True(events.Subscriptions.Single(item => item.Name == "billing").HasDeadLetterQueue);
        var shipping = events.Subscriptions.Single(item => item.Name == "shipping");
        Assert.True(shipping.HasDeadLetterQueue);
        Assert.NotNull(shipping.Runtime.DeadLetterCountError);
        Assert.Contains("no subscription to read them from", shipping.Note);
        Assert.Equal("Holds dead letters of billing",
            result.Topology.Topics.Single(topic => topic.Name == "events-dlq").Subscriptions.Single().Note);
        Assert.Contains(result.Topology.Topics, topic => topic.Name == "_deleted-topic_");
        Assert.All(result.Topology.Topics.SelectMany(topic => topic.Subscriptions),
            subscription => Assert.True(subscription.Runtime.CountsUnavailable));
        Assert.False(result.Topology.HasMessageCounts);
    }

    // A dead-letter reader with no Cloud Monitoring series yet has no count: it is sampled by reading, never shown as 0.
    [Fact]
    public void Pubsub_dead_letter_reader_missing_from_cloud_monitoring_is_sampled_not_zero()
    {
        const string project = "orders-prod-4821";
        var deadLetterTopic = new TopicName(project, "events-dlq").ToString();
        var subscriptions = new[]
        {
            Subscription(project, "billing", "events", deadLetterTopic),
            Subscription(project, "dlq-reader", "events-dlq", null)
        };

        var result = GooglePubSubTopology.Build(project, ["events", "events-dlq"], subscriptions, DateTimeOffset.UnixEpoch,
            new Dictionary<string, long> { ["billing"] = 12 });

        var billing = result.Topology.Topics.Single(topic => topic.Name == "events").Subscriptions.Single(item => item.Name == "billing").Runtime;
        Assert.True(billing.CountsUnavailable);
    }

    [Fact]
    public void Pubsub_counts_from_cloud_monitoring_fill_active_and_dead_letter_columns()
    {
        const string project = "orders-prod-4821";
        var deadLetterTopic = new TopicName(project, "events-dlq").ToString();
        var subscriptions = new[]
        {
            Subscription(project, "billing", "events", deadLetterTopic),
            Subscription(project, "shipping", "events", null),
            Subscription(project, "dlq-reader", "events-dlq", null)
        };
        var undelivered = new Dictionary<string, long> { ["billing"] = 12, ["dlq-reader"] = 4 };

        var result = GooglePubSubTopology.Build(project, ["events", "events-dlq"], subscriptions, DateTimeOffset.UnixEpoch, undelivered);

        Assert.True(result.Topology.HasMessageCounts);
        Assert.False(result.Topology.UsesSampledCounts);
        var events = result.Topology.Topics.Single(topic => topic.Name == "events");
        var billing = events.Subscriptions.Single(item => item.Name == "billing").Runtime;
        Assert.False(billing.CountsUnavailable);
        Assert.Equal(12, billing.MessageCounts.Active);
        Assert.Equal(4, billing.MessageCounts.DeadLetter);
        Assert.True(billing.CountsAreEstimates); // Cloud Monitoring series are sampled and delayed.
        var shipping = events.Subscriptions.Single(item => item.Name == "shipping").Runtime;
        Assert.Equal(0, shipping.MessageCounts.Active);
        Assert.False(shipping.CountsUnavailable);
    }

    [Fact]
    public void Search_targets_skip_sources_without_a_dead_letter_queue_and_transfer_queues_outside_Azure()
    {
        var topology = new ServiceBusTopology(DateTimeOffset.UnixEpoch,
            [
                new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty),
                new ServiceBusQueue("orders-dlq", ServiceBusEntityRuntime.Empty) { HasDeadLetterQueue = false }
            ])
        { SupportsTransferDeadLetter = false };

        var target = Assert.Single(DeadLetterSearchTargets.ForTopology(topology));

        Assert.Equal("orders", target.Source.Name);
        Assert.Equal(ServiceBusSubQueue.DeadLetter, target.SubQueue);
    }

    [Fact]
    public async Task Receiving_skips_other_sources_messages_and_duplicate_deliveries_but_holds_them_for_release()
    {
        var mine = Leased("a", belongs: true);
        var channel = new ScriptedChannel(
            [mine, Leased("b", belongs: false)],
            [mine with { LeaseHandle = "a-again" }, Leased("c", belongs: true)],
            [],
            []);
        var held = new List<LeasedMessage>();

        var result = await LeasedMessagingWorkspace.ReceiveUpToAsync(channel, 10, held, CancellationToken.None);

        Assert.Equal(["a", "c"], result.Select(message => message.Identity));
        Assert.Equal(4, held.Count);
    }

    [Fact]
    public async Task The_router_connects_through_the_workspace_of_the_environments_cloud()
    {
        var created = new List<FakeWorkspace>();
        await using var router = new MultiProviderWorkspace(provider =>
        {
            var workspace = new FakeWorkspace(provider);
            created.Add(workspace);
            return workspace;
        });
        var aws = AwsProfile(new AwsSettings("eu-west-1"));
        var azure = new ServiceBusProfile(Guid.NewGuid(), "Azure", EnvironmentKind.Development, null, null,
            AuthenticationSettings.ConnectionString());

        await router.ConnectAsync(aws);
        Assert.Equal(MessagingProvider.AmazonSqsSns, router.ConnectedProvider);
        Assert.Equal(aws.Id, router.ConnectedProfileId);

        await router.ConnectAsync(azure);
        Assert.Equal(WorkspaceConnectionState.Disconnected, created[0].ConnectionState);
        Assert.Equal(azure.Id, router.ConnectedProfileId);
        Assert.Equal(MessagingProvider.AzureServiceBus, created[1].Provider);
    }

    [Fact]
    public void Entities_without_counts_show_a_dash_not_zero()
    {
        var item = new EntityItemViewModel(
            ServiceBusEntityReference.Subscription("events", "billing"),
            new ServiceBusEntityRuntime(ServiceBusMessageCounts.Empty) { CountsUnavailable = true, HasTransferDeadLetterCount = false },
            ServiceBusEntityStatus.Active,
            requiresSession: false,
            indent: 1,
            note: "Dead letters go to events-dlq");

        Assert.Equal("—", item.ActiveDisplay);
        Assert.Equal("—", item.DeadLettersDisplay);
        Assert.Equal("—", item.TransferDeadLettersDisplay);
        Assert.Equal("Dead letters go to events-dlq", item.Detail);
    }

    private static ServiceBusProfile AwsProfile(AwsSettings settings, AuthenticationKind kind = AuthenticationKind.AwsAccessKey) =>
        new ServiceBusProfile(Guid.NewGuid(), "AWS", EnvironmentKind.Development, null, null, new AuthenticationSettings(kind))
        {
            Provider = MessagingProvider.AmazonSqsSns,
            Aws = settings
        };

    private static ServiceBusProfile GoogleProfile(
        GooglePubSubSettings settings,
        AuthenticationKind kind = AuthenticationKind.GoogleApplicationDefault) =>
        new ServiceBusProfile(Guid.NewGuid(), "GCP", EnvironmentKind.Development, null, null, new AuthenticationSettings(kind))
        {
            Provider = MessagingProvider.GooglePubSub,
            GooglePubSub = settings
        };

    private static AwsQueueInfo Queue(string name, string? deadLetterArn = null, long visible = 0) =>
        new(name, $"http://localhost:4566/000000000000/{name}", $"arn:aws:sqs:eu-west-1:000000000000:{name}", false,
            visible, 0, 0, deadLetterArn, null, null);

    private static Subscription Subscription(string project, string name, string topic, string? deadLetterTopic)
    {
        var subscription = new Subscription
        {
            SubscriptionName = new SubscriptionName(project, name),
            Topic = topic.StartsWith('_') ? topic : new TopicName(project, topic).ToString(),
            State = Google.Cloud.PubSub.V1.Subscription.Types.State.Active
        };
        if (deadLetterTopic is not null)
        {
            subscription.DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = deadLetterTopic, MaxDeliveryAttempts = 5 };
        }
        return subscription;
    }

    private static LeasedMessage Leased(string id, bool belongs) => new(
        new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter,
            LeasedMessageIdentity.SequenceNumberFor(id), "x"u8.ToArray(), new EditableMessageProperties(MessageId: id)),
        $"{id}-handle",
        belongs);

    private sealed class ScriptedChannel(params IReadOnlyList<LeasedMessage>[] batches) : ILeasedMessageChannel
    {
        private int _next;

        public string PhysicalName => "orders-dlq";

        public int MaximumBatchSize => 10;

        public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken) =>
            Task.FromResult(_next < batches.Length ? batches[_next++] : (IReadOnlyList<LeasedMessage>)[]);

        public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(
            IReadOnlyCollection<LeasedMessage> messages,
            CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyCollection<LeasedMessage>>([]);
    }

    private sealed class FakeWorkspace(MessagingProvider provider) : IServiceBusWorkspace
    {
        public MessagingProvider Provider { get; } = provider;
        public WorkspaceConnectionState ConnectionState { get; private set; }
        public Guid? ConnectedProfileId { get; private set; }

        public Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
        {
            ConnectedProfileId = profile.Id;
            ConnectionState = WorkspaceConnectionState.Connected;
            return Task.CompletedTask;
        }

        public Task DisconnectAsync(CancellationToken cancellationToken = default)
        {
            ConnectionState = WorkspaceConnectionState.Disconnected;
            return Task.CompletedTask;
        }

        public Task SetAccessModeAsync(ProfileAccessMode accessMode, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<ServiceBusTopology> GetTopologyAsync(bool forceRefresh = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(BrowseMessagesRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<DeadLetterSearchResult> SearchDeadLettersAsync(DeadLetterSearchRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task SendMessageAsync(QueueLoom.Core.ServiceBus.SendMessageRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task ResubmitDeadLetterAsync(ResubmitDeadLetterRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(DeadLetterPurgeRequest request, CancellationToken cancellationToken = default, IProgress<DeadLetterPurgeProgress>? progress = null) => throw new NotSupportedException();
        public Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(DeleteDeadLetterMessagesRequest request, CancellationToken cancellationToken = default, IProgress<DeadLetterMessageDeletionProgress>? progress = null) => throw new NotSupportedException();
        public Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(DeadLetterMonitorScope scope, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
