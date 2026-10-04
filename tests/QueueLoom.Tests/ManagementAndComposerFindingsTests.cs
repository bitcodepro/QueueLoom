using System.Net;
using System.Reflection;
using System.Text;
using Encoding = System.Text.Encoding;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Google.Protobuf.WellKnownTypes;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

public sealed class ManagementAndComposerFindingsTests
{
    // Finding 1: Pub/Sub subscriptions keep unacknowledged messages for 10 minutes to 31 days
    // (projects.subscriptions messageRetentionDuration: "Cannot be more than 31 days or less than 10 minutes").
    // QueueLoom refuses anything above 7 days, so a subscription that already keeps 14 days cannot be edited at all:
    // the settings dialog sends the unchanged retention back with the ack deadline change.
    [Fact]
    public async Task PubSub_SubscriptionWithFourteenDayRetentionCanStillBeEdited()
    {
        await using var workspace = new GooglePubSubWorkspace(new DeepAuditCloudTests.EmptyVault());
        var subscriber = new RecordingSubscriber(new Subscription
        {
            Name = "projects/project-a/subscriptions/orders",
            Topic = "projects/project-a/topics/orders",
            AckDeadlineSeconds = 10,
            MessageRetentionDuration = Duration.FromTimeSpan(TimeSpan.FromDays(14))
        });
        typeof(GooglePubSubWorkspace).GetField("_subscriber", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, subscriber);
        typeof(GooglePubSubWorkspace).GetField("_projectId", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, "project-a");
        typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
            ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Test, new(AuthenticationKind.GoogleApplicationDefault),
                accessMode: ProfileAccessMode.ReadWrite) with
            { Provider = MessagingProvider.GooglePubSub, AllowQueueManagement = true, GooglePubSub = new("project-a", "localhost:8085") });

        var current = await workspace.GetQueueSettingsAsync("orders");
        var dialog = new QueueDialogViewModel(workspace.QueueManagement!, "isolated", "orders", current) { LockSeconds = 60 };
        var settings = dialog.TryBuildSettings();
        Assert.NotNull(settings);

        await workspace.UpdateQueueSettingsAsync("orders", settings);

        var update = Assert.Single(subscriber.Updates);
        Assert.Equal(60, update.Subscription.AckDeadlineSeconds);
        Assert.Equal(TimeSpan.FromDays(14), update.Subscription.MessageRetentionDuration.ToTimeSpan());
    }

    private sealed class RecordingSubscriber(Subscription current) : SubscriberServiceApiClient
    {
        public List<UpdateSubscriptionRequest> Updates { get; } = [];

        public override Task<Subscription> GetSubscriptionAsync(GetSubscriptionRequest request, CallSettings? callSettings = null) =>
            Task.FromResult(current.Clone());

        public override Task<Subscription> UpdateSubscriptionAsync(UpdateSubscriptionRequest request, CallSettings? callSettings = null)
        {
            Updates.Add(request);
            return Task.FromResult(request.Subscription);
        }
    }

    // Finding 2: the queue settings dialog shows a TTL that is not a whole number of hours or days as minutes rounded
    // to two decimals and sends that rounded value back on Save, so an untouched TTL changes on every edit
    // (Azure DefaultMessageTimeToLive, Kafka retention.ms, SQS MessageRetentionPeriod).
    [Theory]
    [InlineData(10)]
    [InlineData(20)]
    [InlineData(100)]
    [InlineData(3601)]
    public void QueueDialog_UntouchedTimeToLiveIsSavedUnchanged(int seconds)
    {
        const QueueSettingFlags flags = QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount |
                                        QueueSettingFlags.LockDuration | QueueSettingFlags.DeadLetterOnExpiration;
        var current = new QueueSettings(TimeSpan.FromSeconds(seconds), 10, TimeSpan.FromSeconds(30), false);
        var dialog = new QueueDialogViewModel(new QueueManagementCapabilities("queue", flags, flags, false), "isolated", "orders", current)
        {
            MaxDeliveryCount = 5 // the operator only changes the delivery count
        };

        var settings = dialog.TryBuildSettings();

        Assert.NotNull(settings);
        Assert.Equal(TimeSpan.FromSeconds(seconds), settings.MessageTimeToLive);
        Assert.Equal(5, settings.MaxDeliveryCount);
    }

    // Finding 3: creating a RabbitMQ queue with "also create a dead-letter queue" declares "<name>.dlq" before the
    // queue itself. When the queue cannot be created (the name is taken, or the broker refuses an argument) the
    // dead-letter queue stays behind, and every retry then fails with "A queue named '<name>.dlq' already exists."
    [Theory]
    [InlineData("exists")]
    [InlineData("rejected")]
    public async Task RabbitMq_FailedQueueCreationLeavesNoDeadLetterQueueBehind(string failure)
    {
        var broker = new QueueBroker(failure == "exists" ? ["orders"] : []) { RejectPutOf = failure == "rejected" ? "orders" : null };
        await using var workspace = new RabbitMqWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(RabbitMqWorkspace).GetField("_management", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(workspace, new HttpClient(broker) { BaseAddress = new Uri("http://broker.invalid/") });
        typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
            ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Test, new(AuthenticationKind.RabbitMqPassword),
                accessMode: ProfileAccessMode.ReadWrite) with
            { Provider = MessagingProvider.RabbitMq, AllowQueueManagement = true, RabbitMq = new("broker.invalid", "guest") });

        await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.CreateQueueAsync(
            new QueueDefinition("orders", new QueueSettings(TimeSpan.FromDays(60)), CreateDeadLetterQueue: true)));

        Assert.DoesNotContain("orders.dlq", broker.Queues);
    }

    // An indeterminate outcome: the broker creates the queue, but the response is lost (timeout, cancellation or a
    // dropped connection). The main queue routes its dead letters to "<name>.dlq", so that queue must stay.
    [Theory]
    [InlineData("timeout")]
    [InlineData("connection")]
    public async Task RabbitMq_DeadLetterQueueStaysWhenTheMainQueueMayHaveBeenCreated(string loss)
    {
        var broker = new QueueBroker([]) { LoseResponseOf = "orders", Loss = loss };
        await using var workspace = new RabbitMqWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(RabbitMqWorkspace).GetField("_management", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(workspace, new HttpClient(broker) { BaseAddress = new Uri("http://broker.invalid/") });
        typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
            ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Test, new(AuthenticationKind.RabbitMqPassword),
                accessMode: ProfileAccessMode.ReadWrite) with
            { Provider = MessagingProvider.RabbitMq, AllowQueueManagement = true, RabbitMq = new("broker.invalid", "guest") });

        await Assert.ThrowsAnyAsync<Exception>(() => workspace.CreateQueueAsync(
            new QueueDefinition("orders", new QueueSettings(TimeSpan.FromMinutes(5)), CreateDeadLetterQueue: true)));

        Assert.Contains("orders", broker.Queues);
        Assert.Contains("orders.dlq", broker.Queues);
    }

    // Another creator wins the race: after the first lookup of "orders" returns 404 and this call creates
    // "orders.dlq", someone else creates "orders" routing to that dead-letter queue. The live queue's dead-letter
    // target must survive, whether the race shows up in the second lookup or as RabbitMQ's 204 on the PUT.
    [Theory]
    [InlineData("before-lookup")]
    [InlineData("before-put")]
    public async Task RabbitMq_DeadLetterQueueStaysWhenAnotherCreatorWinsTheRace(string moment)
    {
        var broker = new QueueBroker([]) { CreateConcurrently = ("orders", moment) };
        await using var workspace = new RabbitMqWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(RabbitMqWorkspace).GetField("_management", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(workspace, new HttpClient(broker) { BaseAddress = new Uri("http://broker.invalid/") });
        typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
            ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Test, new(AuthenticationKind.RabbitMqPassword),
                accessMode: ProfileAccessMode.ReadWrite) with
            { Provider = MessagingProvider.RabbitMq, AllowQueueManagement = true, RabbitMq = new("broker.invalid", "guest") });

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.CreateQueueAsync(
            new QueueDefinition("orders", new QueueSettings(TimeSpan.FromMinutes(5)), CreateDeadLetterQueue: true)));

        Assert.Contains("already exists", error.Message, StringComparison.Ordinal);
        Assert.Contains("orders", broker.Queues);
        Assert.Contains("orders.dlq", broker.Queues);
        Assert.DoesNotContain(broker.Requests, request => request.StartsWith("DELETE", StringComparison.Ordinal));
    }

    private sealed class QueueBroker(IEnumerable<string> existing) : HttpMessageHandler
    {
        /// <summary>Another creator declares this queue right after the dead-letter queue PUT succeeds.</summary>
        public (string Name, string Moment)? CreateConcurrently { get; init; }
        public List<string> Requests { get; } = [];
        public HashSet<string> Queues { get; } = [.. existing];
        public string? RejectPutOf { get; init; }
        public string? LoseResponseOf { get; init; }
        public string Loss { get; init; } = "timeout";

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var name = Uri.UnescapeDataString(request.RequestUri!.AbsolutePath.Split('/').Last());
            Requests.Add($"{request.Method} {name}");
            if (CreateConcurrently is { } race && name == race.Name && Queues.Contains(race.Name + ".dlq") && !Queues.Contains(race.Name) &&
                (race.Moment == "before-lookup" && request.Method == HttpMethod.Get ||
                 race.Moment == "before-put" && request.Method == HttpMethod.Put))
            {
                // The other creator's declaration lands now; a PUT then finds an equivalent queue (RabbitMQ: 204).
                Queues.Add(race.Name);
                if (request.Method == HttpMethod.Put)
                {
                    return Task.FromResult(Reply(HttpStatusCode.NoContent, string.Empty));
                }
            }
            HttpResponseMessage Reply(HttpStatusCode status, string body = "{}") =>
                new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
            if (request.Method == HttpMethod.Get)
                return Task.FromResult(Queues.Contains(name) ? Reply(HttpStatusCode.OK) : Reply(HttpStatusCode.NotFound));
            if (request.Method == HttpMethod.Put)
            {
                if (name == RejectPutOf)
                    return Task.FromResult(Reply(HttpStatusCode.BadRequest,
                        """{"error":"bad_request","reason":"precondition_failed: invalid arg 'x-message-ttl': {value_too_large,5184000000}"}"""));
                Queues.Add(name);
                if (name == LoseResponseOf)
                {
                    // Created on the broker, but the answer never arrives.
                    return Loss == "timeout"
                        ? Task.FromException<HttpResponseMessage>(new TaskCanceledException("The request timed out."))
                        : Task.FromException<HttpResponseMessage>(new HttpRequestException("The connection was reset."));
                }
                return Task.FromResult(Reply(HttpStatusCode.Created));
            }
            if (request.Method == HttpMethod.Delete)
            {
                Queues.Remove(name);
                return Task.FromResult(Reply(HttpStatusCode.NoContent));
            }
            return Task.FromResult(Reply(HttpStatusCode.MethodNotAllowed));
        }
    }
}

public sealed partial class ViewModelStateTests
{
    // Finding 4: MessageDraftValidator applies Azure Service Bus' 128-character limit to PartitionKey for every
    // service. A Kafka record key has no such limit (Debezium/Kafka Connect JSON keys with schemas are typically
    // longer), so resending such a dead-letter record is refused with "PartitionKey cannot exceed 128 characters".
    [Fact]
    public async Task KafkaResend_RecordWithLongKeyIsResent()
    {
        var key = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"}]," +
                  "\"optional\":false,\"name\":\"dbserver1.inventory.customers.Key\"},\"payload\":{\"id\":1001}}";
        Assert.True(key.Length > 128);
        var profile = ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development, new(AuthenticationKind.KafkaNone),
            accessMode: ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.Kafka, Kafka = new KafkaSettings("broker.invalid:9092") };
        var topic = new ServiceBusQueue("customers", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)));
        var headers = new Confluent.Kafka.Headers { { "CorrelationId", Encoding.UTF8.GetBytes("correlation-42") } };
        var record = KafkaMessageMapper.FromKafka(new Confluent.Kafka.ConsumeResult<byte[]?, byte[]?>
        {
            Topic = "customers", Partition = new Confluent.Kafka.Partition(0), Offset = new Confluent.Kafka.Offset(7),
            Message = new Confluent.Kafka.Message<byte[]?, byte[]?> { Key = Encoding.UTF8.GetBytes(key), Value = Encoding.UTF8.GetBytes("{}"), Headers = headers }
        }, topic.Reference, ServiceBusSubQueue.DeadLetter);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [topic]),
            SearchMatches = { [profile.Id] = [record] }
        };
        workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow, [new DeadLetterEntitySnapshot(topic.Reference, 1)]);
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        viewModel.DeadLetterSearchQuery = "correlation-42";
        await viewModel.SearchDeadLettersCommand.ExecuteAsync();
        Assert.Single(viewModel.Messages).IsMarked = true;

        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();

        Assert.Equal(string.Empty, viewModel.ErrorText);
        Assert.Equal(key, Assert.Single(workspace.SentMessages).Message.Properties.PartitionKey);
    }

    // Finding 5: the composer parses "TTL · seconds" with double.TryParse(text) — the current culture and
    // NumberStyles.AllowThousands. A decimal written with the other separator is read as a group separator and the
    // TTL silently becomes ten times longer: "1.5" is 15 s under de-DE, "1,5" is 15 s under en-US.
    [Theory]
    [InlineData("de-DE", "1.5")]
    [InlineData("en-US", "1,5")]
    public async Task Composer_TimeToLiveIsNotSilentlyMultipliedByTheGroupSeparator(string culture, string text)
    {
        using var _ = new TestCulture(culture);
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.Destinations.Add(new DestinationItemViewModel(ServiceBusEntityReference.Queue("orders")));
        viewModel.NewMessageCommand.Execute(null);
        viewModel.SelectedDestination = viewModel.Destinations.Last();
        viewModel.DraftTimeToLiveSeconds = text;

        await viewModel.SendDraftCommand.ExecuteAsync();

        // Either the intended 1.5 seconds is sent, or the ambiguous text is refused; never 15 seconds.
        Assert.DoesNotContain(workspace.SentMessages, request => request.Message.Properties.TimeToLive == TimeSpan.FromSeconds(15));
    }
}
