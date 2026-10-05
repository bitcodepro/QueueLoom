using System.ComponentModel;
using System.Text.Json;
using Google.Cloud.PubSub.V1;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

/// <summary>
/// SQS, Pub/Sub (with a dead-letter policy) and RabbitMQ quorum queues up to 4.2 count QueueLoom's receive-and-release
/// read as a delivery; Azure Service Bus peeks and Kafka reads by offset. The browse actions say so where it applies.
/// </summary>
public sealed partial class ViewModelStateTests
{
    private static readonly ServiceBusEntityReference DeliveryQueue = ServiceBusEntityReference.Queue("orders");
    private static readonly ServiceBusEntityReference DeliverySubscription = ServiceBusEntityReference.Subscription("events", "billing");

    [Theory]
    [InlineData(MessagingProvider.AmazonSqsSns, "Browsing on Amazon SQS counts as a receive: each look raises a message's receive count, and the redrive policy moves it to the dead-letter queue after 5 receives.")]
    [InlineData(MessagingProvider.GooglePubSub, "Browsing on Google Pub/Sub counts as a delivery: each look raises a message's delivery attempts, and the dead-letter policy forwards it to the dead-letter topic after about 7 attempts.")]
    [InlineData(MessagingProvider.RabbitMq, "Browsing a RabbitMQ quorum queue requeues each message, and RabbitMQ 4.2 and earlier count that as a delivery: past the delivery limit of 5 the message is dead-lettered or dropped.")]
    [InlineData(MessagingProvider.AzureServiceBus, null)]
    [InlineData(MessagingProvider.Kafka, null)]
    public async Task BrowseDeliveryNoteAppearsOnlyWhereReadingCountsAsADelivery(MessagingProvider provider, string? expected)
    {
        var (viewModel, _) = await ConnectForDeliveryNoteAsync(provider);
        await using var _ = viewModel;
        var source = provider == MessagingProvider.GooglePubSub ? DeliverySubscription : DeliveryQueue;

        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.Reference == source);
        Assert.Equal(expected ?? string.Empty, viewModel.SelectedEntityDeliveryNote);
        Assert.Equal(expected is not null, viewModel.HasSelectedEntityDeliveryNote);

        // Messages / DLQ: the selected dead-letter source is what "Peek selected source" reads.
        viewModel.SelectedDlqSource = viewModel.FilteredDeadLetterSources.Single(item => item.Entity == source);
        Assert.Equal(expected is not null, viewModel.HasBrowseDeliveryNote);
        if (expected is null)
        {
            Assert.Equal(string.Empty, viewModel.BrowseDeliveryNote);
        }
        else
        {
            Assert.Contains(provider switch
            {
                MessagingProvider.AmazonSqsSns => "Amazon SQS",
                MessagingProvider.GooglePubSub => "Google Pub/Sub",
                _ => "RabbitMQ"
            }, viewModel.BrowseDeliveryNote, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task SqsDeadLetterReadUsesTheDeadLetterQueuesOwnSettingsAndTheStatusSaysItCounted()
    {
        var (viewModel, workspace) = await ConnectForDeliveryNoteAsync(MessagingProvider.AmazonSqsSns,
            queue => queue with { DeadLetterQueueName = "orders-dlq" },
            new ServiceBusQueue("orders-dlq", new ServiceBusEntityRuntime(ServiceBusMessageCounts.Empty)));
        await using var _ = viewModel;

        viewModel.SelectedDlqSource = viewModel.FilteredDeadLetterSources.Single(item => item.Entity == DeliveryQueue);
        // The dead-letter queue has no redrive policy of its own: only the receive count goes up.
        Assert.Equal("Browsing on Amazon SQS counts as a receive: each look raises a message's receive count.",
            viewModel.BrowseDeliveryNote);

        workspace.BrowseMessages = [new BrowsedMessage(DeliveryQueue, ServiceBusSubQueue.DeadLetter, 1, "x"u8.ToArray(),
            new EditableMessageProperties(MessageId: "m-1"))];
        await viewModel.BrowseDlqSourceCommand.ExecuteAsync();
        Assert.Equal("Read 1 messages and released them unchanged; each read counts as a receive", viewModel.StatusText);
        Assert.True(viewModel.HasBrowseDeliveryNote);
    }

    [Fact]
    public async Task BrowseDeliveryNoteFollowsTheConnectedEnvironment()
    {
        var aws = CreateProfile("Aws", EnvironmentKind.Test) with { Provider = MessagingProvider.AmazonSqsSns };
        var azure = CreateProfile("Azure", EnvironmentKind.Test);
        var workspace = new FakeWorkspace { Topology = DeliveryTopology(queue => queue) };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([aws, azure], aws.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.Reference == DeliveryQueue);
        Assert.True(viewModel.HasSelectedEntityDeliveryNote);

        var changed = new List<string?>();
        PropertyChangedEventHandler record = (_, args) => changed.Add(args.PropertyName);
        viewModel.PropertyChanged += record;
        viewModel.SelectedProfile = viewModel.Profiles.Single(profile => profile.Id == azure.Id);
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.Reference == DeliveryQueue);
        viewModel.PropertyChanged -= record;

        Assert.Equal(azure.Id, viewModel.ConnectedProfileId);
        Assert.False(viewModel.HasSelectedEntityDeliveryNote);
        Assert.Equal(string.Empty, viewModel.SelectedEntityDeliveryNote);
        Assert.Contains(nameof(MainWindowViewModel.HasSelectedEntityDeliveryNote), changed);
        Assert.Contains(nameof(MainWindowViewModel.BrowseDeliveryNote), changed);
    }

    [Fact]
    public void BrowseDeliveryNoteIsGeneralWithoutTopologyAndSilentWhereNothingIsCounted()
    {
        // Another environment's source: no topology at hand, so no number.
        Assert.Equal("Browsing on Amazon SQS counts as a receive: each look raises a message's receive count, and a redrive " +
                     "policy can move it to the dead-letter queue after its maximum receives.",
            MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.AmazonSqsSns, null, DeliveryQueue, ServiceBusSubQueue.Active));
        Assert.Equal("Browsing on Google Pub/Sub counts as a delivery: with a dead-letter policy, each look raises a message's " +
                     "delivery attempts and can forward it to the dead-letter topic.",
            MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.GooglePubSub, null, DeliverySubscription, ServiceBusSubQueue.Active));
        Assert.StartsWith("On RabbitMQ 4.2 and earlier, browsing a quorum queue counts as a delivery",
            MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.RabbitMq, null, DeliveryQueue, ServiceBusSubQueue.Active));
        Assert.Null(MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.AzureServiceBus, null, DeliveryQueue, ServiceBusSubQueue.DeadLetter));
        Assert.Null(MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.Kafka, null, DeliveryQueue, ServiceBusSubQueue.Active));

        // Known entities: a Pub/Sub subscription without a dead-letter policy and a classic RabbitMQ queue count nothing.
        var plain = DeliveryTopology(queue => queue with { MaxDeliveryCount = null, CountsRequeues = false }, maxDeliveryAttempts: null);
        Assert.Null(MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.GooglePubSub, plain, DeliverySubscription, ServiceBusSubQueue.Active));
        Assert.Null(MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.RabbitMq, plain, DeliveryQueue, ServiceBusSubQueue.Active));
        Assert.Equal("Browsing on Amazon SQS counts as a receive: each look raises a message's receive count.",
            MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.AmazonSqsSns, plain, DeliveryQueue, ServiceBusSubQueue.Active));

        // A quorum queue without its own limit gets RabbitMQ 4.0's default; one receive is singular.
        var quorum = DeliveryTopology(queue => queue with { MaxDeliveryCount = null, CountsRequeues = true });
        Assert.Contains("past its delivery limit (20 by default since RabbitMQ 4.0)",
            MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.RabbitMq, quorum, DeliveryQueue, ServiceBusSubQueue.Active));
        var once = DeliveryTopology(queue => queue with { MaxDeliveryCount = 1 });
        Assert.EndsWith("after 1 receive.",
            MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.AmazonSqsSns, once, DeliveryQueue, ServiceBusSubQueue.Active));
    }

    [Fact]
    public void TopologiesCarryTheDeliveryLimitThatReadingCountsToward()
    {
        var dlqArn = "arn:aws:sqs:us-east-1:1:orders-dlq";
        var orders = AwsQueueInfo.From("https://sqs.us-east-1.amazonaws.com/1/orders", new Dictionary<string, string>
        {
            ["QueueArn"] = "arn:aws:sqs:us-east-1:1:orders",
            ["RedrivePolicy"] = $$"""{"deadLetterTargetArn":"{{dlqArn}}","maxReceiveCount":4}"""
        });
        var legacy = AwsQueueInfo.From("https://sqs.us-east-1.amazonaws.com/1/legacy", new Dictionary<string, string>
        {
            ["QueueArn"] = "arn:aws:sqs:us-east-1:1:legacy",
            ["RedrivePolicy"] = $$"""{"deadLetterTargetArn":"{{dlqArn}}","maxReceiveCount":"9"}"""
        });
        var dlq = AwsQueueInfo.From("https://sqs.us-east-1.amazonaws.com/1/orders-dlq", new Dictionary<string, string> { ["QueueArn"] = dlqArn });
        var sqs = new AwsTopologyIndex([orders, legacy, dlq], []).ToTopology(DateTimeOffset.UnixEpoch);
        Assert.Equal(4, sqs.Queues.Single(queue => queue.Name == "orders").MaxDeliveryCount);
        Assert.Equal("orders-dlq", sqs.Queues.Single(queue => queue.Name == "orders").DeadLetterQueueName);
        Assert.Equal(9, sqs.Queues.Single(queue => queue.Name == "legacy").MaxDeliveryCount);
        Assert.Null(sqs.Queues.Single(queue => queue.Name == "orders-dlq").MaxDeliveryCount);
        Assert.Null(AwsQueueInfo.ReadMaxReceiveCount("not json"));

        const string project = "orders-prod-4821";
        var deadLetterTopic = new TopicName(project, "events-dlq").ToString();
        Subscription PubSub(string name, int? attempts) => new()
        {
            SubscriptionName = new SubscriptionName(project, name),
            Topic = new TopicName(project, "events").ToString(),
            State = Subscription.Types.State.Active,
            DeadLetterPolicy = attempts is { } value ? new DeadLetterPolicy { DeadLetterTopic = deadLetterTopic, MaxDeliveryAttempts = value } : null
        };
        var pubsub = GooglePubSubTopology.Build(project, ["events", "events-dlq"],
            [PubSub("billing", 12), PubSub("defaulted", 0), PubSub("plain", null)], DateTimeOffset.UnixEpoch).Topology;
        var events = pubsub.Topics.Single(topic => topic.Name == "events").Subscriptions;
        Assert.Equal(12, events.Single(item => item.Name == "billing").MaxDeliveryCount);
        Assert.Equal(5, events.Single(item => item.Name == "defaulted").MaxDeliveryCount);
        Assert.Null(events.Single(item => item.Name == "plain").MaxDeliveryCount);

        RabbitQueueInfo Rabbit(string json)
        {
            using var document = JsonDocument.Parse(json);
            return RabbitQueueInfo.From(document.RootElement);
        }
        var rabbit = new RabbitMqTopologyIndex(
        [
            Rabbit("""{"name":"limited","type":"quorum","arguments":{"x-delivery-limit":3,"x-dead-letter-exchange":"","x-dead-letter-routing-key":"parked"}}"""),
            Rabbit("""{"name":"defaulted","type":"quorum","arguments":{}}"""),
            Rabbit("""{"name":"unlimited","type":"quorum","arguments":{"x-delivery-limit":-1}}"""),
            Rabbit("""{"name":"parked","type":"classic","arguments":{}}""")
        ], [], []).ToTopology(DateTimeOffset.UnixEpoch);
        var limited = rabbit.Queues.Single(queue => queue.Name == "limited");
        Assert.True(limited.CountsRequeues);
        Assert.Equal(3, limited.MaxDeliveryCount);
        Assert.Equal("parked", limited.DeadLetterQueueName);
        Assert.True(rabbit.Queues.Single(queue => queue.Name == "defaulted").CountsRequeues);
        Assert.Null(rabbit.Queues.Single(queue => queue.Name == "defaulted").MaxDeliveryCount);
        Assert.False(rabbit.Queues.Single(queue => queue.Name == "unlimited").CountsRequeues);
        Assert.False(rabbit.Queues.Single(queue => queue.Name == "parked").CountsRequeues);
        // Reading "limited"'s dead letters reads the classic "parked" queue: nothing is counted there.
        Assert.Null(MainWindowViewModel.DescribeBrowseDelivery(MessagingProvider.RabbitMq, rabbit,
            ServiceBusEntityReference.Queue("limited"), ServiceBusSubQueue.DeadLetter));
    }

    private static ServiceBusTopology DeliveryTopology(
        Func<ServiceBusQueue, ServiceBusQueue> configureQueue,
        int? maxDeliveryAttempts = 7,
        params ServiceBusQueue[] extraQueues)
    {
        var queue = configureQueue(new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 2)))
        {
            MaxDeliveryCount = 5,
            CountsRequeues = true
        });
        var subscription = new ServiceBusSubscription("events", "billing", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 1)))
        {
            MaxDeliveryCount = maxDeliveryAttempts
        };
        return new ServiceBusTopology(DateTimeOffset.UtcNow, [queue, .. extraQueues],
            [new ServiceBusTopic("events", new ServiceBusEntityRuntime(ServiceBusMessageCounts.Empty), [subscription])]);
    }

    private static async Task<(MainWindowViewModel ViewModel, FakeWorkspace Workspace)> ConnectForDeliveryNoteAsync(
        MessagingProvider provider,
        Func<ServiceBusQueue, ServiceBusQueue>? configureQueue = null,
        params ServiceBusQueue[] extraQueues)
    {
        var profile = CreateProfile(provider.ToString(), EnvironmentKind.Test) with { Provider = provider };
        var workspace = new FakeWorkspace { Topology = DeliveryTopology(configureQueue ?? (queue => queue), 7, extraQueues) };
        workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow,
            [new DeadLetterEntitySnapshot(DeliveryQueue, 2), new DeadLetterEntitySnapshot(DeliverySubscription, 1)]);
        var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        return (viewModel, workspace);
    }
}
