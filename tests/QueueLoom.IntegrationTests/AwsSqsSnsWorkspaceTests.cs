using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Persistence;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.IntegrationTests;

/// <summary>Runs the SQS / SNS workspace against LocalStack.</summary>
public sealed class AwsSqsSnsWorkspaceTests : IAsyncLifetime
{
    private const string Region = "eu-west-1";
    private readonly TemporaryDirectory _directory = new();
    private readonly InMemorySecretVault _vault = new();
    private AmazonSQSClient _sqs = null!;
    private AmazonSimpleNotificationServiceClient _sns = null!;
    private AwsSqsSnsWorkspace _workspace = null!;
    private ServiceBusProfile _profile = null!;
    private string _orders = null!;
    private string _payments = null!;
    private string _deadLetters = null!;
    private string _topic = null!;
    private string _billing = null!;
    private string _billingDeadLetters = null!;

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.LocalStack)))
        {
            return;
        }

        var credentials = new BasicAWSCredentials("test", "test");
        _sqs = new AmazonSQSClient(credentials, new AmazonSQSConfig
        {
            ServiceURL = Emulators.LocalStackUrl,
            AuthenticationRegion = Region
        });
        _sns = new AmazonSimpleNotificationServiceClient(credentials, new AmazonSimpleNotificationServiceConfig
        {
            ServiceURL = Emulators.LocalStackUrl,
            AuthenticationRegion = Region
        });

        _deadLetters = Emulators.Unique("dlq");
        _orders = Emulators.Unique("orders");
        _payments = Emulators.Unique("payments");
        _topic = Emulators.Unique("events");
        _billing = Emulators.Unique("billing");
        _billingDeadLetters = Emulators.Unique("billing-dlq");

        var deadLetterArn = await CreateQueueAsync(_deadLetters);
        await CreateQueueAsync(_orders, deadLetterArn);
        await CreateQueueAsync(_payments, deadLetterArn);
        var billingArn = await CreateQueueAsync(_billing);
        var billingDeadLetterArn = await CreateQueueAsync(_billingDeadLetters);

        var topicArn = (await _sns.CreateTopicAsync(_topic)).TopicArn;
        var subscriptionArn = (await _sns.SubscribeAsync(new Sns.SubscribeRequest
        {
            TopicArn = topicArn,
            Protocol = "sqs",
            Endpoint = billingArn,
            ReturnSubscriptionArn = true
        })).SubscriptionArn;
        await _sns.SetSubscriptionAttributesAsync(new Sns.SetSubscriptionAttributesRequest
        {
            SubscriptionArn = subscriptionArn,
            AttributeName = "RedrivePolicy",
            AttributeValue = $$"""{"deadLetterTargetArn":"{{billingDeadLetterArn}}"}"""
        });
        await _sns.SetSubscriptionAttributesAsync(new Sns.SetSubscriptionAttributesRequest
        {
            SubscriptionArn = subscriptionArn,
            AttributeName = "RawMessageDelivery",
            AttributeValue = "true"
        });

        var profile = ServiceBusProfile.CreateNew(
                "LocalStack",
                EnvironmentKind.Development,
                new AuthenticationSettings(AuthenticationKind.AwsAccessKey),
                accessMode: ProfileAccessMode.ReadWrite)
            with
            {
                Provider = MessagingProvider.AmazonSqsSns,
                Aws = new AwsSettings(Region, Emulators.LocalStackUrl),
                AllowQueueManagement = true
            };
        _profile = profile;
        await _vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), new AwsAccessKey("test", "test").ToSecret());
        _workspace = new AwsSqsSnsWorkspace(_vault, backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)));
        await _workspace.ConnectAsync(profile);
    }

    public async ValueTask DisposeAsync()
    {
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        _sqs?.Dispose();
        _sns?.Dispose();
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Topology_shows_queues_dead_letter_queues_topics_and_subscriptions()
    {
        await DeadLetterAsync(_orders, "order 1042 failed");

        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);

        var orders = Assert.Single(topology.Queues, queue => queue.Name == _orders);
        Assert.True(orders.HasDeadLetterQueue);
        Assert.Equal(1, orders.Runtime.MessageCounts.DeadLetter);
        var deadLetters = Assert.Single(topology.Queues, queue => queue.Name == _deadLetters);
        Assert.False(deadLetters.HasDeadLetterQueue);
        Assert.Contains(_orders, deadLetters.Note);
        Assert.Contains(_payments, deadLetters.Note);
        Assert.False(topology.SupportsTransferDeadLetter);

        var topic = Assert.Single(topology.Topics, item => item.Name == _topic);
        var subscription = Assert.Single(topic.Subscriptions);
        Assert.Equal($"sqs:{_billing}", subscription.Name);
        Assert.True(subscription.HasDeadLetterQueue);

        var targets = DeadLetterSearchTargets.ForTopology(topology);
        Assert.DoesNotContain(targets, target => target.Source.Name == _deadLetters);
        Assert.DoesNotContain(targets, target => target.SubQueue == ServiceBusSubQueue.TransferDeadLetter);
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Browsing_a_shared_dead_letter_queue_shows_only_this_queues_messages_and_keeps_them()
    {
        await DeadLetterAsync(_orders, "order 1042 failed");
        await DeadLetterAsync(_payments, "payment 7 failed");
        await _workspace.GetTopologyAsync(forceRefresh: true);

        var orders = ServiceBusEntityReference.Queue(_orders);
        var first = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));
        var second = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));

        var message = Assert.Single(first);
        Assert.Equal("order 1042 failed", message.CreateDraft().Body.Content);
        Assert.Equal("Moved by the redrive policy", message.DeadLetterReason);
        Assert.Contains(_orders, message.DeadLetterErrorDescription);
        Assert.Equal(message.Properties.MessageId, Assert.Single(second).Properties.MessageId);
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Search_finds_dead_letters_by_body_text()
    {
        await DeadLetterAsync(_orders, "order 1042 failed");
        await DeadLetterAsync(_orders, "order 1043 failed");
        await DeadLetterAsync(_payments, "payment for order 1042 failed");
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
        var targets = DeadLetterSearchTargets.ForTopology(topology)
            .Where(target => target.Source.Name == _orders || target.Source.Name == _payments);

        var result = await _workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest("1042", targets));

        Assert.True(result.IsComplete);
        Assert.Equal(2, result.MatchCount);
        Assert.Contains(result.Matches, match => match.Source.Name == _orders);
        Assert.Contains(result.Matches, match => match.Source.Name == _payments);
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Deleting_selected_messages_backs_them_up_and_leaves_the_rest()
    {
        await DeadLetterAsync(_orders, "delete me");
        await DeadLetterAsync(_orders, "keep me");
        await _workspace.GetTopologyAsync(forceRefresh: true);
        var orders = ServiceBusEntityReference.Queue(_orders);
        var messages = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));
        var victim = messages.Single(message => message.CreateDraft().Body.Content == "delete me");

        var result = await _workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest(
            [new DeadLetterMessageKey(orders, ServiceBusSubQueue.DeadLetter, victim.SequenceNumber, victim.Properties.MessageId)]));

        Assert.Equal(1, result.DeletedCount);
        Assert.Single(Directory.GetFiles(result.BackupDirectory, "*.json", SearchOption.AllDirectories),
            file => !file.EndsWith("session.json", StringComparison.Ordinal));
        var remaining = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));
        Assert.Equal("keep me", Assert.Single(remaining).CreateDraft().Body.Content);
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Purging_empties_only_this_queues_dead_letters_after_backing_them_up()
    {
        await DeadLetterAsync(_orders, "a");
        await DeadLetterAsync(_orders, "b");
        await DeadLetterAsync(_payments, "c");
        await _workspace.GetTopologyAsync(forceRefresh: true);
        var orders = ServiceBusEntityReference.Queue(_orders);

        var result = await _workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([orders], [ServiceBusSubQueue.DeadLetter]));

        Assert.False(result.HasFailures);
        Assert.Equal(2, result.DeletedCount);
        Assert.Empty(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter)));
        Assert.Single(await _workspace.BrowseMessagesAsync(
            new BrowseMessagesRequest(ServiceBusEntityReference.Queue(_payments), ServiceBusSubQueue.DeadLetter)));
        // SQS counts per queue: the shared dead-letter queue still holds the payments message.
        var snapshot = await _workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.ForEntity(orders));
        Assert.Equal(1, Assert.Single(snapshot.Entities).Count);
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
        Assert.Contains("Shares dead-letter queue", topology.Queues.Single(queue => queue.Name == _orders).Note);
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Sent_properties_come_back_when_the_message_is_read()
    {
        await _workspace.GetTopologyAsync(forceRefresh: true);
        var orders = ServiceBusEntityReference.Queue(_orders);
        var draft = new MessageDraft(
            new EditableMessageBody("""{"orderId":1042}""", MessageBodyFormat.Json),
            new EditableMessageProperties(CorrelationId: "c-1", ContentType: "application/json", Subject: "OrderPlaced"),
            [
                new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "contoso"),
                new MessageApplicationProperty("attempt", ApplicationPropertyType.Int32, "3"),
                new MessageApplicationProperty("urgent", ApplicationPropertyType.Boolean, "True")
            ]);

        await _workspace.SendMessageAsync(new QueueLoom.Core.ServiceBus.SendMessageRequest(orders, draft));
        var message = Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders)));

        Assert.Equal("c-1", message.Properties.CorrelationId);
        Assert.Equal("application/json", message.Properties.ContentType);
        Assert.Equal("OrderPlaced", message.Properties.Subject);
        Assert.Equal(draft.ApplicationProperties.OrderBy(item => item.Name), message.ApplicationProperties);
        Assert.Equal(MessageBodyFormat.Json, message.CreateDraft().Body.Format);
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Publishing_to_a_topic_reaches_the_sqs_subscription()
    {
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
        var subscription = topology.Topics.Single(topic => topic.Name == _topic).Subscriptions.Single().Reference;

        await _workspace.SendMessageAsync(new QueueLoom.Core.ServiceBus.SendMessageRequest(
            ServiceBusEntityReference.Topic(_topic),
            new MessageDraft(new EditableMessageBody("invoice 5", MessageBodyFormat.Text))));
        var messages = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(subscription));

        Assert.Equal("invoice 5", Assert.Single(messages).CreateDraft().Body.Content);
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Fifo_queues_take_the_session_as_the_message_group()
    {
        var name = Emulators.Unique("invoices") + ".fifo";
        await _sqs.CreateQueueAsync(new CreateQueueRequest
        {
            QueueName = name,
            Attributes = new Dictionary<string, string> { ["FifoQueue"] = "true" }
        });
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
        Assert.Contains("FIFO", topology.Queues.Single(queue => queue.Name == name).Note);
        var queue = ServiceBusEntityReference.Queue(name);
        foreach (var (group, id) in new[] { ("customer-1", "a"), ("customer-1", "b"), ("customer-2", "c") })
        {
            await _workspace.SendMessageAsync(new QueueLoom.Core.ServiceBus.SendMessageRequest(queue, new MessageDraft(
                new EditableMessageBody(id, MessageBodyFormat.Text),
                new EditableMessageProperties(MessageId: id, SessionId: group))));
        }

        var messages = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(queue));

        Assert.Equal(["a", "b", "c"], messages.Select(message => message.CreateDraft().Body.Content).Order());
        Assert.Equal(["customer-1", "customer-1", "customer-2"], messages.Select(message => message.Properties.SessionId).Order());
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Moving_a_dead_letter_sends_it_back_and_removes_the_backed_up_original()
    {
        await DeadLetterAsync(_orders, "retry me");
        await DeadLetterAsync(_orders, "leave me");
        await _workspace.GetTopologyAsync(forceRefresh: true);
        var orders = ServiceBusEntityReference.Queue(_orders);
        var dead = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter));
        var retry = dead.Single(message => message.CreateDraft().Body.Content == "retry me");

        var result = await DeadLetterResender.ResendAsync(_workspace,
            [new ResendItem(retry, DeadLetterResender.OriginalDestination(retry.Source), retry.CreateDraft())], ResendMode.Move);

        Assert.Equal(ResendOutcome.Moved, Assert.Single(result.Items).Outcome);
        Assert.NotEmpty(Directory.GetFiles(result.BackupDirectory!, "0*.json", SearchOption.AllDirectories));
        Assert.Equal("retry me", Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(orders)))
            .CreateDraft().Body.Content);
        Assert.Equal("leave me", Assert.Single(await _workspace.BrowseMessagesAsync(
            new BrowseMessagesRequest(orders, ServiceBusSubQueue.DeadLetter))).CreateDraft().Body.Content);
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task A_read_only_environment_cannot_send()
    {
        await _workspace.GetTopologyAsync(forceRefresh: true);
        await _workspace.SetAccessModeAsync(ProfileAccessMode.ReadOnly);

        await Assert.ThrowsAsync<InvalidOperationException>(() => _workspace.SendMessageAsync(
            new QueueLoom.Core.ServiceBus.SendMessageRequest(
                ServiceBusEntityReference.Queue(_orders),
                new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text)))));
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Wrong_credentials_fail_to_connect_with_a_clear_error()
    {
        var profile = _profile with { Id = Guid.NewGuid(), Aws = new AwsSettings(Region, "http://localhost:1") };
        await _vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), new AwsAccessKey("test", "test").ToSecret());
        await using var workspace = new AwsSqsSnsWorkspace(_vault);

        await Assert.ThrowsAnyAsync<Exception>(() => workspace.ConnectAsync(profile));
        Assert.Equal(WorkspaceConnectionState.Faulted, workspace.ConnectionState);
    }

    private async Task<string> CreateQueueAsync(string name, string? deadLetterArn = null)
    {
        var request = new CreateQueueRequest { QueueName = name, Attributes = [] };
        if (deadLetterArn is not null)
        {
            request.Attributes["RedrivePolicy"] = $$"""{"deadLetterTargetArn":"{{deadLetterArn}}","maxReceiveCount":"1"}""";
        }
        var url = (await _sqs.CreateQueueAsync(request)).QueueUrl;
        var attributes = await _sqs.GetQueueAttributesAsync(url, ["QueueArn"]);
        return attributes.Attributes["QueueArn"];
    }

    /// <summary>Sends a message and receives it past maxReceiveCount so SQS moves it to the dead-letter queue.</summary>
    private async Task DeadLetterAsync(string queueName, string body)
    {
        var url = (await _sqs.GetQueueUrlAsync(queueName)).QueueUrl;
        await _sqs.SendMessageAsync(url, body);
        for (var attempt = 0; attempt < 10; attempt++)
        {
            var received = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = url,
                VisibilityTimeout = 0,
                WaitTimeSeconds = 1
            });
            if (received.Messages is null or { Count: 0 })
            {
                return;
            }
        }

        throw new InvalidOperationException("The message was not moved to the dead-letter queue.");
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Queues_are_created_with_a_dead_letter_queue_changed_and_deleted()
    {
        var name = Emulators.Unique("invoices");

        await _workspace.CreateQueueAsync(new QueueDefinition(name,
            new QueueSettings(TimeSpan.FromDays(2), MaxDeliveryCount: 4, LockDuration: TimeSpan.FromSeconds(45))));
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
        Assert.True(topology.Queues.Single(queue => queue.Name == name).HasDeadLetterQueue);
        Assert.Equal(new QueueSettings(TimeSpan.FromDays(2), 4, TimeSpan.FromSeconds(45)), await _workspace.GetQueueSettingsAsync(name));

        await _workspace.UpdateQueueSettingsAsync(name, new QueueSettings(MaxDeliveryCount: 9, LockDuration: TimeSpan.FromMinutes(5)));
        Assert.Equal(new QueueSettings(TimeSpan.FromDays(2), 9, TimeSpan.FromMinutes(5)), await _workspace.GetQueueSettingsAsync(name));

        await _workspace.DeleteQueueAsync(name);
        await _workspace.DeleteQueueAsync(name + "-dlq");
        Assert.DoesNotContain((await _workspace.GetTopologyAsync(forceRefresh: true)).Queues, queue => queue.Name.StartsWith(name, StringComparison.Ordinal));
    }

}
