using System.Reflection;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.Tests;

/// <summary>Provider limits the app applied wrongly or not at all (cycle 4).</summary>
public sealed class ProviderLimitRegressionTests
{
    // Finding 1: SetQueueAttributes accepts MessageRetentionPeriod 60 to 1,209,600 seconds and VisibilityTimeout 0 to
    // 43,200; a redrive policy's maxReceiveCount is 1 to 1,000. The settings dialog accepted any positive TTL and up to
    // 2,000 deliveries, and AwsSqsSnsWorkspace.Management clamped what it accepted: asking for 30 days quietly saved
    // 14, 1,500 receives quietly saved 1,000, and 30 seconds quietly saved 60.
    [Theory]
    [InlineData(30d, TimeUnit.Days, null)]
    [InlineData(0.5d, TimeUnit.Minutes, null)]
    [InlineData(null, TimeUnit.Days, 1500)]
    public void SqsQueueDialog_RefusesValuesSqsDoesNotAccept(double? timeToLive, TimeUnit unit, int? maxDeliveryCount)
    {
        var capabilities = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault()).QueueManagement!;
        var dialog = new QueueDialogViewModel(capabilities, "isolated")
        {
            Name = "orders", TimeToLive = timeToLive, TimeToLiveUnit = unit, MaxDeliveryCount = maxDeliveryCount
        };

        Assert.Null(dialog.TryBuildSettings());
        Assert.Contains("Amazon SQS", dialog.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void SqsQueueDialog_AcceptsTheWholeSqsRange()
    {
        var capabilities = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault()).QueueManagement!;
        var dialog = new QueueDialogViewModel(capabilities, "isolated")
        {
            Name = "orders", TimeToLive = 14, TimeToLiveUnit = TimeUnit.Days, MaxDeliveryCount = 1000, LockSeconds = 43_200
        };

        var settings = dialog.TryBuildSettings();

        Assert.NotNull(settings);
        Assert.Equal(TimeSpan.FromDays(14), settings.MessageTimeToLive);
    }

    [Theory]
    [InlineData(30 * 24 * 3600, null, "14 days")]
    [InlineData(30, null, "1 minute")]
    [InlineData(null, 1500, "1,000")]
    public async Task SqsUpdate_OutOfRangeValueIsRefusedNotClamped(int? retentionSeconds, int? maxReceiveCount, string limit)
    {
        using var sqs = new ManagementSqs();
        await using var workspace = SqsWorkspace(sqs);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.UpdateQueueSettingsAsync("orders",
            new QueueSettings(retentionSeconds is { } seconds ? TimeSpan.FromSeconds(seconds) : null, maxReceiveCount)));

        Assert.Contains(limit, error.Message, StringComparison.Ordinal);
        Assert.Empty(sqs.Changes);
    }

    [Fact]
    public async Task SqsCreate_OutOfRangeRetentionCreatesNothing()
    {
        using var sqs = new ManagementSqs();
        await using var workspace = SqsWorkspace(sqs);

        await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.CreateQueueAsync(
            new QueueDefinition("orders", new QueueSettings(TimeSpan.FromDays(30)), CreateDeadLetterQueue: true)));

        Assert.Empty(sqs.Created);
    }

    // Finding 2: the dialog allowed any lock from 0 seconds to 12 hours for every service. Azure Service Bus:
    // CreateQueueOptions.LockDuration "Max value is 5 minutes" (a zero lock fails in the SDK setter with an
    // ArgumentOutOfRangeException); Pub/Sub: ackDeadlineSeconds 10 to 600 and maxDeliveryAttempts 5 to 100. A 10-minute
    // Azure lock was only refused by the service, with its own error text, after Save.
    [Theory]
    [InlineData(301)]
    [InlineData(600)]
    [InlineData(0)]
    public void AzureQueueDialog_RefusesLockServiceBusDoesNotAccept(int lockSeconds)
    {
        var capabilities = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault()).QueueManagement!;
        var dialog = new QueueDialogViewModel(capabilities, "isolated", "orders", new QueueSettings(null, 10, TimeSpan.FromMinutes(1), false))
        {
            LockSeconds = lockSeconds
        };

        Assert.Null(dialog.TryBuildSettings());
        Assert.Contains("Azure Service Bus", dialog.Error, StringComparison.Ordinal);
        Assert.Contains("5 minutes", dialog.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void AzureQueueDialog_AcceptsFiveMinuteLock()
    {
        var capabilities = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault()).QueueManagement!;
        var dialog = new QueueDialogViewModel(capabilities, "isolated", "orders", new QueueSettings(null, 10, TimeSpan.FromMinutes(1), false))
        {
            LockSeconds = 300
        };

        Assert.Equal(TimeSpan.FromMinutes(5), dialog.TryBuildSettings()?.LockDuration);
    }

    [Theory]
    [InlineData(700, null)]
    [InlineData(5, null)]
    [InlineData(null, 3)]
    [InlineData(null, 150)]
    public void PubSubSubscriptionDialog_RefusesValuesPubSubDoesNotAccept(int? ackDeadline, int? deliveryAttempts)
    {
        var capabilities = new GooglePubSubWorkspace(new DeepAuditCloudTests.EmptyVault()).QueueManagement!;
        var dialog = new QueueDialogViewModel(capabilities, "isolated", "orders", new QueueSettings(null, 5, TimeSpan.FromSeconds(10)))
        {
            LockSeconds = ackDeadline ?? 10,
            MaxDeliveryCount = deliveryAttempts ?? 5
        };

        Assert.Null(dialog.TryBuildSettings());
        Assert.Contains("Google Pub/Sub", dialog.Error, StringComparison.Ordinal);
    }

    // Finding 3: MessageDraftValidator held every non-Kafka service to Azure's 128-character identifiers. A Pub/Sub
    // ordering key (SessionId) "can be up to 1 KB in length", so a 200-character key was refused on resend; a RabbitMQ
    // message-id is an AMQP short string (255 bytes of UTF-8), so a 200-character ASCII id was refused while 128
    // Cyrillic letters (256 bytes) passed and then failed inside the AMQP client.
    [Theory]
    [InlineData(MessagingProvider.GooglePubSub, "session", 200, 'k', true)]
    [InlineData(MessagingProvider.GooglePubSub, "session", 1024, 'k', true)]
    [InlineData(MessagingProvider.GooglePubSub, "session", 513, 'я', false)]
    [InlineData(MessagingProvider.RabbitMq, "message", 200, 'm', true)]
    [InlineData(MessagingProvider.RabbitMq, "message", 255, 'm', true)]
    [InlineData(MessagingProvider.RabbitMq, "message", 128, 'я', false)]
    [InlineData(MessagingProvider.AzureServiceBus, "session", 129, 'k', false)]
    [InlineData(MessagingProvider.AmazonSqsSns, "session", 129, 'k', false)]
    public void Identifiers_FollowTheDestinationServicesLimit(MessagingProvider provider, string field, int length, char character, bool valid)
    {
        var value = new string(character, length);
        var properties = field == "session"
            ? new EditableMessageProperties(MessageId: "id-1", SessionId: value)
            : new EditableMessageProperties(MessageId: value);

        var result = MessageDraftValidator.Validate(new MessageDraft(new EditableMessageBody("{}", MessageBodyFormat.Json), properties), provider);

        Assert.Equal(valid, result.IsValid);
    }

    // Finding 4: SQS and SNS attribute names may only contain A-Z, a-z, 0-9, '_', '-' and '.' (no leading, trailing or
    // doubled period, up to 256 characters); SQS also reserves "AWS." and "Amazon." and allows 10 attributes per
    // message. QueueLoom adds the correlation ID, subject, content type, reply-to and to as attributes, so a message
    // with 8 application properties reaches the SDK with 11 attributes. None of this was checked before the SDK call.
    [Theory]
    [InlineData("order id")]
    [InlineData(".hidden")]
    [InlineData("a..b")]
    public void SqsAttributeName_IsCheckedByTheValidator(string name)
    {
        var draft = new MessageDraft(EditableMessageBody.Empty, EditableMessageProperties.Empty,
            [new MessageApplicationProperty(name, ApplicationPropertyType.String, "1")]);

        var result = MessageDraftValidator.Validate(draft, MessagingProvider.AmazonSqsSns);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, error => error.Message.Contains("Amazon SQS and SNS", StringComparison.Ordinal));
    }

    [Fact]
    public async Task SqsSend_ElevenAttributesAreRefusedBeforeTheSdk()
    {
        using var fixture = new AwsSendFixture();
        var draft = new MessageDraft(EditableMessageBody.Empty,
            new EditableMessageProperties(MessageId: "m-1", CorrelationId: "c-1", ContentType: "application/json", Subject: "orders"),
            Enumerable.Range(1, 8).Select(index => new MessageApplicationProperty($"p{index}", ApplicationPropertyType.String, "v")));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Send(ServiceBusEntityReference.Queue("orders"), draft));

        Assert.Contains("at most 10", error.Message, StringComparison.Ordinal);
        Assert.Empty(fixture.Sqs.Sends);
    }

    [Fact]
    public async Task SqsSend_ReservedAttributePrefixIsRefusedBeforeTheSdk()
    {
        using var fixture = new AwsSendFixture();
        var draft = new MessageDraft(EditableMessageBody.Empty, new EditableMessageProperties(MessageId: "m-1"),
            [new MessageApplicationProperty("aws.trace", ApplicationPropertyType.String, "v")]);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Send(ServiceBusEntityReference.Queue("orders"), draft));

        Assert.Contains("AWS.", error.Message, StringComparison.Ordinal);
        Assert.Empty(fixture.Sqs.Sends);
    }

    [Fact]
    public async Task SqsFifoSend_DeduplicationIdWithSpaceIsRefusedBeforeTheSdk()
    {
        using var fixture = new AwsSendFixture();
        var draft = new MessageDraft(EditableMessageBody.Empty, new EditableMessageProperties(MessageId: "order 42", SessionId: "group"));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Send(ServiceBusEntityReference.Queue("orders.fifo"), draft));

        Assert.Contains("MessageDeduplicationId", error.Message, StringComparison.Ordinal);
        Assert.Empty(fixture.Sqs.Sends);
    }

    [Fact]
    public async Task SnsPublish_KeepsMobilePushAttributesAndMoreThanTenAttributes()
    {
        using var fixture = new AwsSendFixture();
        var draft = new MessageDraft(EditableMessageBody.Empty, new EditableMessageProperties(MessageId: "m-1", CorrelationId: "c-1"),
            Enumerable.Range(1, 10).Select(index => new MessageApplicationProperty($"p{index}", ApplicationPropertyType.String, "v"))
                .Append(new MessageApplicationProperty("AWS.SNS.MOBILE.APNS.TTL", ApplicationPropertyType.String, "60")));

        await fixture.Send(ServiceBusEntityReference.Topic("events"), draft);

        Assert.Equal(12, Assert.Single(fixture.Sns.Sends).MessageAttributes.Count);
    }

    private static AwsSqsSnsWorkspace SqsWorkspace(ManagementSqs sqs)
    {
        var workspace = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(AwsSqsSnsWorkspace).GetField("_sqs", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, sqs);
        typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
            ViewModelStateTests.CreateProfile("isolated", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with
            {
                Provider = MessagingProvider.AmazonSqsSns, AllowQueueManagement = true
            });
        return workspace;
    }

    private sealed class ManagementSqs() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public List<SetQueueAttributesRequest> Changes { get; } = [];
        public List<CreateQueueRequest> Created { get; } = [];

        public override Task<GetQueueUrlResponse> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken = default) =>
            Task.FromResult(new GetQueueUrlResponse { QueueUrl = "https://fake.invalid/" + queueName });

        public override Task<GetQueueAttributesResponse> GetQueueAttributesAsync(GetQueueAttributesRequest request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new GetQueueAttributesResponse
            {
                Attributes = new Dictionary<string, string>
                {
                    ["QueueArn"] = "arn:aws:sqs:us-east-1:0:orders-dlq",
                    ["RedrivePolicy"] = "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:0:orders-dlq\",\"maxReceiveCount\":5}"
                }
            });

        public override Task<SetQueueAttributesResponse> SetQueueAttributesAsync(SetQueueAttributesRequest request,
            CancellationToken cancellationToken = default)
        {
            Changes.Add(request);
            return Task.FromResult(new SetQueueAttributesResponse());
        }

        public override Task<CreateQueueResponse> CreateQueueAsync(CreateQueueRequest request, CancellationToken cancellationToken = default)
        {
            Created.Add(request);
            return Task.FromResult(new CreateQueueResponse { QueueUrl = "https://fake.invalid/" + request.QueueName });
        }
    }

    private sealed class AwsSendFixture : IDisposable
    {
        private readonly AwsSqsSnsWorkspace _workspace = new(new DeepAuditCloudTests.EmptyVault());
        public SendSqs Sqs { get; } = new();
        public SendSns Sns { get; } = new();

        public AwsSendFixture()
        {
            void Set(string name, object value) =>
                typeof(AwsSqsSnsWorkspace).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(_workspace, value);
            Set("_sqs", Sqs);
            Set("_sns", Sns);
            Set("_index", new AwsTopologyIndex(new[] { "orders", "orders.fifo" }.Select(name =>
                    AwsQueueInfo.From("https://fake.invalid/" + name, new Dictionary<string, string> { ["QueueArn"] = "arn:aws:sqs:us-east-1:0:" + name })),
                new[] { "events", "events.fifo" }.Select(name => AwsTopicInfo.From("arn:aws:sns:us-east-1:0:" + name, []))));
        }

        public Task Send(ServiceBusEntityReference target, MessageDraft draft) => (Task)typeof(AwsSqsSnsWorkspace)
            .GetMethod("SendCoreAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(_workspace,
                [new ServiceBusTopology(DateTimeOffset.UtcNow), target, draft, CancellationToken.None])!;

        public void Dispose()
        {
            Sqs.Dispose();
            Sns.Dispose();
        }
    }

    internal sealed class SendSqs() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public List<Amazon.SQS.Model.SendMessageRequest> Sends { get; } = [];

        public override Task<SendMessageResponse> SendMessageAsync(Amazon.SQS.Model.SendMessageRequest request, CancellationToken cancellationToken = default)
        {
            Sends.Add(request);
            return Task.FromResult(new SendMessageResponse());
        }
    }

    internal sealed class SendSns() : AmazonSimpleNotificationServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public List<Sns.PublishRequest> Sends { get; } = [];

        public override Task<Sns.PublishResponse> PublishAsync(Sns.PublishRequest request, CancellationToken cancellationToken = default)
        {
            Sends.Add(request);
            return Task.FromResult(new Sns.PublishResponse());
        }
    }
}

public sealed partial class ViewModelStateTests
{
    // Finding 3 (call sites): resending a Pub/Sub dead letter whose ordering key is 200 characters (well within
    // Pub/Sub's 1 KB) was refused with "SessionId cannot exceed 128 characters" - first by the resend check, then by the
    // durable operation store, which validated without knowing the destination service at all.
    [Fact]
    public async Task PubSubResend_LongOrderingKeyWithinPubSubLimitIsResent()
    {
        using var directory = new TemporaryDirectory();
        var orderingKey = string.Concat(Enumerable.Repeat("tenant-42/customer-1001/", 9));
        Assert.InRange(orderingKey.Length, 129, 1024);
        var profile = CreateProfile("PubSub", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.GooglePubSub };
        var workspace = new FakeWorkspace();
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            new FakeDialogService { ConfirmResult = true }, replayStore: store);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 7, "{}"u8.ToArray(),
            new EditableMessageProperties(MessageId: "pubsub-1", SessionId: orderingKey));
        vm.Messages.Add(new MessageItemViewModel(message, profile.Id) { IsMarked = true });

        await vm.ResendMarkedMessagesCommand.ExecuteAsync();

        Assert.Equal(string.Empty, vm.ErrorText);
        Assert.Equal(orderingKey, Assert.Single(workspace.SentMessages).Message.Properties.SessionId);
    }

    // Finding 3 (composer): the composer validated nothing for SQS, Pub/Sub or RabbitMQ, so a RabbitMQ MessageId of
    // 300 bytes went to the AMQP client, which refuses short strings over 255 bytes with an error that does not name
    // the field. It is now refused before the confirmation, naming the limit.
    [Fact]
    public async Task RabbitComposer_MessageIdOverTheShortStringLimitIsRefusedBeforeSending()
    {
        var profile = CreateProfile("Rabbit", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.RabbitMq };
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.Destinations.Add(new DestinationItemViewModel(ServiceBusEntityReference.Queue("orders")));
        vm.NewMessageCommand.Execute(null);
        vm.SelectedDestination = vm.Destinations.Last();
        vm.DraftMessageId = new string('m', 300);

        await vm.SendDraftCommand.ExecuteAsync();

        Assert.Empty(workspace.SentMessages);
        Assert.Contains("255 bytes", vm.ErrorText, StringComparison.Ordinal);
    }
}
