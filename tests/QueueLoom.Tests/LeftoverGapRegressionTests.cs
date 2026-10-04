using System.Reflection;
using System.Text;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using Azure.Messaging.ServiceBus;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Mcp;
using QueueLoom.Tests.Infrastructure;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.Tests;

/// <summary>Gaps left open by cycle 4: total message size, the search window, export names, wire types, SNS subjects.</summary>
public sealed class LeftoverGapRegressionTests
{
    // ---- Finding 1: total message size ------------------------------------------------------------------------------

    // SendMessage: "The maximum size is 1 MiB or 1,048,576 bytes", and "All components of a message attribute are
    // included in the 1 MiB message size restriction". One attribute "p" = "v" of type String counts 1 + 6 + 1 bytes.
    [Theory]
    [InlineData(1_048_568, true)]
    [InlineData(1_048_569, false)]
    public void SqsSnsDraft_TotalSizeIncludingAttributesIsCheckedByTheValidator(int bodyLength, bool valid)
    {
        var draft = new MessageDraft(new EditableMessageBody(new string('a', bodyLength), MessageBodyFormat.Text),
            EditableMessageProperties.Empty, [new MessageApplicationProperty("p", ApplicationPropertyType.String, "v")]);

        var result = MessageDraftValidator.Validate(draft, MessagingProvider.AmazonSqsSns);

        Assert.Equal(valid, result.IsValid);
        if (!valid)
        {
            Assert.Contains("1 MiB (1,048,576 bytes)", Assert.Single(result.Errors).Message, StringComparison.Ordinal);
        }
    }

    // Pub/Sub: a publish request is at most 10 MB (10,485,760 bytes) with the data, attributes and ordering key.
    [Fact]
    public void PubSubDraft_OverTheTenMegabytePublishRequestIsRefusedByTheValidator()
    {
        var draft = new MessageDraft(new EditableMessageBody(new string('a', 10_485_700), MessageBodyFormat.Text),
            new EditableMessageProperties(SessionId: new string('k', 100)), []);

        var result = MessageDraftValidator.Validate(draft, MessagingProvider.GooglePubSub);

        Assert.False(result.IsValid);
        Assert.Contains("10 MB (10,485,760 bytes)", Assert.Single(result.Errors).Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SqsSend_QueueMaximumMessageSizeIsCheckedBeforeTheSdk()
    {
        using var fixture = new AwsSendFixture();
        var draft = new MessageDraft(new EditableMessageBody(new string('a', 2_000), MessageBodyFormat.Text), EditableMessageProperties.Empty);

        var error = await Assert.ThrowsAsync<DeliveryRejectedException>(() => fixture.Send(ServiceBusEntityReference.Queue("small"), draft));

        Assert.Contains("1,024 bytes", error.Message, StringComparison.Ordinal);
        Assert.Empty(fixture.Sqs.Sends);
    }

    // SNS: "All parts of the message attribute, including name, type, and value, are included in the message size
    // restriction, which is the topic's configured MaximumMessageSize (default 262,144 bytes)".
    [Fact]
    public async Task SnsPublish_OverTheTopicsMaximumMessageSizeIsRefusedBeforeTheSdk()
    {
        using var fixture = new AwsSendFixture();
        var draft = new MessageDraft(new EditableMessageBody(new string('a', 262_000), MessageBodyFormat.Text),
            new EditableMessageProperties(CorrelationId: new string('c', 200)));

        var error = await Assert.ThrowsAsync<DeliveryRejectedException>(() => fixture.Send(ServiceBusEntityReference.Topic("events"), draft));

        Assert.Contains("262,144 bytes", error.Message, StringComparison.Ordinal);
        Assert.Empty(fixture.Sns.Sends);
    }

    [Fact]
    public async Task SnsPublish_TopicRaisedToOneMebibyteTakesALargerMessage()
    {
        using var fixture = new AwsSendFixture();
        var draft = new MessageDraft(new EditableMessageBody(new string('a', 600_000), MessageBodyFormat.Text), EditableMessageProperties.Empty);

        await fixture.Send(ServiceBusEntityReference.Topic("large-events"), draft);

        Assert.Single(fixture.Sns.Sends);
    }

    // A message the namespace cannot take is never sent; the durable resend must record that as a proven rejection
    // ("Retry proven failures"), not as an uncertain delivery that blocks the batch for manual inspection.
    [Fact]
    public async Task AzureSend_MessageTheEntityCannotTakeIsAProvenRejection()
    {
        await using var workspace = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        var client = new BatchRefusingClient();
        typeof(AzureServiceBusWorkspace).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, client);
        typeof(AzureServiceBusWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
            ViewModelStateTests.CreateProfile("Isolated", EnvironmentKind.Test, ProfileAccessMode.ReadWrite));
        var draft = new MessageDraft(new EditableMessageBody(new string('a', 300_000), MessageBodyFormat.Text), new EditableMessageProperties(MessageId: "m-1"));

        var error = await Assert.ThrowsAsync<DeliveryRejectedException>(() =>
            workspace.SendMessageAsync(new QueueLoom.Core.ServiceBus.SendMessageRequest(ServiceBusEntityReference.Queue("orders"), draft)));

        Assert.Contains("262,144 bytes", error.Message, StringComparison.Ordinal);
        Assert.Equal(0, client.Sender.Sent);
    }

    // ---- Finding 2: the "enqueued within" window is applied before the result cap -----------------------------------

    private static readonly DateTimeOffset Now = DateTimeOffset.UtcNow;

    [Fact]
    public async Task Search_LeasedWindowIsAppliedBeforeTheResultCap()
    {
        using var directory = new TemporaryDirectory();
        // Dead letters come oldest first: three old matches, then the one inside the window.
        await using var workspace = new WindowWorkspace(directory.Path,
        [
            (Now.AddDays(-2), "needle old 1"), (Now.AddDays(-2), "needle old 2"), (Now.AddDays(-1), "needle old 3"),
            (Now.AddMinutes(-5), "needle recent")
        ]);
        await workspace.ConnectAsync(ServiceBusProfile.CreateNew("Isolated Rabbit", EnvironmentKind.Development,
            new(AuthenticationKind.RabbitMqPassword)) with { Provider = MessagingProvider.RabbitMq });
        var target = new DeadLetterSearchTarget(WindowWorkspace.Source, ServiceBusSubQueue.DeadLetter, KnownMessageCount: 4);

        var result = await workspace.SearchDeadLettersAsync(
            new DeadLetterSearchRequest("needle", [target], maximumResults: 2) { EnqueuedSince = Now.AddMinutes(-15) });

        Assert.Equal("needle recent", Encoding.UTF8.GetString(Assert.Single(result.Matches).Body.Span));
        Assert.False(result.ResultLimitReached);
    }

    [Fact]
    public async Task Search_AzureWindowIsAppliedBeforeTheResultCap()
    {
        var source = ServiceBusEntityReference.Queue("orders");
        var target = new DeadLetterSearchTarget(source, ServiceBusSubQueue.DeadLetter, KnownMessageCount: 4);
        var request = new DeadLetterSearchRequest("needle", [target], batchSize: 10, maximumMessagesPerTarget: 10, maximumResults: 2)
        {
            EnqueuedSince = Now.AddMinutes(-15)
        };
        ServiceBusReceivedMessage[] page =
        [
            Received(1, Now.AddDays(-2)), Received(2, Now.AddDays(-2)), Received(3, Now.AddDays(-1)), Received(4, Now.AddMinutes(-5))
        ];
        var accepted = 0;

        var result = await AzureServiceBusWorkspace.SearchTargetPagesAsync(
            target,
            request,
            (_, from, _) => Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(from is null ? page : []),
            // The shared cap of SearchDeadLettersAsync: the first MaximumResults matches are kept.
            message => ++accepted <= request.MaximumResults ? AzureMessageMapper.FromAzure(message, source, ServiceBusSubQueue.DeadLetter) : null,
            shouldStop: () => false,
            CancellationToken.None);

        Assert.Equal(4, Assert.Single(result.Matches).SequenceNumber);
    }

    private static ServiceBusReceivedMessage Received(long sequenceNumber, DateTimeOffset enqueuedAt) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("needle"), messageId: $"m-{sequenceNumber}",
            sequenceNumber: sequenceNumber, enqueuedTime: enqueuedAt);

    // ---- Finding 3: MCP export file names are reserved atomically ---------------------------------------------------

    // ExportPath checked File.Exists and the export moved its file in with overwrite later, so two exports choosing a
    // name in the same second both got "found.json" and the second replaced the first.
    [Fact]
    public void McpExport_TwoExportsChoosingANameAtOnceGetDifferentFiles()
    {
        var directory = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "mcp-race", Guid.NewGuid().ToString("N"));
        try
        {
            var tools = new QueueLoomReadTools(null!, new McpServerSettings(ExportDirectory: directory), null!);
            var exportPath = typeof(QueueLoomReadTools).GetMethod("ExportPath", BindingFlags.Instance | BindingFlags.NonPublic)!;

            var first = (string)exportPath.Invoke(tools, ["Development", "search", "found", ".json"])!;
            var second = (string)exportPath.Invoke(tools, ["Development", "search", "found", ".json"])!;

            Assert.NotEqual(first, second);
            Assert.EndsWith("found.json", first, StringComparison.Ordinal);
            Assert.EndsWith("found (2).json", second, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
        }
    }

    // ---- Finding 4: contradictory wire types are refused ------------------------------------------------------------

    [Theory]
    [InlineData(ApplicationPropertyType.String, "hello", "Number.x")]
    [InlineData(ApplicationPropertyType.Binary, "AQID", "String.x")]
    [InlineData(ApplicationPropertyType.Binary, "AQID", "Binary.png")]
    [InlineData(ApplicationPropertyType.Int32, "7", "String.Array")]
    [InlineData(ApplicationPropertyType.Boolean, "true", "Number.flag")]
    [InlineData(ApplicationPropertyType.String, "v", "Custom.label")]
    [InlineData(ApplicationPropertyType.String, "v", "String")]
    [InlineData(ApplicationPropertyType.String, "v", "String.")]
    public void WireType_ContradictingTheTypeIsRefused(ApplicationPropertyType type, string value, string wireType)
    {
        var draft = new MessageDraft(EditableMessageBody.Empty, EditableMessageProperties.Empty,
            [new MessageApplicationProperty("p", type, value) { WireType = wireType }]);

        var result = MessageDraftValidator.Validate(draft, MessagingProvider.AmazonSqsSns);

        Assert.False(result.IsValid);
        Assert.Contains("wireType", Assert.Single(result.Errors).Message, StringComparison.Ordinal);
    }

    // What QueueLoom itself reads from SQS and SNS stays sendable: String.Array, another producer's labels and a Number
    // too large for decimal (SQS allows up to 10^126), which is kept as text.
    [Theory]
    [InlineData(ApplicationPropertyType.String, """["blue","green"]""", "String.Array")]
    [InlineData(ApplicationPropertyType.String, "contoso", "String.customer")]
    [InlineData(ApplicationPropertyType.Int64, "7", "Number.1")]
    [InlineData(ApplicationPropertyType.Decimal, "7.5", "Number.float")]
    [InlineData(ApplicationPropertyType.String, "1e100", "Number.big")]
    public void WireType_ReadFromAwsStaysValid(ApplicationPropertyType type, string value, string wireType)
    {
        var draft = new MessageDraft(EditableMessageBody.Empty, EditableMessageProperties.Empty,
            [new MessageApplicationProperty("p", type, value) { WireType = wireType }]);

        Assert.True(MessageDraftValidator.Validate(draft, MessagingProvider.AmazonSqsSns).IsValid);
    }

    // ---- Finding 5: the SNS Subject survives an envelope read and a resend to the topic -----------------------------

    private const string EnvelopeWithSubject = """
        {
          "Type": "Notification",
          "MessageId": "dc1e94d9-56c5-5e96-808d-cc7f68faa162",
          "TopicArn": "arn:aws:sns:us-east-1:123:events",
          "Subject": "Order 7 shipped",
          "Message": "{\"orderId\":7}",
          "Timestamp": "2026-10-01T10:00:00.000Z",
          "MessageAttributes": { "tenant": { "Type": "String", "Value": "contoso" } }
        }
        """;

    [Fact]
    public async Task SnsEnvelopeSubject_IsPublishedAgainWhenResentToTheTopic()
    {
        var message = new Message { MessageId = "sqs-1", ReceiptHandle = "r-1", Body = EnvelopeWithSubject };
        var browsed = AwsMessageMapper.FromSqs(message, ServiceBusEntityReference.Subscription("events", "billing"),
            ServiceBusSubQueue.DeadLetter, snsEnvelope: true);

        Assert.Equal("Order 7 shipped", browsed.Properties.Subject);
        Assert.Equal("tenant", Assert.Single(browsed.ApplicationProperties).Name);

        using var fixture = new AwsSendFixture();
        await fixture.Send(ServiceBusEntityReference.Topic("events"), browsed.CreateDraft());
        Assert.Equal("Order 7 shipped", Assert.Single(fixture.Sns.Sends).Subject);
    }

    [Fact]
    public void SnsEnvelopeSubject_DoesNotReplaceAPublishedSubjectAttribute()
    {
        var body = EnvelopeWithSubject.Replace(
            "\"tenant\": { \"Type\": \"String\", \"Value\": \"contoso\" }",
            "\"Subject\": { \"Type\": \"String\", \"Value\": \"from attribute\" }", StringComparison.Ordinal);
        var message = new Message { MessageId = "sqs-1", ReceiptHandle = "r-1", Body = body };

        var browsed = AwsMessageMapper.FromSqs(message, ServiceBusEntityReference.Subscription("events", "billing"),
            ServiceBusSubQueue.Active, snsEnvelope: true);

        Assert.Equal("from attribute", browsed.Properties.Subject);
    }

    // Publish Subject: "UTF-8 text with no line breaks or control characters, and less than 100 characters long".
    [Theory]
    [InlineData("two\nlines")]
    [InlineData("tab\there")]
    public async Task SnsPublish_SubjectSnsWouldRefuseStaysAnAttributeOnly(string subject)
    {
        using var fixture = new AwsSendFixture();
        var draft = new MessageDraft(new EditableMessageBody("{}", MessageBodyFormat.Json), new EditableMessageProperties(Subject: subject));

        await fixture.Send(ServiceBusEntityReference.Topic("events"), draft);

        var request = Assert.Single(fixture.Sns.Sends);
        Assert.Null(request.Subject);
        Assert.Equal(subject, request.MessageAttributes[MessageAttributeConventions.Subject].StringValue);
    }

    [Fact]
    public async Task SnsPublish_HundredCharacterSubjectStaysAnAttributeOnly()
    {
        using var fixture = new AwsSendFixture();
        var draft = new MessageDraft(new EditableMessageBody("{}", MessageBodyFormat.Json),
            new EditableMessageProperties(Subject: new string('s', 100)));

        await fixture.Send(ServiceBusEntityReference.Topic("events"), draft);

        Assert.Null(Assert.Single(fixture.Sns.Sends).Subject);
    }

    // ---- Fixtures ---------------------------------------------------------------------------------------------------

    private sealed class AwsSendFixture : IDisposable
    {
        private readonly AwsSqsSnsWorkspace _workspace = new(new DeepAuditCloudTests.EmptyVault());
        public ProviderLimitRegressionTests.SendSqs Sqs { get; } = new();
        public ProviderLimitRegressionTests.SendSns Sns { get; } = new();

        public AwsSendFixture()
        {
            void Set(string name, object value) =>
                typeof(AwsSqsSnsWorkspace).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(_workspace, value);
            Set("_sqs", Sqs);
            Set("_sns", Sns);
            AwsQueueInfo Queue(string name, string? maximumMessageSize) => AwsQueueInfo.From("https://fake.invalid/" + name,
                maximumMessageSize is null
                    ? new Dictionary<string, string> { ["QueueArn"] = "arn:aws:sqs:us-east-1:0:" + name }
                    : new Dictionary<string, string> { ["QueueArn"] = "arn:aws:sqs:us-east-1:0:" + name, ["MaximumMessageSize"] = maximumMessageSize });
            Set("_index", new AwsTopologyIndex(
                [Queue("orders", null), Queue("small", "1024")],
                [
                    AwsTopicInfo.From("arn:aws:sns:us-east-1:0:events", []) with { MaximumMessageSize = MessageSizeLimits.AmazonSnsDefaultMaximumBytes },
                    AwsTopicInfo.From("arn:aws:sns:us-east-1:0:large-events", []) with { MaximumMessageSize = MessageSizeLimits.AmazonMaximumBytes }
                ]));
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

    private sealed class BatchRefusingClient : ServiceBusClient
    {
        public BatchRefusingSender Sender { get; } = new();
        public override ServiceBusSender CreateSender(string queueOrTopicName) => Sender;
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class BatchRefusingSender : ServiceBusSender
    {
        public int Sent { get; private set; }

        public override ValueTask<ServiceBusMessageBatch> CreateMessageBatchAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(ServiceBusModelFactory.ServiceBusMessageBatch(0, [],
                new CreateMessageBatchOptions { MaxSizeInBytes = 262_144 }, tryAddCallback: _ => false));

        public override Task SendMessagesAsync(ServiceBusMessageBatch messageBatch, CancellationToken cancellationToken = default)
        {
            Sent++;
            return Task.CompletedTask;
        }

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class WindowWorkspace(string root, (DateTimeOffset EnqueuedAt, string Body)[] messages)
        : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(root)), null)
    {
        public static readonly ServiceBusEntityReference Source = ServiceBusEntityReference.Queue("orders");
        private readonly BrowsedMessage[] _messages = messages.Select((message, index) => new BrowsedMessage(Source,
            ServiceBusSubQueue.DeadLetter, index + 1, Encoding.UTF8.GetBytes(message.Body),
            new EditableMessageProperties(MessageId: $"m-{index}"), enqueuedAt: message.EnqueuedAt)).ToArray();
        public override MessagingProvider Provider => MessagingProvider.RabbitMq;
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(
            new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty)]));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source,
            ServiceBusSubQueue subQueue) => new Channel(_messages);
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference target, MessageDraft message,
            CancellationToken token) => throw new NotSupportedException();

        private sealed class Channel(BrowsedMessage[] messages) : ILeasedMessageChannel
        {
            private int _next;
            public string PhysicalName => "orders-dlq";
            public int MaximumBatchSize => 10;
            public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken token)
            {
                var batch = messages.Skip(_next).Take(maxMessages).ToArray();
                _next += batch.Length;
                return Task.FromResult<IReadOnlyList<LeasedMessage>>(batch.Select(message =>
                    new LeasedMessage(message, message.Properties.MessageId!) { DeliveryIdentity = message.Properties.MessageId! }).ToArray());
            }
            public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> released, CancellationToken token) => Task.CompletedTask;
            public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> settled, CancellationToken token) =>
                throw new NotSupportedException();
        }
    }
}

public sealed partial class ViewModelStateTests
{
    // Finding 1 (scheduled resend): a copy SQS can never accept was scheduled anyway; the size is now part of the
    // validation that runs before anything is sent or scheduled.
    [Fact]
    public async Task ScheduledResend_OversizedSqsMessageIsRefusedBeforeItIsScheduled()
    {
        var profile = CreateProfile("Aws", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.AmazonSqsSns };
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true, ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddHours(1) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 7,
            Encoding.UTF8.GetBytes(new string('a', 1_100_000)), new EditableMessageProperties(MessageId: "sqs-1"));
        vm.Messages.Add(new MessageItemViewModel(message, profile.Id) { IsMarked = true });

        await vm.ResendMarkedMessagesCommand.ExecuteAsync();

        Assert.Empty(vm.ScheduledResends);
        Assert.Empty(workspace.SentMessages);
        Assert.Contains("1 MiB", vm.ErrorText, StringComparison.Ordinal);
    }

    // Finding 2 (view model): the window goes to the workspace with the search, not only to the capped results.
    [Fact]
    public async Task Search_PassesTheWindowToTheWorkspace()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        viewModel.SearchWindow = MainWindowViewModel.SearchWindows.Single(option => option.Minutes == 60);

        await viewModel.SearchDeadLettersCommand.ExecuteAsync();

        var since = Assert.IsType<DateTimeOffset>(workspace.SearchRequests[^1].EnqueuedSince);
        Assert.InRange(since, DateTimeOffset.UtcNow.AddMinutes(-61), DateTimeOffset.UtcNow.AddMinutes(-59));
    }

    // Finding 4 (raw editor): "type": "String" with "wireType": "Number.x" and a text value went to SQS as a Number
    // attribute, which SQS refuses. It is now refused before the confirmation.
    [Fact]
    public async Task AwsComposer_ContradictoryWireTypeIsRefusedBeforeSending()
    {
        var profile = CreateProfile("Aws", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.AmazonSqsSns };
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.Destinations.Add(new DestinationItemViewModel(ServiceBusEntityReference.Queue("orders")));
        vm.NewMessageCommand.Execute(null);
        vm.SelectedDestination = vm.Destinations.Last();
        vm.DraftApplicationProperties = """{ "amount": { "type": "String", "value": "twelve", "wireType": "Number.x" } }""";

        await vm.SendDraftCommand.ExecuteAsync();

        Assert.Empty(workspace.SentMessages);
        Assert.Contains("wireType 'Number.x'", vm.ErrorText, StringComparison.Ordinal);
    }
}
