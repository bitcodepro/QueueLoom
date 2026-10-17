using System.Reflection;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Google;

namespace QueueLoom.Tests;

public sealed class AwsPreflightRejectionTests
{
    [Theory]
    [InlineData(false, "invalid")]
    [InlineData(true, "invalid")]
    [InlineData(false, "reserved")]
    [InlineData(false, "too many")]
    public async Task AttributesRefusedBeforeDispatchAreProvenNonDelivery(bool topic, string shape)
    {
        await using var fixture = new AwsFixture();
        var properties = shape == "too many" ? new EditableMessageProperties(Subject: "orders") : EditableMessageProperties.Empty;
        var attributes = shape == "too many"
            ? Enumerable.Range(0, 10).Select(index => new MessageApplicationProperty($"p{index}", ApplicationPropertyType.String, "v"))
            : [new MessageApplicationProperty(shape == "invalid" ? "order id" : "aws.trace", ApplicationPropertyType.String, "v")];
        var draft = new MessageDraft(new EditableMessageBody("body", MessageBodyFormat.Text), properties, attributes);

        var error = await Record.ExceptionAsync(() => fixture.Send(topic, draft));
        Assert.Empty(fixture.Sqs.Sends);
        Assert.Empty(fixture.Sns.Sends);
        Assert.Contains("Nothing was sent", Assert.IsType<DeliveryRejectedException>(error).Message, StringComparison.Ordinal);
    }

    // SNS raw delivery hands the attributes to SQS as message attributes, and SQS takes at most 10: SNS accepts the
    // publish and then fails that delivery, so a move would delete an original no raw SQS subscriber received.
    [Theory]
    [InlineData("sqs", true, 11, true)]
    [InlineData("sqs", true, 10, false)]
    [InlineData("sqs", false, 11, false)]
    [InlineData("https", true, 11, false)]
    [InlineData("lambda", false, 11, false)]
    public async Task TopicsWithRawSqsSubscribersRefuseMoreThanTenAttributes(string protocol, bool raw, int attributes, bool refused)
    {
        await using var fixture = new AwsFixture(new AwsSubscriptionInfo(
            "arn:aws:sns:us-east-1:0:events:sub", protocol, protocol == "sqs" ? "arn:aws:sqs:us-east-1:0:billing" : "https://hooks.example/x", null)
        {
            RawMessageDelivery = raw
        });
        // The subject travels as an attribute too, so 'attributes' counts it.
        var draft = new MessageDraft(new EditableMessageBody("body", MessageBodyFormat.Text), new EditableMessageProperties(Subject: "orders"),
            Enumerable.Range(0, attributes - 1).Select(index => new MessageApplicationProperty($"p{index}", ApplicationPropertyType.String, "v")));

        var error = await Record.ExceptionAsync(() => fixture.Send(topic: true, draft));

        if (refused)
        {
            var rejected = Assert.IsType<DeliveryRejectedException>(error);
            Assert.Contains("sqs:billing", rejected.Message, StringComparison.Ordinal);
            Assert.Contains("Nothing was sent", rejected.Message, StringComparison.Ordinal);
            Assert.Empty(fixture.Sns.Sends);
        }
        else
        {
            Assert.Null(error);
            Assert.Equal(attributes, Assert.Single(fixture.Sns.Sends).MessageAttributes.Count);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AcceptedAwsAttributesAndBinaryBodyKeepTheirOutgoingRepresentation(bool topic)
    {
        await using var fixture = new AwsFixture();
        var draft = new MessageDraft(new EditableMessageBody("AP8=", MessageBodyFormat.Base64), applicationProperties:
        [
            new("image", ApplicationPropertyType.Binary, "AP8=") { WireType = "Binary.png" },
            new("amount", ApplicationPropertyType.Int64, "42") { WireType = "Number.customer" }
        ]);

        await fixture.Send(topic, draft);

        if (topic)
        {
            var request = Assert.Single(fixture.Sns.Sends);
            Assert.Empty(fixture.Sqs.Sends);
            Assert.Equal("AP8=", request.Message);
            Assert.Equal("arn:aws:sns:us-east-1:0:events", request.TopicArn);
            Assert.Equal("Binary.png", request.MessageAttributes["image"].DataType);
            Assert.Equal(new byte[] { 0, 255 }, request.MessageAttributes["image"].BinaryValue.ToArray());
            Assert.Equal("Number.customer", request.MessageAttributes["amount"].DataType);
            Assert.Equal("42", request.MessageAttributes["amount"].StringValue);
        }
        else
        {
            var request = Assert.Single(fixture.Sqs.Sends);
            Assert.Empty(fixture.Sns.Sends);
            Assert.Equal("AP8=", request.MessageBody);
            Assert.Equal("https://isolated.invalid/orders", request.QueueUrl);
            Assert.Equal("Binary.png", request.MessageAttributes["image"].DataType);
            Assert.Equal(new byte[] { 0, 255 }, request.MessageAttributes["image"].BinaryValue.ToArray());
            Assert.Equal("Number.customer", request.MessageAttributes["amount"].DataType);
            Assert.Equal("42", request.MessageAttributes["amount"].StringValue);
        }
    }

    [Fact]
    public async Task PubSubPreservesBinaryDataAttributesAndOrderingKey()
    {
        var publisher = new RecordingPublisher();
        await using var workspace = PubSub(publisher);
        var draft = new MessageDraft(new EditableMessageBody("AP8=", MessageBodyFormat.Base64),
            new EditableMessageProperties(ContentType: "application/octet-stream", SessionId: "ordering-key"),
            [new("binary", ApplicationPropertyType.Binary, "AP8=")]);

        await SendPubSub(workspace, draft, CancellationToken.None);

        var request = Assert.Single(publisher.Requests);
        Assert.Equal("projects/project-a/topics/events", request.Topic);
        var message = Assert.Single(request.Messages);
        Assert.Equal(new byte[] { 0, 255 }, message.Data.ToByteArray());
        Assert.Equal("AP8=", message.Attributes["binary"]);
        Assert.Equal("application/octet-stream", message.Attributes["ContentType"]);
        Assert.Equal("ordering-key", message.OrderingKey);
    }

    [Fact]
    public async Task PubSubBatchCancellationKeepsConfirmedAndUnattemptedItemsDistinct()
    {
        using var cancellation = new CancellationTokenSource();
        var publisher = new RecordingPublisher
        {
            OnPublish = index =>
            {
                if (index != 1) return null;
                cancellation.Cancel();
                return new OperationCanceledException(cancellation.Token);
            }
        };
        await using var workspace = PubSub(publisher);
        var forwarding = DispatchProxy.Create<IServiceBusWorkspace, ForwardingWorkspace>();
        ((ForwardingWorkspace)(object)forwarding).Call = (method, args) => method.Name switch
        {
            "get_ConnectedProvider" => MessagingProvider.GooglePubSub,
            "SendMessageAsync" => SendPubSub(workspace, ((SendMessageRequest)args![0]!).Message, (CancellationToken)args[1]!),
            _ => throw new NotSupportedException(method.Name)
        };
        var items = Enumerable.Range(0, 3).Select(index =>
        {
            var original = new BrowsedMessage(ServiceBusEntityReference.Queue("source"), ServiceBusSubQueue.Active,
                index, "body"u8.ToArray(), new EditableMessageProperties(MessageId: $"m-{index}"));
            return new ResendItem(original, ServiceBusEntityReference.Topic("events"), original.CreateDraft());
        }).ToArray();

        var result = await DeadLetterResender.ResendAsync(forwarding, items, ResendMode.Copy, cancellationToken: cancellation.Token);

        Assert.Equal(1, result.SentCount);
        Assert.Equal(1, result.FailedCount);
        Assert.Equal(1, result.CancelledCount);
        Assert.Contains("unknown", result.Items[1].Detail, StringComparison.Ordinal);
        Assert.Equal(2, publisher.Requests.Count);
    }

    private sealed class AwsFixture : IAsyncDisposable
    {
        private readonly AwsSqsSnsWorkspace _workspace = new(new DeepAuditCloudTests.EmptyVault());
        public ProviderLimitRegressionTests.SendSqs Sqs { get; } = new();
        public ProviderLimitRegressionTests.SendSns Sns { get; } = new();
        public AwsFixture(params AwsSubscriptionInfo[] topicSubscriptions)
        {
            void Set(string name, object value) => typeof(AwsSqsSnsWorkspace).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(_workspace, value);
            Set("_sqs", Sqs);
            Set("_sns", Sns);
            Set("_index", new AwsTopologyIndex(
                [AwsQueueInfo.From("https://isolated.invalid/orders", new Dictionary<string, string> { ["QueueArn"] = "arn:aws:sqs:us-east-1:0:orders" })],
                [AwsTopicInfo.From("arn:aws:sns:us-east-1:0:events", topicSubscriptions)]));
        }
        public Task Send(bool topic, MessageDraft draft) => (Task)typeof(AwsSqsSnsWorkspace)
            .GetMethod("SendCoreAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(_workspace,
                [new ServiceBusTopology(DateTimeOffset.UtcNow), topic ? ServiceBusEntityReference.Topic("events") : ServiceBusEntityReference.Queue("orders"), draft, CancellationToken.None])!;
        public ValueTask DisposeAsync() => _workspace.DisposeAsync();
    }
    private static GooglePubSubWorkspace PubSub(RecordingPublisher publisher)
    {
        var workspace = new GooglePubSubWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(GooglePubSubWorkspace).GetField("_publisher", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, publisher);
        typeof(GooglePubSubWorkspace).GetField("_projectId", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, "project-a");
        return workspace;
    }
    private static Task SendPubSub(GooglePubSubWorkspace workspace, MessageDraft draft, CancellationToken token) =>
        (Task)typeof(GooglePubSubWorkspace).GetMethod("SendCoreAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(workspace,
            [new ServiceBusTopology(DateTimeOffset.UtcNow), ServiceBusEntityReference.Topic("events"), draft, token])!;
    private sealed class RecordingPublisher : PublisherServiceApiClient
    {
        public Func<int, Exception?> OnPublish { get; init; } = _ => null;
        public List<PublishRequest> Requests { get; } = [];
        public override Task<PublishResponse> PublishAsync(PublishRequest request, CallSettings? callSettings = null)
        {
            Requests.Add(request);
            var error = OnPublish(Requests.Count - 1);
            return error is null ? Task.FromResult(new PublishResponse { MessageIds = { "confirmed" } }) : Task.FromException<PublishResponse>(error);
        }
    }
    public class ForwardingWorkspace : DispatchProxy
    {
        public Func<MethodInfo, object?[]?, object?> Call { get; set; } = null!;
        protected override object? Invoke(MethodInfo? method, object?[]? args) => Call(method!, args);
    }
}
