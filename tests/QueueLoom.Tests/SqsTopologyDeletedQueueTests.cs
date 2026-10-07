using System.Net;
using System.Reflection;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Messaging;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.Tests;

public sealed class SqsTopologyDeletedQueueTests
{
    // A queue is deleted (another test, a cleanup job, a colleague) between ListQueues and its GetQueueAttributes:
    // it is left out and the surviving queue is listed, instead of the whole refresh failing (which also failed an
    // unrelated SNS send on CI).
    [Theory]
    [InlineData("modelled")]
    [InlineData("error-code")]
    public async Task AQueueDeletedDuringTheRefreshIsLeftOut(string how)
    {
        var sqs = new ScriptedSqs(gone => how == "modelled"
            ? new QueueDoesNotExistException("The specified queue does not exist.")
            : new AmazonSQSException("gone") { ErrorCode = "AWS.SimpleQueueService.NonExistentQueue" });

        var topology = await ReadTopologyAsync(sqs);

        Assert.Equal(["kept"], topology.Queues.Select(queue => queue.Name));
    }

    // Only "queue does not exist" is skipped: access, throttling and network failures still fail the refresh, so a
    // missing permission is not mistaken for an empty account.
    [Theory]
    [InlineData("AccessDenied")]
    [InlineData("RequestThrottled")]
    [InlineData("network")]
    public async Task OtherFailuresStillFailTheRefresh(string kind)
    {
        var sqs = new ScriptedSqs(_ => kind == "network"
            ? new HttpRequestException("connection reset")
            : new AmazonSQSException(kind) { ErrorCode = kind, StatusCode = HttpStatusCode.Forbidden });

        await Assert.ThrowsAnyAsync<Exception>(() => ReadTopologyAsync(sqs));
    }

    // The same for SNS: a topic deleted after ListTopics (at ListSubscriptionsByTopic or GetTopicAttributes) and a
    // subscription removed after ListSubscriptionsByTopic are left out; the surviving topic and subscription stay.
    [Theory]
    [InlineData("subscriptions")]
    [InlineData("topic-attributes")]
    public async Task ATopicOrSubscriptionDeletedDuringTheRefreshIsLeftOut(string where)
    {
        var sns = new ScriptedSns(where);

        var topology = await ReadTopologyAsync(new ScriptedSqs(_ => new InvalidOperationException("unused")), sns, queues: false);

        var topic = Assert.Single(topology.Topics);
        Assert.Equal("kept", topic.Name);
        Assert.Single(topic.Subscriptions);
    }

    // A failure other than "not found" on a topic still fails the refresh.
    [Fact]
    public async Task OtherTopicFailuresStillFailTheRefresh()
    {
        await Assert.ThrowsAnyAsync<Exception>(() =>
            ReadTopologyAsync(new ScriptedSqs(_ => new InvalidOperationException("unused")), new ScriptedSns("throttled"), queues: false));
    }

    private static async Task<ServiceBusTopology> ReadTopologyAsync(ScriptedSqs sqs) => await ReadTopologyAsync(sqs, new EmptySns(), queues: true);

    private static async Task<ServiceBusTopology> ReadTopologyAsync(ScriptedSqs sqs, AmazonSimpleNotificationServiceClient sns, bool queues)
    {
        sqs.ListNothing = !queues;
        await using var workspace = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(AwsSqsSnsWorkspace).GetField("_sqs", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, sqs);
        typeof(AwsSqsSnsWorkspace).GetField("_sns", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, sns);
        var read = typeof(AwsSqsSnsWorkspace).GetMethod("ReadTopologyAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        return await (Task<ServiceBusTopology>)read.Invoke(workspace, [CancellationToken.None])!;
    }

    private sealed class ScriptedSqs(Func<string, Exception> failGone)
        : AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost" })
    {
        public bool ListNothing { get; set; }

        public override Task<ListQueuesResponse> ListQueuesAsync(ListQueuesRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new ListQueuesResponse
            {
                QueueUrls = ListNothing ? [] : ["http://localhost/000000000000/gone", "http://localhost/000000000000/kept"]
            });

        public override Task<GetQueueAttributesResponse> GetQueueAttributesAsync(GetQueueAttributesRequest request, CancellationToken cancellationToken = default) =>
            request.QueueUrl.EndsWith("/gone", StringComparison.Ordinal)
                ? Task.FromException<GetQueueAttributesResponse>(failGone(request.QueueUrl))
                : Task.FromResult(new GetQueueAttributesResponse
                {
                    Attributes = new() { ["QueueArn"] = "arn:aws:sqs:eu-west-1:000000000000:kept", ["ApproximateNumberOfMessages"] = "0" }
                });
    }

    private sealed class ScriptedSns(string where)
        : AmazonSimpleNotificationServiceClient(new BasicAWSCredentials("test", "test"), new AmazonSimpleNotificationServiceConfig { ServiceURL = "http://localhost" })
    {
        private const string Gone = "arn:aws:sns:eu-west-1:000000000000:gone";
        private const string Kept = "arn:aws:sns:eu-west-1:000000000000:kept";

        private static Exception NotFound() => new Sns.NotFoundException("Topic does not exist");

        public override Task<Sns.ListTopicsResponse> ListTopicsAsync(Sns.ListTopicsRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new Sns.ListTopicsResponse { Topics = [new() { TopicArn = Gone }, new() { TopicArn = Kept }] });

        public override Task<Sns.ListSubscriptionsByTopicResponse> ListSubscriptionsByTopicAsync(Sns.ListSubscriptionsByTopicRequest request,
            CancellationToken cancellationToken = default)
        {
            if (request.TopicArn == Gone && where == "subscriptions") return Task.FromException<Sns.ListSubscriptionsByTopicResponse>(NotFound());
            if (request.TopicArn == Gone && where == "throttled")
                return Task.FromException<Sns.ListSubscriptionsByTopicResponse>(new AmazonSimpleNotificationServiceException("slow down") { ErrorCode = "Throttling" });
            return Task.FromResult(new Sns.ListSubscriptionsByTopicResponse
            {
                Subscriptions =
                [
                    new() { SubscriptionArn = request.TopicArn + ":live", Protocol = "sqs", Endpoint = "arn:aws:sqs:eu-west-1:000000000000:q", TopicArn = request.TopicArn },
                    new() { SubscriptionArn = request.TopicArn + ":removed", Protocol = "sqs", Endpoint = "arn:aws:sqs:eu-west-1:000000000000:r", TopicArn = request.TopicArn }
                ]
            });
        }

        public override Task<Sns.GetSubscriptionAttributesResponse> GetSubscriptionAttributesAsync(Sns.GetSubscriptionAttributesRequest request,
            CancellationToken cancellationToken = default) =>
            request.SubscriptionArn.EndsWith(":removed", StringComparison.Ordinal)
                ? Task.FromException<Sns.GetSubscriptionAttributesResponse>(new Sns.NotFoundException("Subscription does not exist"))
                : Task.FromResult(new Sns.GetSubscriptionAttributesResponse { Attributes = [] });

        public override Task<Sns.GetTopicAttributesResponse> GetTopicAttributesAsync(Sns.GetTopicAttributesRequest request,
            CancellationToken cancellationToken = default) =>
            request.TopicArn == Gone && where == "topic-attributes"
                ? Task.FromException<Sns.GetTopicAttributesResponse>(NotFound())
                : Task.FromResult(new Sns.GetTopicAttributesResponse { Attributes = [] });
    }

    private sealed class EmptySns()
        : AmazonSimpleNotificationServiceClient(new BasicAWSCredentials("test", "test"), new AmazonSimpleNotificationServiceConfig { ServiceURL = "http://localhost" })
    {
        public override Task<Sns.ListTopicsResponse> ListTopicsAsync(Sns.ListTopicsRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new Sns.ListTopicsResponse { Topics = [] });
    }
}
