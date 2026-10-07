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

    private static async Task<ServiceBusTopology> ReadTopologyAsync(ScriptedSqs sqs)
    {
        await using var workspace = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(AwsSqsSnsWorkspace).GetField("_sqs", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, sqs);
        typeof(AwsSqsSnsWorkspace).GetField("_sns", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, new EmptySns());
        var read = typeof(AwsSqsSnsWorkspace).GetMethod("ReadTopologyAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        return await (Task<ServiceBusTopology>)read.Invoke(workspace, [CancellationToken.None])!;
    }

    private sealed class ScriptedSqs(Func<string, Exception> failGone)
        : AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost" })
    {
        public override Task<ListQueuesResponse> ListQueuesAsync(ListQueuesRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new ListQueuesResponse { QueueUrls = ["http://localhost/000000000000/gone", "http://localhost/000000000000/kept"] });

        public override Task<GetQueueAttributesResponse> GetQueueAttributesAsync(GetQueueAttributesRequest request, CancellationToken cancellationToken = default) =>
            request.QueueUrl.EndsWith("/gone", StringComparison.Ordinal)
                ? Task.FromException<GetQueueAttributesResponse>(failGone(request.QueueUrl))
                : Task.FromResult(new GetQueueAttributesResponse
                {
                    Attributes = new() { ["QueueArn"] = "arn:aws:sqs:eu-west-1:000000000000:kept", ["ApproximateNumberOfMessages"] = "0" }
                });
    }

    private sealed class EmptySns()
        : AmazonSimpleNotificationServiceClient(new BasicAWSCredentials("test", "test"), new AmazonSimpleNotificationServiceConfig { ServiceURL = "http://localhost" })
    {
        public override Task<Sns.ListTopicsResponse> ListTopicsAsync(Sns.ListTopicsRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new Sns.ListTopicsResponse { Topics = [] });
    }
}
