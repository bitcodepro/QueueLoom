using System.Reflection;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;

namespace QueueLoom.Tests;

// A resend moves at most 1,000 dead letters and receives each one here. SQS can move a whole dead-letter queue back to
// its source queues itself (StartMessageMoveTask); QueueLoom now offers it, with a confirmation that says where the
// messages go.
public sealed class SqsNativeRedriveTests
{
    private const string DeadLetterArn = "arn:aws:sqs:us-east-1:0:orders-dlq";

    [Fact]
    public async Task ThePlanNamesTheDeadLetterQueueEverySourceThatSharesItAndTheCount()
    {
        await using var fixture = new Fixture(ProfileAccessMode.ReadOnly);

        var plan = await fixture.Workspace.PlanNativeRedriveAsync(ServiceBusEntityReference.Queue("orders"));

        Assert.Equal("orders-dlq", plan.DeadLetterQueue);
        Assert.Equal(["orders", "payments"], plan.SourceQueues);
        Assert.Equal(42, plan.ApproximateMessages);
        Assert.Empty(fixture.Sqs.Moves);
    }

    [Fact]
    public async Task StartingMovesTheDeadLetterQueueBackToEachMessagesSource()
    {
        await using var fixture = new Fixture(ProfileAccessMode.ReadWrite);
        var plan = await fixture.Workspace.PlanNativeRedriveAsync(ServiceBusEntityReference.Queue("orders"));

        var handle = await fixture.Workspace.StartNativeRedriveAsync(plan);

        Assert.Equal("task-1", handle);
        var move = Assert.Single(fixture.Sqs.Moves);
        Assert.Equal(DeadLetterArn, move.SourceArn);
        // No destination: AWS returns each message to the queue it came from, also when the queue is shared.
        Assert.Null(move.DestinationArn);
    }

    [Fact]
    public async Task AReadOnlyEnvironmentMovesNothing()
    {
        await using var fixture = new Fixture(ProfileAccessMode.ReadOnly);
        var plan = await fixture.Workspace.PlanNativeRedriveAsync(ServiceBusEntityReference.Queue("orders"));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Workspace.StartNativeRedriveAsync(plan));

        Assert.Contains("read-only", error.Message, StringComparison.Ordinal);
        Assert.Empty(fixture.Sqs.Moves);
    }

    // What was confirmed must still hold: another queue started sharing the dead-letter queue in between.
    [Fact]
    public async Task APlanThatNoLongerMatchesTheSetupMovesNothing()
    {
        await using var fixture = new Fixture(ProfileAccessMode.ReadWrite);
        var plan = await fixture.Workspace.PlanNativeRedriveAsync(ServiceBusEntityReference.Queue("orders"));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Workspace.StartNativeRedriveAsync(plan with { SourceQueues = ["orders"] }));

        Assert.Contains("changed since you confirmed", error.Message, StringComparison.Ordinal);
        Assert.Empty(fixture.Sqs.Moves);
    }

    // AWS moves a message back to the queue it came from; a message an SNS subscription dead-lettered came from none.
    [Fact]
    public async Task ADeadLetterQueueThatAlsoServesAnSnsSubscriptionIsNotRedriven()
    {
        await using var fixture = new Fixture(ProfileAccessMode.ReadWrite, snsUsesTheDeadLetterQueue: true);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Workspace.PlanNativeRedriveAsync(ServiceBusEntityReference.Queue("orders")));

        Assert.Contains("SNS subscription", error.Message, StringComparison.Ordinal);
        Assert.Empty(fixture.Sqs.Moves);
    }

    [Theory]
    [InlineData("standalone", "has no dead-letter queue")]
    [InlineData("missing", "was not found")]
    public async Task AQueueWithoutADeadLetterQueueHasNothingToRedrive(string queue, string expected)
    {
        await using var fixture = new Fixture(ProfileAccessMode.ReadWrite);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Workspace.PlanNativeRedriveAsync(ServiceBusEntityReference.Queue(queue)));

        Assert.Contains(expected, error.Message, StringComparison.Ordinal);
    }

    private sealed class Fixture : IAsyncDisposable
    {
        public AwsSqsSnsWorkspace Workspace { get; } = new(new DeepAuditCloudTests.EmptyVault());
        public MoveSqs Sqs { get; } = new();

        public Fixture(ProfileAccessMode access, bool snsUsesTheDeadLetterQueue = false)
        {
            void Set(Type type, string name, object value) =>
                type.GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(Workspace, value);
            static AwsQueueInfo Queue(string name, string? deadLetterArn) => AwsQueueInfo.From($"https://isolated.invalid/{name}",
                new Dictionary<string, string>
                {
                    ["QueueArn"] = $"arn:aws:sqs:us-east-1:0:{name}",
                    ["ApproximateNumberOfMessages"] = "7",
                    ["RedrivePolicy"] = deadLetterArn is null ? "" : $"{{\"deadLetterTargetArn\":\"{deadLetterArn}\",\"maxReceiveCount\":3}}"
                });
            Set(typeof(AwsSqsSnsWorkspace), "_sqs", Sqs);
            Set(typeof(AwsSqsSnsWorkspace), "_index", new AwsTopologyIndex(
                [Queue("orders", DeadLetterArn), Queue("payments", DeadLetterArn), Queue("orders-dlq", null), Queue("standalone", null)],
                [AwsTopicInfo.From("arn:aws:sns:us-east-1:0:events", snsUsesTheDeadLetterQueue
                    ? [new AwsSubscriptionInfo("arn:aws:sns:us-east-1:0:events:s1", "https", "https://hooks.example/x", DeadLetterArn)]
                    : [])]));
            var profile = ServiceBusProfile.CreateNew("AWS", EnvironmentKind.Development,
                new AuthenticationSettings(AuthenticationKind.AwsDefaultCredentials), accessMode: access) with
            {
                Provider = MessagingProvider.AmazonSqsSns,
                Aws = new AwsSettings("us-east-1")
            };
            Set(typeof(QueueLoom.Infrastructure.Messaging.LeasedMessagingWorkspace), "_profile", profile);
        }

        public ValueTask DisposeAsync() => Workspace.DisposeAsync();
    }

    internal sealed class MoveSqs() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public List<StartMessageMoveTaskRequest> Moves { get; } = [];

        public override Task<StartMessageMoveTaskResponse> StartMessageMoveTaskAsync(StartMessageMoveTaskRequest request,
            CancellationToken cancellationToken = default)
        {
            Moves.Add(request);
            return Task.FromResult(new StartMessageMoveTaskResponse { TaskHandle = "task-1" });
        }

        public override Task<GetQueueAttributesResponse> GetQueueAttributesAsync(GetQueueAttributesRequest request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new GetQueueAttributesResponse
            {
                Attributes = new Dictionary<string, string> { ["ApproximateNumberOfMessages"] = request.QueueUrl.EndsWith("orders-dlq") ? "42" : "0" }
            });
    }
}
