using System.Reflection;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.Tests;

// A resend moves at most 1,000 dead letters and receives each one here. SQS can move a whole dead-letter queue back to
// its source queues itself (StartMessageMoveTask); QueueLoom offers it, with a confirmation that says where the messages
// go. The plan and the start read AWS as it is at that moment: the fakes below change between the two.
public sealed class SqsNativeRedriveTests
{
    private const string DeadLetterArn = "arn:aws:sqs:us-east-1:0:orders-dlq";

    [Fact]
    public async Task ThePlanReadsAwsAfreshAndNamesTheDeadLetterQueueItsCurrentSourcesAndTheCount()
    {
        // A cold connection: nothing was read before the plan.
        await using var fixture = new Fixture(ProfileAccessMode.ReadOnly);

        var plan = await fixture.Workspace.PlanNativeRedriveAsync(ServiceBusEntityReference.Queue("orders"));

        Assert.Equal("orders-dlq", plan.DeadLetterQueue);
        Assert.Equal(DeadLetterArn, plan.DeadLetterArn);
        Assert.Equal(["orders", "payments"], plan.SourceQueues);
        Assert.Equal(42, plan.ApproximateMessages);
        Assert.Equal(fixture.Workspace.ConnectedConfigurationIdentity, plan.ConfigurationIdentity);
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

    // While the confirmation was open, another client changed AWS. The start reads AWS again and refuses.
    [Theory]
    [InlineData("another queue starts using it")]
    [InlineData("the dead-letter queue is created again")]
    [InlineData("the dead-letter queue is created again under the same name")]
    [InlineData("the source points elsewhere")]
    [InlineData("an SNS subscription starts using it")]
    public async Task AChangeInAwsAfterThePlanMovesNothing(string change)
    {
        await using var fixture = new Fixture(ProfileAccessMode.ReadWrite);
        var plan = await fixture.Workspace.PlanNativeRedriveAsync(ServiceBusEntityReference.Queue("orders"));

        switch (change)
        {
            case "another queue starts using it":
                fixture.Sqs.Queues["refunds"] = DeadLetterArn;
                break;
            case "the dead-letter queue is created again":
                // Same name, and the sources point at it: only its ARN shows it is another queue.
                fixture.Sqs.ArnSuffix = "-v2";
                fixture.Sqs.Queues["orders"] = DeadLetterArn + "-v2";
                fixture.Sqs.Queues["payments"] = DeadLetterArn + "-v2";
                break;
            case "the dead-letter queue is created again under the same name":
                // SQS builds the ARN from the account, region and name: only the creation time differs.
                fixture.Sqs.CreatedAt["orders-dlq"] = 1_700_000_500;
                break;
            case "the source points elsewhere":
                fixture.Sqs.Queues["other-dlq"] = null;
                fixture.Sqs.Queues["orders"] = "arn:aws:sqs:us-east-1:0:other-dlq";
                break;
            default:
                fixture.Sns.DeadLetterArn = DeadLetterArn;
                break;
        }
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Workspace.StartNativeRedriveAsync(plan));

        Assert.Matches("changed since you confirmed|SNS subscription", error.Message);
        Assert.Empty(fixture.Sqs.Moves);
    }

    // The start's token also ends with temporary write access; it ended while AWS was read again before the move.
    [Fact]
    public async Task ATokenThatEndsBeforeSubmissionMovesNothing()
    {
        await using var fixture = new Fixture(ProfileAccessMode.ReadWrite);
        var plan = await fixture.Workspace.PlanNativeRedriveAsync(ServiceBusEntityReference.Queue("orders"));
        using var cancellation = new CancellationTokenSource();
        fixture.Sqs.OnListQueues = cancellation.Cancel;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.Workspace.StartNativeRedriveAsync(plan, cancellation.Token));

        Assert.Empty(fixture.Sqs.Moves);
    }

    [Fact]
    public async Task APlanFromAnotherEnvironmentSetupMovesNothing()
    {
        await using var fixture = new Fixture(ProfileAccessMode.ReadWrite);
        var plan = await fixture.Workspace.PlanNativeRedriveAsync(ServiceBusEntityReference.Queue("orders"));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fixture.Workspace.StartNativeRedriveAsync(plan with { ConfigurationIdentity = "another" }));

        Assert.Contains("environment changed", error.Message, StringComparison.Ordinal);
        Assert.Empty(fixture.Sqs.Moves);
    }

    // AWS moves a message back to the queue it came from; a message an SNS subscription dead-lettered came from none.
    [Fact]
    public async Task ADeadLetterQueueThatAlsoServesAnSnsSubscriptionIsNotRedriven()
    {
        await using var fixture = new Fixture(ProfileAccessMode.ReadWrite);
        fixture.Sns.DeadLetterArn = DeadLetterArn;

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
        public FakeSqs Sqs { get; } = new();
        public FakeSns Sns { get; } = new();

        public Fixture(ProfileAccessMode access)
        {
            void Set(Type type, string name, object value) =>
                type.GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(Workspace, value);
            Set(typeof(AwsSqsSnsWorkspace), "_sqs", Sqs);
            Set(typeof(AwsSqsSnsWorkspace), "_sns", Sns);
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

    /// <summary>Queues by name, each with the dead-letter queue ARN of its redrive policy (or none).</summary>
    internal sealed class FakeSqs() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public Dictionary<string, string?> Queues { get; } = new()
        {
            ["orders"] = DeadLetterArn,
            ["payments"] = DeadLetterArn,
            ["orders-dlq"] = null,
            ["standalone"] = null
        };

        /// <summary>Appended to the dead-letter queue's ARN, as when it is deleted and created again.</summary>
        public string ArnSuffix { get; set; } = string.Empty;

        /// <summary>Each queue's CreatedTimestamp, in Unix seconds.</summary>
        public Dictionary<string, long> CreatedAt { get; } = [];

        /// <summary>Called on each ListQueues (each topology read).</summary>
        public Action? OnListQueues { get; set; }

        public List<StartMessageMoveTaskRequest> Moves { get; } = [];

        private string ArnOf(string name) => $"arn:aws:sqs:us-east-1:0:{name}" + (name == "orders-dlq" ? ArnSuffix : string.Empty);

        public override Task<ListQueuesResponse> ListQueuesAsync(ListQueuesRequest request, CancellationToken cancellationToken = default)
        {
            OnListQueues?.Invoke();
            return Task.FromResult(new ListQueuesResponse { QueueUrls = Queues.Keys.Select(name => $"https://isolated.invalid/{name}").ToList() });
        }

        public override Task<GetQueueAttributesResponse> GetQueueAttributesAsync(GetQueueAttributesRequest request,
            CancellationToken cancellationToken = default)
        {
            var name = request.QueueUrl[(request.QueueUrl.LastIndexOf('/') + 1)..];
            var attributes = new Dictionary<string, string>
            {
                ["QueueArn"] = ArnOf(name),
                ["CreatedTimestamp"] = CreatedAt.GetValueOrDefault(name, 1_700_000_000).ToString(System.Globalization.CultureInfo.InvariantCulture),
                ["ApproximateNumberOfMessages"] = name == "orders-dlq" ? "42" : "0"
            };
            if (Queues[name] is { } deadLetter)
            {
                attributes["RedrivePolicy"] = $"{{\"deadLetterTargetArn\":\"{deadLetter}\",\"maxReceiveCount\":3}}";
            }
            return Task.FromResult(new GetQueueAttributesResponse { Attributes = attributes });
        }

        public override Task<StartMessageMoveTaskResponse> StartMessageMoveTaskAsync(StartMessageMoveTaskRequest request,
            CancellationToken cancellationToken = default)
        {
            Moves.Add(request);
            return Task.FromResult(new StartMessageMoveTaskResponse { TaskHandle = "task-1" });
        }
    }

    /// <summary>One topic with one HTTPS subscription, dead-lettering to <see cref="DeadLetterArn"/> when set.</summary>
    internal sealed class FakeSns() : AmazonSimpleNotificationServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        private const string Topic = "arn:aws:sns:us-east-1:0:events";
        public string? DeadLetterArn { get; set; }

        public override Task<Sns.ListTopicsResponse> ListTopicsAsync(Sns.ListTopicsRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new Sns.ListTopicsResponse { Topics = [new Sns.Topic { TopicArn = Topic }] });

        public override Task<Sns.ListSubscriptionsByTopicResponse> ListSubscriptionsByTopicAsync(Sns.ListSubscriptionsByTopicRequest request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new Sns.ListSubscriptionsByTopicResponse
            {
                Subscriptions = [new Sns.Subscription { SubscriptionArn = Topic + ":s1", Protocol = "https", Endpoint = "https://hooks.example/x", TopicArn = Topic }]
            });

        public override Task<Sns.GetSubscriptionAttributesResponse> GetSubscriptionAttributesAsync(Sns.GetSubscriptionAttributesRequest request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new Sns.GetSubscriptionAttributesResponse
            {
                Attributes = DeadLetterArn is null ? [] : new Dictionary<string, string> { ["RedrivePolicy"] = $"{{\"deadLetterTargetArn\":\"{DeadLetterArn}\"}}" }
            });

        public override Task<Sns.GetTopicAttributesResponse> GetTopicAttributesAsync(Sns.GetTopicAttributesRequest request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new Sns.GetTopicAttributesResponse { Attributes = [] });
    }
}
