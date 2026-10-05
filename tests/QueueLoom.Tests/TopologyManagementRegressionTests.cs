using System.Reflection;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Messaging;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.Tests;

/// <summary>Cycle 6: topology and rule changes measured against what each broker actually does.</summary>
public sealed class TopologyManagementRegressionTests
{
    // ---- Finding 1: SQS CreateQueue is idempotent ---------------------------------------------------------------------

    // CreateQueue: "If you specify the name of an existing queue and provide the exact same names and values for all its
    // attributes, the CreateQueue action will return the URL of the existing queue instead of creating a new one."
    // QueueLoom reported such a call as "Queue created" although nothing was created.
    [Fact]
    public async Task SqsCreate_ExistingQueueIsRefusedInsteadOfReportedAsCreated()
    {
        using var sqs = new SqsBroker { Queues = { ["orders"] = new Dictionary<string, string>() } };
        await using var workspace = SqsWorkspace(sqs);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workspace.CreateQueueAsync(new QueueDefinition("orders", new QueueSettings(), CreateDeadLetterQueue: false)));

        Assert.Contains("already exists", error.Message, StringComparison.Ordinal);
        Assert.Empty(sqs.Created);
    }

    // With "also create a dead-letter queue", "<name>-dlq" was created before the queue. When CreateQueue then refused the
    // queue (QueueNameExists: "Amazon SQS returns this error only if the request includes attributes whose values differ
    // from those of the existing queue", or any other 400), the dead-letter queue stayed behind. An existing
    // "<name>-dlq" (perhaps another queue's dead-letter queue) was silently adopted, since CreateQueue returns its URL.
    [Theory]
    [InlineData("queue-exists")]
    [InlineData("dead-letter-exists")]
    [InlineData("queue-rejected")]
    public async Task SqsCreate_FailedQueueCreationLeavesNoDeadLetterQueueBehind(string failure)
    {
        using var sqs = new SqsBroker { RejectCreateOf = failure == "queue-rejected" ? "orders" : null };
        if (failure == "queue-exists") sqs.Queues["orders"] = new Dictionary<string, string> { ["VisibilityTimeout"] = "60" };
        if (failure == "dead-letter-exists") sqs.Queues["orders-dlq"] = new Dictionary<string, string> { ["MessageRetentionPeriod"] = "1209600" };
        await using var workspace = SqsWorkspace(sqs);

        var error = await Assert.ThrowsAnyAsync<Exception>(() =>
            workspace.CreateQueueAsync(new QueueDefinition("orders", new QueueSettings(MaxDeliveryCount: 3), CreateDeadLetterQueue: true)));

        // A taken name is refused before anything is created. When SQS refuses the main queue after the dead-letter
        // queue was created, that queue is never deleted (its ownership cannot be proven) but named in the error.
        Assert.Equal(failure != "queue-exists", sqs.Queues.ContainsKey("orders-dlq"));
        Assert.Equal(failure == "queue-exists", sqs.Queues.ContainsKey("orders"));
        Assert.Empty(sqs.Deleted);
        if (failure == "queue-rejected")
        {
            Assert.Contains("'orders-dlq' was left in place", error.Message, StringComparison.Ordinal);
        }
    }

    // Another operator creates "orders-dlq" between QueueLoom's check and its CreateQueue call; SQS returns that
    // existing queue's URL (same attributes). When the main queue is then refused, the other operator's dead-letter
    // queue, with its messages, must never be deleted.
    [Fact]
    public async Task SqsCreate_ADeadLetterQueueAnotherCreatorMadeMeanwhileIsNeverDeleted()
    {
        using var sqs = new SqsBroker { RejectCreateOf = "orders", CreateConcurrently = "orders-dlq" };
        await using var workspace = SqsWorkspace(sqs);

        var error = await Assert.ThrowsAnyAsync<Exception>(() =>
            workspace.CreateQueueAsync(new QueueDefinition("orders", new QueueSettings(MaxDeliveryCount: 3), CreateDeadLetterQueue: true)));

        Assert.True(sqs.Queues.ContainsKey("orders-dlq"));
        Assert.Empty(sqs.Deleted);
        Assert.Contains("orders-dlq", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SqsCreate_NewQueueWithDeadLetterQueueIsStillCreated()
    {
        using var sqs = new SqsBroker();
        await using var workspace = SqsWorkspace(sqs);

        await workspace.CreateQueueAsync(new QueueDefinition("orders.fifo", new QueueSettings(MaxDeliveryCount: 3), CreateDeadLetterQueue: true));

        Assert.True(sqs.Queues.ContainsKey("orders-dlq.fifo"));
        Assert.Contains("orders-dlq.fifo", sqs.Queues["orders.fifo"]["RedrivePolicy"], StringComparison.Ordinal);
    }

    // ---- Finding 2: Kafka CreateTopics is per topic ------------------------------------------------------------------

    // CreateTopics creates each topic of a request on its own: CreateTopicsException.Results holds "the result
    // corresponding to all topics in the request (whether or not they were in error)". With an existing "orders.DLT"
    // (Spring Kafka's recoverer creates it), "orders" was created but QueueLoom reported "Kafka refused the change"; with
    // an existing "orders", the new "orders.DLT" stayed behind.
    [Fact]
    public async Task KafkaCreate_ExistingDeadLetterTopicDoesNotTurnACreatedTopicIntoAFailure()
    {
        var admin = FakeAdmin.Create(["orders.DLT"]);
        await using var workspace = KafkaWorkspaceWith(admin);

        await workspace.CreateQueueAsync(new QueueDefinition("orders", new QueueSettings(Partitions: 3), CreateDeadLetterQueue: true));

        Assert.Contains("orders", admin.Topics);
        Assert.Contains("orders.DLT", admin.Topics);
    }

    [Fact]
    public async Task KafkaCreate_ExistingTopicLeavesNoNewDeadLetterTopicBehind()
    {
        var admin = FakeAdmin.Create(["orders"]);
        await using var workspace = KafkaWorkspaceWith(admin);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workspace.CreateQueueAsync(new QueueDefinition("orders", new QueueSettings(), CreateDeadLetterQueue: true)));

        Assert.Contains("already exists", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("orders.DLT", admin.Topics);
    }

    // ---- Finding 3: SNS filter policy scope switch is two calls --------------------------------------------------------

    // Switching an attribute policy to a body policy sets FilterPolicyScope=MessageBody first, then the policy. SNS refuses
    // policies QueueLoom accepts ("A filter policy can have a maximum of five keys"; "The total combination of values ...
    // must not exceed 150"). The refusal left the old attribute policy applied to the message body, so the subscription
    // silently stopped receiving what it received before. The scope must be put back.
    [Fact]
    public async Task SnsScopeSwitch_RefusedBodyPolicyRestoresTheAttributeScope()
    {
        using var sns = new SnsBroker();
        sns.Add(UsArn, "arn:aws:sqs:us-east-1:111111111111:orders", """{"region":["EU"]}""", "MessageAttributes");
        // Six leaf keys: "A filter policy can have a maximum of five keys."
        sns.Refuse = request => request.AttributeName == "FilterPolicy" && request.AttributeValue.Contains("\"f\"", StringComparison.Ordinal);
        await using var workspace = SnsWorkspace(sns);
        var body = new SubscriptionRule(AwsSqsSnsWorkspace.FilterPolicyRule, RuleFilterKind.SnsFilterPolicy)
        {
            Expression = """{"order":{"a":["1"],"b":["1"],"c":["1"],"d":["1"],"e":["1"],"f":["1"]}}""",
            OnMessageBody = true
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.SaveSubscriptionRuleAsync("events", "sqs:orders", body, replace: true));

        Assert.Equal("MessageAttributes", sns.Attributes[UsArn]["FilterPolicyScope"]);
        Assert.Equal("""{"region":["EU"]}""", sns.Attributes[UsArn]["FilterPolicy"]);
    }

    // The other direction sets the flat policy first and the scope second; a refused scope change must not leave the new
    // attribute policy applied to the body.
    [Fact]
    public async Task SnsScopeSwitch_RefusedScopeChangeRestoresTheBodyPolicy()
    {
        using var sns = new SnsBroker();
        sns.Add(UsArn, "arn:aws:sqs:us-east-1:111111111111:orders", """{"order":{"status":["failed"]}}""", "MessageBody");
        sns.Refuse = request => request.AttributeName == "FilterPolicyScope" && request.AttributeValue == "MessageAttributes";
        await using var workspace = SnsWorkspace(sns);
        var flat = new SubscriptionRule(AwsSqsSnsWorkspace.FilterPolicyRule, RuleFilterKind.SnsFilterPolicy) { Expression = """{"region":["EU"]}""" };

        await Assert.ThrowsAnyAsync<Exception>(() => workspace.SaveSubscriptionRuleAsync("events", "sqs:orders", flat, replace: true));

        Assert.Equal("MessageBody", sns.Attributes[UsArn]["FilterPolicyScope"]);
        Assert.Equal("""{"order":{"status":["failed"]}}""", sns.Attributes[UsArn]["FilterPolicy"]);
    }

    // A timeout or lost response does not mean SNS refused the change. The review's example: a body policy changed to an
    // attribute policy, where SNS applies the scope change but the response is lost. Both changes are in place, so
    // nothing is rolled back (restoring only the policy would leave the old body policy under the attribute scope).
    [Fact]
    public async Task SnsScopeSwitch_SecondChangeAppliedButItsResponseLostIsNotRolledBack()
    {
        using var sns = new SnsBroker();
        sns.Add(UsArn, "arn:aws:sqs:us-east-1:111111111111:orders", """{"kind":["invoice"]}""", "MessageBody");
        sns.LoseResponse = request => request.AttributeName == "FilterPolicyScope";
        await using var workspace = SnsWorkspace(sns);
        var flat = new SubscriptionRule(AwsSqsSnsWorkspace.FilterPolicyRule, RuleFilterKind.SnsFilterPolicy) { Expression = """{"region":["EU"]}""" };

        await workspace.SaveSubscriptionRuleAsync("events", "sqs:orders", flat, replace: true);

        Assert.Equal("MessageAttributes", sns.Attributes[UsArn]["FilterPolicyScope"]);
        Assert.Equal("""{"region":["EU"]}""", sns.Attributes[UsArn]["FilterPolicy"]);
        Assert.Equal(2, sns.Changes.Count);
    }

    [Fact]
    public async Task SnsScopeSwitch_SecondChangeNotAppliedWithAnUnknownOutcomeIsRolledBack()
    {
        using var sns = new SnsBroker();
        sns.Add(UsArn, "arn:aws:sqs:us-east-1:111111111111:orders", """{"kind":["invoice"]}""", "MessageBody");
        sns.LoseResponse = request => request.AttributeName == "FilterPolicyScope";
        sns.LoseAfterApplying = false;
        await using var workspace = SnsWorkspace(sns);
        var flat = new SubscriptionRule(AwsSqsSnsWorkspace.FilterPolicyRule, RuleFilterKind.SnsFilterPolicy) { Expression = """{"region":["EU"]}""" };

        await Assert.ThrowsAnyAsync<Exception>(() => workspace.SaveSubscriptionRuleAsync("events", "sqs:orders", flat, replace: true));

        Assert.Equal("MessageBody", sns.Attributes[UsArn]["FilterPolicyScope"]);
        Assert.Equal("""{"kind":["invoice"]}""", sns.Attributes[UsArn]["FilterPolicy"]);
    }

    [Fact]
    public async Task SnsScopeSwitch_UnknownOutcomeThatCannotBeReadBackWarnsInsteadOfGuessing()
    {
        using var sns = new SnsBroker();
        sns.Add(UsArn, "arn:aws:sqs:us-east-1:111111111111:orders", """{"kind":["invoice"]}""", "MessageBody");
        await using var workspace = SnsWorkspace(sns);
        _ = await workspace.GetTopicRulesAsync("events");
        sns.LoseResponse = request => request.AttributeName == "FilterPolicyScope";
        var flat = new SubscriptionRule(AwsSqsSnsWorkspace.FilterPolicyRule, RuleFilterKind.SnsFilterPolicy) { Expression = """{"region":["EU"]}""" };
        var reads = 0;
        sns.BeforeRead = () => { if (++reads > 1) sns.FailReads = true; };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workspace.SaveSubscriptionRuleAsync("events", "sqs:orders", flat, replace: true));

        Assert.Contains("Check FilterPolicy and FilterPolicyScope", error.Message, StringComparison.Ordinal);
        Assert.Equal(2, sns.Changes.Count);
    }

    // ---- Finding 4: SNS subscriptions are addressed by a derived name --------------------------------------------------

    // Subscriptions have no names; QueueLoom derives "sqs:orders" from the endpoint ARN's last segment and numbers clashes
    // ("sqs:orders (2)") in list order. Two "orders" queues in different regions/accounts clash. When the first is
    // unsubscribed after the rules were read, "sqs:orders" names the other one, so a policy change (or removal) meant for
    // the deleted subscription is applied to a different queue's subscription.
    [Fact]
    public async Task SnsPolicy_ChangeForAnUnsubscribedSubscriptionDoesNotHitItsNamesake()
    {
        using var sns = new SnsBroker();
        sns.Add(UsArn, "arn:aws:sqs:us-east-1:111111111111:orders", null, null);
        sns.Add(EuArn, "arn:aws:sqs:eu-west-1:222222222222:orders", """{"tier":["gold"]}""", "MessageAttributes");
        await using var workspace = SnsWorkspace(sns);
        var rules = await workspace.GetTopicRulesAsync("events");
        Assert.Equal(["sqs:orders", "sqs:orders (2)"], rules.Select(rule => rule.Subscription));

        sns.Remove(UsArn); // unsubscribed meanwhile, in the console or by another tool
        var policy = new SubscriptionRule(AwsSqsSnsWorkspace.FilterPolicyRule, RuleFilterKind.SnsFilterPolicy) { Expression = """{"region":["EU"]}""" };

        await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.SaveSubscriptionRuleAsync("events", "sqs:orders", policy, replace: false));
        await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.DeleteSubscriptionRuleAsync("events", "sqs:orders", AwsSqsSnsWorkspace.FilterPolicyRule));

        Assert.Empty(sns.Changes);
        Assert.Equal("""{"tier":["gold"]}""", sns.Attributes[EuArn]["FilterPolicy"]);
    }

    [Fact]
    public async Task SnsPolicy_ChangeReachesTheSubscriptionThatWasShownEvenAfterItsNameShifts()
    {
        using var sns = new SnsBroker();
        sns.Add(UsArn, "arn:aws:sqs:us-east-1:111111111111:orders", null, null);
        sns.Add(EuArn, "arn:aws:sqs:eu-west-1:222222222222:orders", """{"tier":["gold"]}""", "MessageAttributes");
        await using var workspace = SnsWorkspace(sns);
        await workspace.GetTopicRulesAsync("events");

        sns.Remove(UsArn);
        var policy = new SubscriptionRule(AwsSqsSnsWorkspace.FilterPolicyRule, RuleFilterKind.SnsFilterPolicy) { Expression = """{"tier":["silver"]}""" };
        await workspace.SaveSubscriptionRuleAsync("events", "sqs:orders (2)", policy, replace: true);

        Assert.Equal("""{"tier":["silver"]}""", sns.Attributes[EuArn]["FilterPolicy"]);
    }

    // ---- Fixtures ------------------------------------------------------------------------------------------------------

    private const string UsArn = "arn:aws:sns:us-east-1:111111111111:events:0f1e2d3c-us";
    private const string EuArn = "arn:aws:sns:us-east-1:111111111111:events:9a8b7c6d-eu";

    private static ServiceBusProfile ManagedProfile(MessagingProvider provider) =>
        ViewModelStateTests.CreateProfile("isolated", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with
        {
            Provider = provider, AllowQueueManagement = true
        };

    private static void SetField(Type type, object owner, string name, object value) =>
        type.GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, value);

    private static AwsSqsSnsWorkspace SqsWorkspace(SqsBroker sqs)
    {
        var workspace = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault());
        SetField(typeof(AwsSqsSnsWorkspace), workspace, "_sqs", sqs);
        SetField(typeof(LeasedMessagingWorkspace), workspace, "_profile", ManagedProfile(MessagingProvider.AmazonSqsSns));
        return workspace;
    }

    private static AwsSqsSnsWorkspace SnsWorkspace(SnsBroker sns)
    {
        var workspace = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault());
        SetField(typeof(AwsSqsSnsWorkspace), workspace, "_sns", sns);
        SetField(typeof(AwsSqsSnsWorkspace), workspace, "_index",
            new AwsTopologyIndex([], [AwsTopicInfo.From("arn:aws:sns:us-east-1:111111111111:events", [])]));
        SetField(typeof(LeasedMessagingWorkspace), workspace, "_profile", ManagedProfile(MessagingProvider.AmazonSqsSns));
        return workspace;
    }

    private static KafkaWorkspace KafkaWorkspaceWith(FakeAdmin admin)
    {
        var workspace = new KafkaWorkspace(new DeepAuditCloudTests.EmptyVault());
        SetField(typeof(KafkaWorkspace), workspace, "_admin", admin);
        SetField(typeof(LeasedMessagingWorkspace), workspace, "_profile", ManagedProfile(MessagingProvider.Kafka));
        return workspace;
    }

    /// <summary>SQS as CreateQueue documents it: an existing name with the same attributes returns its URL.</summary>
    private sealed class SqsBroker() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public Dictionary<string, Dictionary<string, string>> Queues { get; } = new(StringComparer.Ordinal);
        public List<CreateQueueRequest> Created { get; } = [];
        public string? RejectCreateOf { get; init; }
        /// <summary>Another operator creates this queue, with the same attributes, right before QueueLoom's CreateQueue.</summary>
        public string? CreateConcurrently { get; init; }
        public List<string> Deleted { get; } = [];

        private static string Url(string name) => "https://sqs.us-east-1.amazonaws.com/111111111111/" + name;
        private static string NameOf(string url) => url.TrimEnd('/').Split('/')[^1];

        public override Task<GetQueueUrlResponse> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken = default) =>
            GetQueueUrlAsync(new GetQueueUrlRequest { QueueName = queueName }, cancellationToken);

        public override Task<GetQueueUrlResponse> GetQueueUrlAsync(GetQueueUrlRequest request, CancellationToken cancellationToken = default) =>
            Queues.ContainsKey(request.QueueName)
                ? Task.FromResult(new GetQueueUrlResponse { QueueUrl = Url(request.QueueName) })
                : throw new QueueDoesNotExistException("The specified queue does not exist.") { StatusCode = System.Net.HttpStatusCode.BadRequest };

        public override Task<CreateQueueResponse> CreateQueueAsync(CreateQueueRequest request, CancellationToken cancellationToken = default)
        {
            var attributes = request.Attributes ?? [];
            if (request.QueueName == CreateConcurrently && !Queues.ContainsKey(request.QueueName))
            {
                Queues[request.QueueName] = new Dictionary<string, string>(attributes, StringComparer.Ordinal) { ["(other operator)"] = "x" };
                // SQS still answers with the existing queue's URL: the attributes it was asked for match.
                return Task.FromResult(new CreateQueueResponse { QueueUrl = Url(request.QueueName) });
            }
            if (request.QueueName == RejectCreateOf)
                throw new AmazonSQSException("Invalid value for the parameter RedrivePolicy.")
                    { ErrorCode = "InvalidAttributeValue", StatusCode = System.Net.HttpStatusCode.BadRequest };
            if (Queues.TryGetValue(request.QueueName, out var existing))
            {
                if (attributes.Count == existing.Count && attributes.All(pair => existing.GetValueOrDefault(pair.Key) == pair.Value))
                    return Task.FromResult(new CreateQueueResponse { QueueUrl = Url(request.QueueName) });
                throw new QueueNameExistsException("A queue already exists with the same name and a different value for attribute VisibilityTimeout")
                    { StatusCode = System.Net.HttpStatusCode.BadRequest };
            }
            Created.Add(request);
            Queues[request.QueueName] = new Dictionary<string, string>(attributes, StringComparer.Ordinal);
            return Task.FromResult(new CreateQueueResponse { QueueUrl = Url(request.QueueName) });
        }

        public override Task<GetQueueAttributesResponse> GetQueueAttributesAsync(GetQueueAttributesRequest request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new GetQueueAttributesResponse
            {
                // The queue's stored attributes, as GetQueueAttributes returns them, plus its ARN.
                Attributes = new Dictionary<string, string>(Queues.GetValueOrDefault(NameOf(request.QueueUrl)) ?? [], StringComparer.Ordinal)
                {
                    ["QueueArn"] = "arn:aws:sqs:us-east-1:111111111111:" + NameOf(request.QueueUrl)
                }
            });

        public override Task<SetQueueAttributesResponse> SetQueueAttributesAsync(SetQueueAttributesRequest request,
            CancellationToken cancellationToken = default)
        {
            foreach (var (name, value) in request.Attributes) Queues[NameOf(request.QueueUrl)][name] = value;
            return Task.FromResult(new SetQueueAttributesResponse());
        }

        public override Task<DeleteQueueResponse> DeleteQueueAsync(string queueUrl, CancellationToken cancellationToken = default) =>
            DeleteQueueAsync(new DeleteQueueRequest { QueueUrl = queueUrl }, cancellationToken);

        public override Task<DeleteQueueResponse> DeleteQueueAsync(DeleteQueueRequest request, CancellationToken cancellationToken = default)
        {
            Deleted.Add(NameOf(request.QueueUrl));
            Queues.Remove(NameOf(request.QueueUrl));
            return Task.FromResult(new DeleteQueueResponse());
        }
    }

    /// <summary>One SNS topic "events" with confirmed subscriptions whose attributes change as SNS would change them.</summary>
    private sealed class SnsBroker() : AmazonSimpleNotificationServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        private readonly List<Sns.Subscription> _subscriptions = [];
        public Dictionary<string, Dictionary<string, string>> Attributes { get; } = new(StringComparer.Ordinal);
        public List<Sns.SetSubscriptionAttributesRequest> Changes { get; } = [];
        /// <summary>Which changes SNS refuses, for example a policy over its five-key limit.</summary>
        public Func<Sns.SetSubscriptionAttributesRequest, bool>? Refuse { get; set; }
        /// <summary>Changes whose response is lost: applied (or not, per <see cref="LoseAfterApplying"/>), then a timeout.</summary>
        public Func<Sns.SetSubscriptionAttributesRequest, bool>? LoseResponse { get; set; }
        public bool LoseAfterApplying { get; set; } = true;
        public bool FailReads { get; set; }
        public Action? BeforeRead { get; set; }

        public void Add(string arn, string endpoint, string? policy, string? scope)
        {
            _subscriptions.Add(new Sns.Subscription
            {
                SubscriptionArn = arn, Protocol = "sqs", Endpoint = endpoint, TopicArn = "arn:aws:sns:us-east-1:111111111111:events"
            });
            var attributes = new Dictionary<string, string>(StringComparer.Ordinal) { ["SubscriptionArn"] = arn };
            if (policy is not null) attributes["FilterPolicy"] = policy;
            if (scope is not null) attributes["FilterPolicyScope"] = scope;
            Attributes[arn] = attributes;
        }

        public void Remove(string arn)
        {
            _subscriptions.RemoveAll(subscription => subscription.SubscriptionArn == arn);
            Attributes.Remove(arn);
        }

        public override Task<Sns.ListSubscriptionsByTopicResponse> ListSubscriptionsByTopicAsync(Sns.ListSubscriptionsByTopicRequest request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new Sns.ListSubscriptionsByTopicResponse { Subscriptions = [.. _subscriptions] });

        public override Task<Sns.GetSubscriptionAttributesResponse> GetSubscriptionAttributesAsync(Sns.GetSubscriptionAttributesRequest request,
            CancellationToken cancellationToken = default)
        {
            BeforeRead?.Invoke();
            if (FailReads)
                throw new HttpRequestException("The connection was reset.");
            return Attributes.TryGetValue(request.SubscriptionArn, out var attributes)
                ? Task.FromResult(new Sns.GetSubscriptionAttributesResponse { Attributes = new Dictionary<string, string>(attributes) })
                : throw new Amazon.SimpleNotificationService.Model.NotFoundException("Subscription does not exist");
        }

        public override Task<Sns.GetTopicAttributesResponse> GetTopicAttributesAsync(Sns.GetTopicAttributesRequest request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new Sns.GetTopicAttributesResponse { Attributes = [] });

        public override Task<Sns.SetSubscriptionAttributesResponse> SetSubscriptionAttributesAsync(Sns.SetSubscriptionAttributesRequest request,
            CancellationToken cancellationToken = default)
        {
            if (!Attributes.TryGetValue(request.SubscriptionArn, out var attributes))
                throw new Amazon.SimpleNotificationService.Model.NotFoundException("Subscription does not exist");
            if (Refuse?.Invoke(request) == true)
                throw new AmazonSimpleNotificationServiceException($"Invalid parameter: {request.AttributeName}: refused")
                    { ErrorCode = "InvalidParameter", StatusCode = System.Net.HttpStatusCode.BadRequest };
            if (LoseResponse?.Invoke(request) == true)
            {
                if (LoseAfterApplying)
                {
                    Changes.Add(request);
                    attributes[request.AttributeName] = request.AttributeValue;
                }
                throw new TaskCanceledException("The request timed out.");
            }
            Changes.Add(request);
            attributes[request.AttributeName] = request.AttributeValue;
            return Task.FromResult(new Sns.SetSubscriptionAttributesResponse());
        }
    }

    /// <summary>A Kafka admin client whose CreateTopics, like the broker's, succeeds or fails per topic.</summary>
    public class FakeAdmin : DispatchProxy
    {
        public HashSet<string> Topics { get; } = new(StringComparer.Ordinal);

        public static FakeAdmin Create(IEnumerable<string> existing)
        {
            var proxy = DispatchProxy.Create<IAdminClient, FakeAdmin>();
            var admin = (FakeAdmin)(object)proxy;
            admin.Topics.UnionWith(existing);
            return admin;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod!.Name)
            {
                case nameof(IAdminClient.CreateTopicsAsync):
                    var reports = new List<CreateTopicReport>();
                    foreach (var topic in (IEnumerable<TopicSpecification>)args![0]!)
                    {
                        var exists = !Topics.Add(topic.Name);
                        reports.Add(new CreateTopicReport
                        {
                            Topic = topic.Name,
                            Error = exists ? new Error(ErrorCode.TopicAlreadyExists, $"Topic '{topic.Name}' already exists.") : new Error(ErrorCode.NoError)
                        });
                    }
                    return reports.Any(report => report.Error.IsError)
                        ? Task.FromException(new CreateTopicsException(reports))
                        : Task.CompletedTask;
                case nameof(IAdminClient.DeleteTopicsAsync):
                    foreach (var topic in (IEnumerable<string>)args![0]!) Topics.Remove(topic);
                    return Task.CompletedTask;
                case nameof(IAdminClient.GetMetadata):
                    var metadata = Topics.Select(topic => new TopicMetadata(topic,
                        [new PartitionMetadata(0, 1, [1], [1], new Error(ErrorCode.NoError))], new Error(ErrorCode.NoError))).ToList();
                    return new Metadata([new BrokerMetadata(1, "broker", 9092)], metadata, 1, "broker");
                case nameof(IDisposable.Dispose):
                    return null;
                default:
                    throw new NotSupportedException(targetMethod.Name);
            }
        }
    }
}

public sealed partial class ViewModelStateTests
{
    // ---- Cycle 6, finding 5: what deleting a queue really removes -----------------------------------------------------

    // Only a Service Bus dead-letter queue belongs to its queue ("can't be deleted or managed independently of the main
    // entity"). For RabbitMQ, SQS, Pub/Sub and Kafka the DLQ column counts a separate queue or topic that survives, yet
    // the confirmation said those dead letters would be deleted too. And deleting that separate dead-letter queue said
    // nothing about the queues dead-lettering into it, whose dead letters RabbitMQ then drops ("If it is missing then,
    // the messages are silently dropped"); the topology read already knows ("Dead-letter queue of orders").
    [Fact]
    public async Task QueueDelete_RabbitDeadLetterQueueShowsWhoDeadLettersIntoIt()
    {
        var confirmation = await DeleteConfirmationAsync(MessagingProvider.RabbitMq, "orders.dlq", "Dead-letter queue of orders", active: 7, deadLetters: 0);

        Assert.Contains("Dead-letter queue of orders", confirmation, StringComparison.Ordinal);
    }

    [Fact]
    public async Task QueueDelete_SeparateDeadLetterQueueIsNotCountedAsDeleted()
    {
        var confirmation = await DeleteConfirmationAsync(MessagingProvider.RabbitMq, "orders", null, active: 7, deadLetters: 120);

        Assert.DoesNotContain("120 dead-lettered", confirmation, StringComparison.Ordinal);
        Assert.Contains("not deleted", confirmation, StringComparison.Ordinal);
    }

    [Fact]
    public async Task QueueDelete_ServiceBusDeadLetterQueueIsDeletedWithItsQueue()
    {
        var confirmation = await DeleteConfirmationAsync(MessagingProvider.AzureServiceBus, "orders", null, active: 7, deadLetters: 120);

        Assert.Contains("7 active and 120 dead-lettered", confirmation, StringComparison.Ordinal);
    }

    private static async Task<string> DeleteConfirmationAsync(MessagingProvider provider, string queue, string? note, long active, long deadLetters)
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with
        {
            AllowQueueManagement = true, Provider = provider
        };
        var workspace = new FakeWorkspace
        {
            // Service Bus has a built-in dead-letter queue; the other services get a separate one QueueLoom can create.
            QueueManagement = provider == MessagingProvider.AzureServiceBus ? SqsLike with { CanCreateDeadLetterQueue = false } : SqsLike,
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
            [
                new ServiceBusQueue(queue, new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active, deadLetters)), ServiceBusEntityStatus.Active)
                    { Note = note }
            ])
        };
        var dialogs = new FakeDialogService { ConfirmResult = false };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.Name == queue);

        await viewModel.DeleteQueueCommand.ExecuteAsync();

        Assert.Empty(workspace.DeletedQueues);
        return dialogs.Confirmations.Last().Message;
    }
}
