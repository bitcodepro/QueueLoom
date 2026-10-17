using System.Globalization;
using Amazon.SQS.Model;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Aws;

/// <summary>
/// The SQS dead-letter queue redrive (StartMessageMoveTask): AWS moves every message of a dead-letter queue back to the
/// queue it came from, as many as there are and without receiving them here.
/// </summary>
public sealed partial class AwsSqsSnsWorkspace
{
    public override bool SupportsNativeRedrive => true;

    public override async Task<NativeRedrivePlan> PlanNativeRedriveAsync(
        ServiceBusEntityReference source, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        using var operation = await EnterReadOperationAsync(cancellationToken).ConfigureAwait(false);
        GetConnectedProfile();
        var (deadLetterQueue, sources) = ResolveRedrive(source);
        var attributes = (await Sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = deadLetterQueue.Url,
            AttributeNames = ["ApproximateNumberOfMessages"]
        }, cancellationToken).ConfigureAwait(false)).Attributes ?? [];
        var messages = long.TryParse(attributes.GetValueOrDefault("ApproximateNumberOfMessages"), NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var count) ? count : deadLetterQueue.Visible;
        return new NativeRedrivePlan(source, deadLetterQueue.Name, sources, messages);
    }

    public override async Task<string> StartNativeRedriveAsync(NativeRedrivePlan plan, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);
        using var operation = await EnterReadOperationAsync(cancellationToken).ConfigureAwait(false);
        var profile = GetConnectedProfile();
        if (!profile.CanWrite)
        {
            throw new InvalidOperationException(
                $"Environment '{profile.Name}' is read-only. Unlock write access before moving messages.");
        }
        // What was confirmed must still be true: the same dead-letter queue, shared by the same queues.
        var (deadLetterQueue, sources) = ResolveRedrive(plan.Source);
        if (!string.Equals(deadLetterQueue.Name, plan.DeadLetterQueue, StringComparison.Ordinal) ||
            !sources.SequenceEqual(plan.SourceQueues, StringComparer.Ordinal))
        {
            throw new InvalidOperationException(
                "The dead-letter queue setup changed since you confirmed. Refresh and start the redrive again. Nothing was moved.");
        }
        var response = await Sqs.StartMessageMoveTaskAsync(new StartMessageMoveTaskRequest { SourceArn = deadLetterQueue.Arn },
            cancellationToken).ConfigureAwait(false);
        return response.TaskHandle ?? string.Empty;
    }

    /// <summary>The dead-letter queue of <paramref name="source"/> and every queue that uses it, by name.</summary>
    private (AwsQueueInfo DeadLetterQueue, IReadOnlyList<string> Sources) ResolveRedrive(ServiceBusEntityReference source)
    {
        if (source.Kind != ServiceBusEntityKind.Queue)
        {
            throw new InvalidOperationException("Pick the SQS queue whose dead-letter queue should be redriven.");
        }
        var index = _index;
        var queue = index.FindQueue(source.Name)
            ?? throw new InvalidOperationException($"Queue '{source.Name}' was not found. Refresh and try again.");
        var deadLetterQueue = index.FindQueueByArn(queue.DeadLetterTargetArn)
            ?? throw new InvalidOperationException($"Queue '{source.Name}' has no dead-letter queue in its redrive policy.");
        // AWS moves a message back to the queue it came from; a message an SNS subscription dead-lettered came from no
        // queue, and the move task would fail on it.
        if (index.Topics.SelectMany(topic => topic.Subscriptions.Select(subscription => (topic, subscription)))
                .FirstOrDefault(pair => string.Equals(pair.subscription.DeadLetterTargetArn, deadLetterQueue.Arn, StringComparison.Ordinal))
            is { topic: not null } snsUser)
        {
            throw new InvalidOperationException(
                $"Dead-letter queue '{deadLetterQueue.Name}' also receives messages from SNS subscription " +
                $"'{snsUser.topic.Name}/{snsUser.subscription.Name}'. AWS can move messages back only to SQS queues, so " +
                "redrive is not offered here; resend the messages from Messages / DLQ instead.");
        }
        var sources = index.Queues
            .Where(candidate => string.Equals(candidate.DeadLetterTargetArn, deadLetterQueue.Arn, StringComparison.Ordinal))
            .Select(candidate => candidate.Name)
            .Order(StringComparer.Ordinal)
            .ToArray();
        return (deadLetterQueue, sources);
    }
}
