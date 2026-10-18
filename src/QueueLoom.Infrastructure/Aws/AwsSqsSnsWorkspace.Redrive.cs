using System.Globalization;
using Amazon.SQS;
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
        // Fresh from AWS, not from the last refresh: the confirmation must describe the setup as it is.
        await RefreshTopologyInOperationAsync(cancellationToken).ConfigureAwait(false);
        var (deadLetterQueue, sources) = ResolveRedrive(source);
        var attributes = (await Sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = deadLetterQueue.Url,
            AttributeNames = ["ApproximateNumberOfMessages"]
        }, cancellationToken).ConfigureAwait(false)).Attributes ?? [];
        var messages = long.TryParse(attributes.GetValueOrDefault("ApproximateNumberOfMessages"), NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var count) ? count : deadLetterQueue.Visible;
        return new NativeRedrivePlan(source, deadLetterQueue.Name, sources, messages)
        {
            DeadLetterArn = deadLetterQueue.Arn,
            DeadLetterCreatedAt = deadLetterQueue.CreatedAt,
            ConfigurationIdentity = ConnectedConfigurationIdentity
        };
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
        if (!string.Equals(ConnectedConfigurationIdentity, plan.ConfigurationIdentity, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "The environment changed since you confirmed. Start the redrive again. Nothing was moved.");
        }
        // What was confirmed must still be true in AWS right now: the same dead-letter queue (its ARN and, since a
        // queue created again under the same name keeps the ARN, its creation time), used by the same queues, and by no
        // SNS subscription.
        await RefreshTopologyInOperationAsync(cancellationToken).ConfigureAwait(false);
        var (deadLetterQueue, sources) = ResolveRedrive(plan.Source);
        if (!string.Equals(deadLetterQueue.Arn, plan.DeadLetterArn, StringComparison.Ordinal) ||
            deadLetterQueue.CreatedAt != plan.DeadLetterCreatedAt ||
            !sources.SequenceEqual(plan.SourceQueues, StringComparer.Ordinal))
        {
            throw new InvalidOperationException(
                "The dead-letter queue setup changed since you confirmed. Refresh and start the redrive again. Nothing was moved.");
        }
        // The caller's token also ends with temporary write access: checked last, just before the move is submitted.
        cancellationToken.ThrowIfCancellationRequested();
        // Once sent, the request is not cancelled: AWS may accept the move before an answer arrives, and a cancelled
        // wait would leave that unknown. A cancellation therefore only ever happens before anything was sent. An
        // answer that does not arrive (the network, the SDK's own timeout) leaves the outcome unknown, and says so; a
        // refusal from SQS (AmazonSQSException) is a definite one and is reported as it is.
        StartMessageMoveTaskResponse response;
        try
        {
            response = await Sqs.StartMessageMoveTaskAsync(new StartMessageMoveTaskRequest { SourceArn = deadLetterQueue.Arn },
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not AmazonSQSException &&
                                          exception is OperationCanceledException or TimeoutException or HttpRequestException
                                              or IOException or Amazon.Runtime.AmazonClientException)
        {
            throw UnknownRedriveOutcome(deadLetterQueue.Name, exception);
        }
        return response.TaskHandle ?? string.Empty;
    }

    /// <summary>
    /// The request left and no answer came back. The guidance comes first and the failure after it, in the message
    /// itself: displays show an exception's innermost message, which must not be the transport error alone.
    /// </summary>
    internal static InvalidOperationException UnknownRedriveOutcome(string deadLetterQueue, Exception failure) => new(
        $"The redrive request was sent, but no answer came back, so AWS may be moving the messages of '{deadLetterQueue}' now. " +
        "Check its move tasks in the AWS console (or ListMessageMoveTasks) before starting it again. " +
        $"({failure.GetType().Name}: {failure.GetBaseException().Message})");

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
