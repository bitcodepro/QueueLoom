using QueueLoom.Core.Abstractions;

namespace QueueLoom.Core.ServiceBus;

public enum ResendMode
{
    /// <summary>Send a copy; the original stays where it is.</summary>
    Copy,

    /// <summary>Send a copy, then back up the original and remove it from its dead-letter queue.</summary>
    Move
}

/// <summary>One message to resend: the message it came from, where it goes and what is sent (possibly edited).</summary>
public sealed record ResendItem(BrowsedMessage Original, ServiceBusEntityReference Destination, MessageDraft Message)
{
    public DeadLetterMessageKey Key =>
        new(Original.Source, Original.SubQueue, Original.SequenceNumber, Original.Properties.MessageId);

    /// <summary>Assign once when preparing the operation, so SDK retries and scheduled sends reuse this ID.</summary>
    public ResendItem WithNewMessageId() => this with
    {
        Message = new MessageDraft(Message.Body, Message.Properties with { MessageId = Guid.NewGuid().ToString("N") },
            Message.ApplicationProperties) { KafkaEnvelope = Message.KafkaEnvelope }
    };
}

public enum ResendOutcome
{
    /// <summary>The copy was sent; the original was left in place (copy mode).</summary>
    Sent,

    /// <summary>The copy was sent and the original was backed up and removed.</summary>
    Moved,

    /// <summary>The copy was sent but the original could not be removed; it is still in the dead-letter queue.</summary>
    SentOriginalKept,

    Failed,

    Cancelled
}

public sealed record ResendItemResult(ResendItem Item, ResendOutcome Outcome, string? Detail = null);

public sealed record ResendProgress(int Processed, int Total, int Failed);

public sealed record ResendResult(IReadOnlyList<ResendItemResult> Items, string? BackupDirectory)
{
    public int SentCount => Items.Count(item => item.Outcome is ResendOutcome.Sent or ResendOutcome.Moved or ResendOutcome.SentOriginalKept);

    public int MovedCount => Count(ResendOutcome.Moved);

    public int OriginalsKeptCount => Count(ResendOutcome.SentOriginalKept);

    public int FailedCount => Count(ResendOutcome.Failed);

    public int CancelledCount => Count(ResendOutcome.Cancelled);

    private int Count(ResendOutcome outcome) => Items.Count(item => item.Outcome == outcome);
}

/// <summary>
/// Resends dead-lettered (or peeked) messages. Copy mode only sends. Move mode sends every message first and only
/// then removes the originals that were sent, through the backed-up delete, so a failed send never loses a message
/// and a removed original always has a backup.
/// </summary>
public static class DeadLetterResender
{
    public const int MaximumMessages = DeleteDeadLetterMessagesRequest.MaximumMessages;

    /// <summary>
    /// Where a message goes "back to": its queue, or the topic of its subscription (which delivers the copy to every
    /// subscription of that topic whose filter matches).
    /// </summary>
    public static ServiceBusEntityReference OriginalDestination(ServiceBusEntityReference source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.Kind == ServiceBusEntityKind.Subscription
            ? ServiceBusEntityReference.Topic(source.TopicName!)
            : ServiceBusEntityReference.Queue(source.Name);
    }

    public static async Task<ResendResult> ResendAsync(
        IServiceBusWorkspace workspace,
        IReadOnlyList<ResendItem> items,
        ResendMode mode,
        int messagesPerSecond = 0,
        IProgress<ResendProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(workspace);
        ArgumentNullException.ThrowIfNull(items);
        if (items.Count == 0)
        {
            throw new ArgumentException("Choose at least one message to resend.", nameof(items));
        }
        if (items.Count > MaximumMessages)
        {
            throw new ArgumentException($"At most {MaximumMessages:N0} messages can be resent at once.", nameof(items));
        }
        if (mode == ResendMode.Move && items.Any(item => !item.Original.IsDeadLetter))
        {
            throw new ArgumentException("Only dead-lettered messages can be moved. Use copy mode for active messages.", nameof(items));
        }
        ArgumentOutOfRangeException.ThrowIfNegative(messagesPerSecond);
        EnsureSafeMessageIds(workspace.ConnectedProvider, items, mode);

        var results = new ResendItemResult?[items.Count];
        var delay = messagesPerSecond > 0 ? TimeSpan.FromSeconds(1.0 / messagesPerSecond) : TimeSpan.Zero;
        var failed = 0;
        for (var index = 0; index < items.Count; index++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                results[index] = new ResendItemResult(items[index], ResendOutcome.Cancelled);
                continue;
            }

            try
            {
                if (index > 0 && delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
                await workspace.SendMessageAsync(new SendMessageRequest(items[index].Destination, items[index].Message), cancellationToken)
                    .ConfigureAwait(false);
                results[index] = new ResendItemResult(items[index], ResendOutcome.Sent);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // A send interrupted by cancellation may or may not have reached the service: keep the original.
                results[index] = new ResendItemResult(items[index], ResendOutcome.Cancelled);
            }
            catch (Exception exception)
            {
                failed++;
                results[index] = new ResendItemResult(items[index], ResendOutcome.Failed, exception.GetBaseException().Message);
            }

            progress?.Report(new ResendProgress(index + 1, items.Count, failed));
        }

        string? backupDirectory = null;
        var sent = Enumerable.Range(0, items.Count).Where(index => results[index]!.Outcome == ResendOutcome.Sent).ToArray();
        if (mode == ResendMode.Move && sent.Length > 0)
        {
            // Copies are out; the originals must go too, even when the operator cancelled the rest.
            try
            {
                var deletion = await workspace.DeleteDeadLetterMessagesAsync(
                        new DeleteDeadLetterMessagesRequest(sent.Select(index => items[index].Key)),
                        CancellationToken.None)
                    .ConfigureAwait(false);
                backupDirectory = deletion.BackupDirectory;
                var outcomes = deletion.Messages
                    .GroupBy(message => message.Message)
                    .ToDictionary(group => group.Key, group => group.First());
                foreach (var index in sent)
                {
                    results[index] = outcomes.TryGetValue(items[index].Key, out var outcome) &&
                                     outcome.Outcome == DeadLetterMessageDeletionOutcome.Deleted
                        ? new ResendItemResult(items[index], ResendOutcome.Moved)
                        : new ResendItemResult(items[index], ResendOutcome.SentOriginalKept,
                            outcome?.Detail ?? DescribeKept(outcome?.Outcome));
                }
            }
            catch (Exception exception)
            {
                foreach (var index in sent)
                {
                    results[index] = new ResendItemResult(items[index], ResendOutcome.SentOriginalKept,
                        $"The copy was sent, but removing the original failed: {exception.GetBaseException().Message}");
                }
            }
        }

        return new ResendResult(results.Select(result => result!).ToArray(), backupDirectory);
    }

    public static void EnsureSafeMessageIds(QueueLoom.Core.Profiles.MessagingProvider? provider,
        IReadOnlyList<ResendItem> items, ResendMode mode)
    {
        if (provider == QueueLoom.Core.Profiles.MessagingProvider.RabbitMq && mode == ResendMode.Move)
            throw new NotSupportedException("RabbitMQ cannot safely identify a previously reviewed delivery for removal. Use Copy to retain the originals.");
        if (provider is not (QueueLoom.Core.Profiles.MessagingProvider.AzureServiceBus or QueueLoom.Core.Profiles.MessagingProvider.AmazonSqsSns) || mode != ResendMode.Move) return;
        var originalIds = items.Select(item => item.Original.Properties.MessageId).Where(id => !string.IsNullOrWhiteSpace(id)).ToHashSet(StringComparer.Ordinal);
        var sentIds = new HashSet<string>(StringComparer.Ordinal);
        if (items.Any(item => string.IsNullOrWhiteSpace(item.Message.Properties.MessageId) ||
            originalIds.Contains(item.Message.Properties.MessageId) || !sentIds.Add(item.Message.Properties.MessageId)))
            throw new InvalidOperationException($"{(provider == QueueLoom.Core.Profiles.MessagingProvider.AzureServiceBus ? "Azure" : "SQS/SNS FIFO")} moves require distinct new Message IDs. Duplicate detection may accept a preserved ID but suppress the replacement. Choose new IDs, or copy while keeping the originals.");
    }

    private static string DescribeKept(DeadLetterMessageDeletionOutcome? outcome) => outcome switch
    {
        DeadLetterMessageDeletionOutcome.NotFound => "The copy was sent, but the original was no longer in the dead-letter queue.",
        DeadLetterMessageDeletionOutcome.Cancelled => "The copy was sent; removing the original was cancelled.",
        _ => "The copy was sent, but the original could not be removed."
    };
}
