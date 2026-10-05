using System.Globalization;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>
/// Where reading a message counts as a delivery, a warning next to the browse actions. Facts behind the wording:
/// <list type="bullet">
/// <item>Amazon SQS has no peek; QueueLoom receives and releases. ApproximateReceiveCount is "the number of times a
/// message has been received across all queues but not deleted" (SQS API Reference, ReceiveMessage), and a redrive
/// policy moves a message to its dead-letter queue after maxReceiveCount receives (SQS Developer Guide, "Using
/// dead-letter queues in Amazon SQS"). SNS subscriptions are read through their SQS queue.</item>
/// <item>Google Pub/Sub has no peek; QueueLoom pulls and releases with ModifyAckDeadline 0, which is a NACK. With a
/// dead-letter policy, deliveryAttempt is "1 + (number of NACKs) + (number of ack_deadline exceeds)" (REST reference,
/// ReceivedMessage), and after maxDeliveryAttempts (5 to 100, approximate) the message is forwarded to the dead-letter
/// topic; without a policy Pub/Sub does not count attempts ("Handle message failures").</item>
/// <item>RabbitMQ: QueueLoom reads with basic.get and returns with basic.nack requeue. Up to RabbitMQ 4.2 a quorum
/// queue counts every requeue toward its delivery-limit (20 by default since 4.0); past it the message is dead-lettered
/// or dropped. From 4.3 basic.nack no longer counts (Quorum Queues docs, poison message handling; 4.3 release blog).
/// Classic queues only set the redelivered flag.</item>
/// <item>Azure Service Bus peeks a disconnected snapshot without locks ("Browse or peek messages"), and Kafka reads
/// by offset without a consumer group or commit: neither counts as a delivery, so they get no warning.</item>
/// </list>
/// </summary>
public sealed partial class MainWindowViewModel
{
    /// <summary>
    /// The warning for what the Messages page reads next: the selected dead-letter source ("Peek selected source"), or
    /// the open list ("Load next 100" reads it again) when no source is selected. Empty where reading is not a delivery.
    /// </summary>
    public string BrowseDeliveryNote
    {
        get
        {
            if (SelectedDlqSource is { } source)
            {
                var profile = Profiles.FirstOrDefault(item => item.Id == source.ProfileId);
                return profile is null ? string.Empty : DescribeBrowseDelivery(profile, source.Entity, source.Snapshot.SubQueue);
            }
            return _browseProfile is { } browsed && _browseSource is { } open
                ? DescribeBrowseDelivery(browsed, open, _browseSubQueue)
                : string.Empty;
        }
    }

    public bool HasBrowseDeliveryNote => BrowseDeliveryNote.Length > 0;

    /// <summary>The warning for "View active" and "View DLQ" in Explorer: the selected entity of the connected environment.</summary>
    public string SelectedEntityDeliveryNote
    {
        get
        {
            if (SelectedEntity is not { } entity || ConnectedProfileId is not { } connectedId)
            {
                return string.Empty;
            }
            var profile = Profiles.FirstOrDefault(item => item.Id == connectedId);
            if (profile is null)
            {
                return string.Empty;
            }
            var active = DescribeBrowseDelivery(profile, entity.Reference, ServiceBusSubQueue.Active);
            return active.Length > 0 ? active : DescribeBrowseDelivery(profile, entity.Reference, ServiceBusSubQueue.DeadLetter);
        }
    }

    public bool HasSelectedEntityDeliveryNote => SelectedEntityDeliveryNote.Length > 0;

    private void NotifyBrowseDeliveryNotes()
    {
        OnPropertyChanged(nameof(BrowseDeliveryNote));
        OnPropertyChanged(nameof(HasBrowseDeliveryNote));
        OnPropertyChanged(nameof(SelectedEntityDeliveryNote));
        OnPropertyChanged(nameof(HasSelectedEntityDeliveryNote));
    }

    /// <summary>
    /// What a finished read counted as, for the status line, where that is certain: every SQS receive, and a Pub/Sub
    /// read of a subscription with a dead-letter policy. Empty otherwise (RabbitMQ depends on the broker version).
    /// </summary>
    private string BrowseCountedSuffix(ProfileItemViewModel profile, ServiceBusEntityReference source, ServiceBusSubQueue subQueue)
    {
        if (profile.Provider == MessagingProvider.AmazonSqsSns)
        {
            return "; each read counts as a receive";
        }
        var subscription = profile.Provider == MessagingProvider.GooglePubSub && subQueue == ServiceBusSubQueue.Active &&
                           profile.Id == ConnectedProfileId
            ? _topology?.Topics.SelectMany(topic => topic.Subscriptions).FirstOrDefault(item => item.Reference == source)
            : null;
        return subscription?.MaxDeliveryCount is not null ? "; each read counts as a delivery attempt" : string.Empty;
    }

    private string DescribeBrowseDelivery(ProfileItemViewModel profile, ServiceBusEntityReference source, ServiceBusSubQueue subQueue)
    {
        // Only the connected environment's topology is at hand; for another one the wording stays general.
        var topology = profile.Id == ConnectedProfileId ? _topology : null;
        return DescribeBrowseDelivery(profile.Provider, topology, source, subQueue) ?? string.Empty;
    }

    /// <summary>The warning for reading <paramref name="source"/>, or null where reading does not count as a delivery.</summary>
    internal static string? DescribeBrowseDelivery(
        MessagingProvider provider,
        ServiceBusTopology? topology,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue)
    {
        var queue = source.Kind == ServiceBusEntityKind.Queue
            ? topology?.Queues.FirstOrDefault(item => item.Reference == source)
            : null;
        var subscription = source.Kind == ServiceBusEntityKind.Subscription
            ? topology?.Topics.SelectMany(topic => topic.Subscriptions).FirstOrDefault(item => item.Reference == source)
            : null;
        // Dead letters are read from another queue (SQS, RabbitMQ); its own settings apply, when this topology has it.
        var deadLetterQueue = subQueue != ServiceBusSubQueue.Active && queue?.DeadLetterQueueName is { } name
            ? topology!.Queues.FirstOrDefault(item => item.Name == name)
            : null;
        var known = subQueue == ServiceBusSubQueue.Active ? queue is not null || subscription is not null : deadLetterQueue is not null;
        var limit = subQueue == ServiceBusSubQueue.Active
            ? queue?.MaxDeliveryCount ?? subscription?.MaxDeliveryCount
            : deadLetterQueue?.MaxDeliveryCount;

        switch (provider)
        {
            case MessagingProvider.AmazonSqsSns:
                {
                    const string counts = "Browsing on Amazon SQS counts as a receive: each look raises a message's receive count";
                    if (limit is { } receives)
                    {
                        return subQueue == ServiceBusSubQueue.Active
                            ? $"{counts}, and the redrive policy moves it to the dead-letter queue after {Times(receives, "receive")}."
                            : $"{counts}, and this dead-letter queue's own redrive policy moves it on after {Times(receives, "receive")}.";
                    }
                    return known || subQueue != ServiceBusSubQueue.Active
                        ? $"{counts}."
                        : $"{counts}, and a redrive policy can move it to the dead-letter queue after its maximum receives.";
                }
            case MessagingProvider.GooglePubSub:
                if (limit is { } attempts && subQueue == ServiceBusSubQueue.Active)
                {
                    return "Browsing on Google Pub/Sub counts as a delivery: each look raises a message's delivery attempts, and the " +
                           $"dead-letter policy forwards it to the dead-letter topic after about {Times(attempts, "attempt")}.";
                }
                // A subscription without a dead-letter policy: Pub/Sub does not count delivery attempts at all.
                return known && subQueue == ServiceBusSubQueue.Active
                    ? null
                    : "Browsing on Google Pub/Sub counts as a delivery: with a dead-letter policy, each look raises a message's " +
                      "delivery attempts and can forward it to the dead-letter topic.";
            case MessagingProvider.RabbitMq:
                {
                    var read = subQueue == ServiceBusSubQueue.Active ? queue : deadLetterQueue;
                    if (read is { CountsRequeues: false })
                    {
                        return null; // A classic queue, or a quorum queue without a delivery limit: only "redelivered" is set.
                    }
                    if (read is null)
                    {
                        return "On RabbitMQ 4.2 and earlier, browsing a quorum queue counts as a delivery: past its delivery limit " +
                               "the message is dead-lettered or dropped.";
                    }
                    var past = limit is { } deliveries
                        ? $"past the delivery limit of {deliveries.ToString("N0", CultureInfo.CurrentCulture)}"
                        : "past its delivery limit (20 by default since RabbitMQ 4.0)";
                    return "Browsing a RabbitMQ quorum queue requeues each message, and RabbitMQ 4.2 and earlier count that as a " +
                           $"delivery: {past} the message is dead-lettered or dropped.";
                }
            default:
                // Azure Service Bus peeks without locks; Kafka reads by offset and commits nothing.
                return null;
        }

        static string Times(int value, string noun) =>
            $"{value.ToString("N0", CultureInfo.CurrentCulture)} {noun}{(value == 1 ? string.Empty : "s")}";
    }
}
