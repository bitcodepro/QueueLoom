using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Azure;

/// <summary>Peeking active messages of session-enabled queues and subscriptions.</summary>
public sealed partial class AzureServiceBusWorkspace
{
    /// <summary>
    /// Accepts the available sessions one after another and peeks their messages. Accepting a session locks it
    /// (consumers of that session wait) until the browse ends; messages themselves are only peeked, never locked or
    /// changed. Sessions held by a consumer at the moment are skipped by Service Bus.
    /// </summary>
    private async Task<IReadOnlyList<BrowsedMessage>> BrowseSessionsAsync(
        BrowseMessagesRequest request,
        CancellationToken cancellationToken)
    {
        var client = GetMessagingClient();
        var options = new ServiceBusSessionReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            PrefetchCount = 0
        };
        var perSessionLimit = request.LoadAll ? int.MaxValue : request.MaxMessages;
        var result = new List<BrowsedMessage>();
        var receivers = new List<ServiceBusSessionReceiver>();
        var everySessionReadToTheEnd = true;
        Exception? browseError = null;
        try
        {
            // Read each available session's prefix, then merge globally. Stopping after one session can skip
            // lower sequence numbers in another session when the next page uses a global cursor.
            while (receivers.Count <= MaximumSessionsPerBrowse)
            {
                ServiceBusSessionReceiver receiver;
                using (var wait = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    wait.CancelAfter(SessionAcceptWait);
                    try
                    {
                        receiver = request.Source.Kind == ServiceBusEntityKind.Queue
                            ? await client.AcceptNextSessionAsync(request.Source.Name, options, wait.Token).ConfigureAwait(false)
                            : await client.AcceptNextSessionAsync(request.Source.TopicName!, request.Source.Name, options, wait.Token)
                                .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                    {
                        break; // No further session has messages or every other one is in use.
                    }
                    catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.ServiceTimeout)
                    {
                        break;
                    }
                }

                // Kept open until the end, so the same session is not handed out again.
                receivers.Add(receiver);
                if (receivers.Count > MaximumSessionsPerBrowse)
                    throw new InvalidOperationException($"More than {MaximumSessionsPerBrowse} sessions are available. Global page order cannot be guaranteed; browse again when fewer sessions are available.");
                var remaining = perSessionLimit;
                long? cursor = request.FromSequenceNumber;
                while (remaining > 0)
                {
                    var page = await receiver.PeekMessagesAsync(Math.Min(BrowseBatchSize, remaining), cursor, cancellationToken)
                        .ConfigureAwait(false);
                    if (page.Count == 0)
                    {
                        break;
                    }

                    foreach (var message in page.Take(remaining))
                    {
                        result.Add(AzureMessageMapper.FromAzure(message, request.Source, ServiceBusSubQueue.Active));
                    }
                    remaining -= Math.Min(page.Count, remaining);
                    var last = page[^1].SequenceNumber;
                    if (last == long.MaxValue || cursor.HasValue && last < cursor.Value)
                    {
                        break;
                    }
                    cursor = last + 1;
                }
                if (remaining == 0)
                {
                    everySessionReadToTheEnd = false;
                }
            }
        }
        catch (Exception error)
        {
            browseError = error;
            throw;
        }
        finally
        {
            var cleanupErrors = new List<Exception>();
            foreach (var receiver in receivers)
            {
                try { await receiver.DisposeAsync().ConfigureAwait(false); }
                catch (Exception error) { cleanupErrors.Add(error); }
            }
            if (cleanupErrors.Count > 0)
            {
                const string warning = "Could not close all browsed sessions. Some session locks may remain until they expire.";
                var cleanupError = new AggregateException(warning, cleanupErrors);
                // Cleanup must attempt every accepted session, even after cancellation. Keep
                // the primary failure (and its cancellation token) intact for the caller.
                if (browseError is not null) browseError.Data["SessionCleanupErrors"] = cleanupError;
                else
                {
                    // A single-inner aggregate is unwrapped by general exception summaries.
                    // Keep the operator warning separate from potentially sensitive SDK details.
                    cleanupError.Data["SessionCleanupWarning"] = warning;
                    throw cleanupError;
                }
            }
        }

        if (everySessionReadToTheEnd && request.FromSequenceNumber is null)
        {
            await ReportUnreadSessionMessagesAsync(request.Source, result.Select(message => message.State).ToList(),
                cancellationToken).ConfigureAwait(false);
        }

        return result
            .OrderBy(message => message.SequenceNumber)
            .Take(request.LoadAll ? int.MaxValue : request.MaxMessages)
            .ToArray();
    }

    /// <summary>
    /// Service Bus hands out only sessions that no consumer holds and that have an active message, so the browse missed
    /// the messages of held sessions, and of sessions whose messages are all deferred, without a word. The entity's active
    /// count (which includes deferred messages) is compared with what the readable sessions held and any gap is reported.
    /// The count is best effort: without management rights, or on the emulator (which reports no counts), nothing is said.
    /// </summary>
    private async Task ReportUnreadSessionMessagesAsync(ServiceBusEntityReference source,
        IReadOnlyCollection<QueueLoom.Core.ServiceBus.ServiceBusMessageState> read, CancellationToken cancellationToken)
    {
        if (_isEmulator) return;
        long active;
        try
        {
            var administration = GetAdministrationClient();
            active = source.Kind == ServiceBusEntityKind.Queue
                ? (await administration.GetQueueRuntimePropertiesAsync(source.Name, cancellationToken).ConfigureAwait(false)).Value.ActiveMessageCount
                : (await administration.GetSubscriptionRuntimePropertiesAsync(source.TopicName!, source.Name, cancellationToken).ConfigureAwait(false))
                    .Value.ActiveMessageCount;
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return;
        }
        if (UnreadSessionMessageNotice(source.DisplayName, active, read) is { } notice)
        {
            ReportNotice(notice.Title, notice.Details);
        }
    }

    /// <summary>
    /// The notice for a session browse that read messages in the given <paramref name="read"/> states of an entity that
    /// reports <paramref name="active"/> active messages, or null when nothing is missing. Service Bus counts deferred
    /// messages as active, so every read message but the scheduled ones is compared with it. Only the gap is known, not
    /// its cause, so no cause is named as certain and no deferred count is made up.
    /// </summary>
    internal static (string Title, string Details)? UnreadSessionMessageNotice(
        string displayName, long active, IEnumerable<QueueLoom.Core.ServiceBus.ServiceBusMessageState> read)
    {
        var readNonScheduled = read.Count(state => state != QueueLoom.Core.ServiceBus.ServiceBusMessageState.Scheduled);
        if (active <= readNonScheduled) return null;
        var culture = System.Globalization.CultureInfo.CurrentCulture;
        return ("Session browse may be incomplete",
            $"{displayName} reports {active.ToString("N0", culture)} active messages, but this browse read " +
            $"{readNonScheduled.ToString("N0", culture)}, so {(active - readNonScheduled).ToString("N0", culture)} message(s) the counters " +
            "report were not listed. Possible causes: sessions held by another consumer, sessions whose messages are all deferred " +
            "(Service Bus does not hand those out to a browse), or messages changing during the browse. Browse again to check.");
    }
}
