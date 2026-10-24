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
            await ReportUnreadSessionMessagesAsync(request.Source,
                result.Count(message => message.State is QueueLoom.Core.ServiceBus.ServiceBusMessageState.Active or QueueLoom.Core.ServiceBus.ServiceBusMessageState.Unknown),
                result.Count(message => message.State == QueueLoom.Core.ServiceBus.ServiceBusMessageState.Deferred),
                cancellationToken).ConfigureAwait(false);
        }

        return result
            .OrderBy(message => message.SequenceNumber)
            .Take(request.LoadAll ? int.MaxValue : request.MaxMessages)
            .ToArray();
    }

    /// <summary>
    /// Service Bus hands out only sessions that no consumer holds and that have an active message, so the browse missed
    /// the messages of held sessions, and the deferred messages of sessions with no active message left, without a word.
    /// The entity's counts are compared with what the readable sessions held and any gap is reported. The counts are best
    /// effort: without management rights, or on the emulator (which reports no counts), nothing is said.
    /// </summary>
    private async Task ReportUnreadSessionMessagesAsync(ServiceBusEntityReference source, int readActive, int readDeferred,
        CancellationToken cancellationToken)
    {
        if (_isEmulator) return;
        long active;
        long deferred;
        try
        {
            var administration = GetAdministrationClient();
            if (source.Kind == ServiceBusEntityKind.Queue)
            {
                var properties = (await administration.GetQueueRuntimePropertiesAsync(source.Name, cancellationToken).ConfigureAwait(false)).Value;
                (active, deferred) = (properties.ActiveMessageCount, DeferredCount(properties.TotalMessageCount, properties.ActiveMessageCount,
                    properties.ScheduledMessageCount, properties.DeadLetterMessageCount, properties.TransferMessageCount, properties.TransferDeadLetterMessageCount));
            }
            else
            {
                var properties = (await administration.GetSubscriptionRuntimePropertiesAsync(source.TopicName!, source.Name, cancellationToken)
                    .ConfigureAwait(false)).Value;
                // A subscription has no scheduled count: scheduled messages wait in the topic.
                (active, deferred) = (properties.ActiveMessageCount, DeferredCount(properties.TotalMessageCount, properties.ActiveMessageCount,
                    scheduled: 0, properties.DeadLetterMessageCount, properties.TransferMessageCount, properties.TransferDeadLetterMessageCount));
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return;
        }
        foreach (var (title, details) in UnreadSessionMessageNotices(source.DisplayName, active, deferred, readActive, readDeferred))
        {
            ReportNotice(title, details);
        }
    }

    /// <summary>
    /// The SDK's runtime properties have no deferred count; deferred messages are the part of the total that is neither
    /// active, scheduled, dead-lettered nor in transfer. The counts are read at slightly different moments, so never below zero.
    /// </summary>
    internal static long DeferredCount(long total, long active, long scheduled, long deadLetter, long transfer, long transferDeadLetter) =>
        Math.Max(0, total - active - scheduled - deadLetter - transfer - transferDeadLetter);

    /// <summary>
    /// The notices for a session browse that read <paramref name="readActive"/> active and <paramref name="readDeferred"/>
    /// deferred messages of an entity that reports <paramref name="active"/> and <paramref name="deferred"/>. Only the
    /// discrepancy is known, not its cause: messages may also have arrived or been settled while the browse ran.
    /// </summary>
    internal static IReadOnlyList<(string Title, string Details)> UnreadSessionMessageNotices(
        string displayName, long active, long deferred, int readActive, int readDeferred)
    {
        var culture = System.Globalization.CultureInfo.CurrentCulture;
        var notices = new List<(string, string)>();
        if (active > readActive)
        {
            notices.Add(("Session browse may be incomplete",
                $"{displayName} reports {active.ToString("N0", culture)} active messages, " +
                $"but this browse read {readActive.ToString("N0", culture)}. Possible reasons: another " +
                "consumer holds some sessions (Service Bus does not hand those out), or messages arrived while browsing. Browse again to see them."));
        }
        if (deferred > readDeferred)
        {
            var missing = deferred - readDeferred;
            notices.Add(("Deferred session messages not listed",
                $"{displayName} reports {deferred.ToString("N0", culture)} deferred messages, but this browse read " +
                $"{readDeferred.ToString("N0", culture)}. Service Bus hands out only sessions that have an active message, so the " +
                $"{missing.ToString("N0", culture)} deferred message(s) of sessions with no active message left (or of sessions another " +
                "consumer holds) cannot be listed here. They stay in the entity; the application that deferred them can receive them by " +
                "sequence number, and they appear here once their session gets an active message."));
        }
        return notices;
    }
}
