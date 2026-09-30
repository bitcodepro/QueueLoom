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
        var remaining = request.LoadAll ? int.MaxValue : request.MaxMessages;
        var result = new List<BrowsedMessage>();
        var receivers = new List<ServiceBusSessionReceiver>();
        try
        {
            while (remaining > 0 && receivers.Count < MaximumSessionsPerBrowse)
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
                long? cursor = null;
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
            }
        }
        finally
        {
            foreach (var receiver in receivers)
            {
                await receiver.DisposeAsync().ConfigureAwait(false);
            }
        }

        return result
            .OrderBy(message => message.SequenceNumber)
            .ToArray();
    }
}
