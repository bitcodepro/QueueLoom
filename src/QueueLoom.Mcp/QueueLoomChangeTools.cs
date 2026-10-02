using System.ComponentModel;
using System.Globalization;
using System.Text;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;

namespace QueueLoom.Mcp;

/// <summary>
/// Tools that change messages. Every call is shown to a person for approval before anything happens;
/// a declined or unanswered request returns without touching the namespace.
/// </summary>
[McpServerToolType]
public sealed class QueueLoomChangeTools(McpWorkspaceSession session, IOperationApprover approver)
{
    private const string EnvironmentDescription =
        "Saved environment name (see list_environments). Optional when only one environment is saved.";

    private const string ReasonDescription =
        "Why this change is needed, in one or two sentences. Shown to the person who approves it.";

    [McpServerTool(Name = "delete_dead_letter_messages", Title = "Delete specific dead-letter messages",
        Destructive = true, Idempotent = true, OpenWorld = false)]
    [Description("Backs up and permanently deletes exactly the listed dead-lettered messages (e.g. results of search_dead_letters). " +
                 "Other messages stay in the queue. Requires approval by the user in QueueLoom; at most 1,000 messages per call.")]
    public Task<ChangeResult> DeleteDeadLetterMessagesAsync(
        McpServer server,
        [Description("Messages to delete: entity ('queue' or 'topic/subscription'), subQueue ('dlq' or 'transfer-dlq'), " +
                     "sequenceNumber and messageId exactly as returned by search_dead_letters or peek_messages.")]
        MessageSelection[] messages,
        [Description(ReasonDescription)] string reason,
        [Description(EnvironmentDescription)] string? environment = null,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            if (messages is not { Length: > 0 })
            {
                throw new McpException("List at least one message to delete.");
            }

            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var topology = await session.ReadAsync(profile,
                    (workspace, token) => workspace.GetTopologyAsync(forceRefresh: false, token), cancellationToken)
                .ConfigureAwait(false);
            if (!topology.CanDeleteSelectedMessages)
            {
                throw new McpException(
                    $"{profile.Provider.DisplayName()} cannot safely delete previously reviewed single messages. Use an approved source purge with purge_dead_letters instead.");
            }
            DeleteDeadLetterMessagesRequest request;
            try
            {
                request = new DeleteDeadLetterMessagesRequest(messages.Select(message => new DeadLetterMessageKey(
                    EntityResolver.Resolve(topology, message.Entity, requireMessageSource: true),
                    McpMapping.ParseSubQueue(message.SubQueue),
                    message.SequenceNumber,
                    string.IsNullOrWhiteSpace(message.MessageId) ? null : message.MessageId)));
            }
            catch (ArgumentException exception)
            {
                throw new McpException(exception.Message);
            }

            var perQueue = request.BySubQueue
                .Select(group => $"• {McpMapping.EntityName(group.Key.Source)} ({McpMapping.SubQueueName(group.Key.SubQueue)}): {group.Count()}")
                .ToArray();
            var examples = request.Messages.Take(10)
                .Select(message => $"  #{message.SequenceNumber} {message.MessageId ?? "(no Message ID)"}");
            var decision = await RequestApprovalAsync(server, profile, "Delete dead-letter messages",
                $"{request.Messages.Count} dead-lettered message(s) will be backed up locally and then permanently deleted. " +
                "Other messages stay in the queue.\n\n" +
                string.Join("\n", perQueue.Take(20)) + (perQueue.Length > 20 ? $"\n… and {perQueue.Length - 20} more queues" : string.Empty) +
                "\n\nFirst messages:\n" + string.Join("\n", examples),
                reason, cancellationToken).ConfigureAwait(false);
            if (!decision.Approved)
            {
                return Declined(profile, "Delete dead-letter messages", decision);
            }

            var result = await session.WriteAsync(profile,
                    (workspace, token) => workspace.DeleteDeadLetterMessagesAsync(request, token), cancellationToken)
                .ConfigureAwait(false);
            var summary = $"{result.DeletedCount} of {request.Messages.Count} deleted" +
                          (result.NotFoundCount > 0 ? $", {result.NotFoundCount} not found" : string.Empty) +
                          (result.FailedCount > 0 ? $", {result.FailedCount} failed" : string.Empty) +
                          (result.CancelledCount > 0 ? $", {result.CancelledCount} not processed" : string.Empty) + ".";
            session.Record(result.FailedCount == 0 ? "Warning" : "Error", "Deleted dead-letter messages",
                $"{summary} Reason: {reason}. Backup: {result.BackupDirectory}", profile);
            return new ChangeResult(profile.Name, true, summary, result.BackupDirectory,
                result.Messages.Select(message =>
                    $"{McpMapping.EntityName(message.Message.Source)} #{message.Message.SequenceNumber}: {message.Outcome}" +
                    (message.Detail is null ? string.Empty : $" ({message.Detail})")).ToArray());
        });

    [McpServerTool(Name = "purge_dead_letters", Title = "Back up and purge a dead-letter queue",
        Destructive = true, Idempotent = false, OpenWorld = false)]
    [Description("Backs up and deletes up to maxMessages messages from one dead-letter queue, oldest first. " +
                 "Requires approval by the user in QueueLoom.")]
    public Task<ChangeResult> PurgeDeadLettersAsync(
        McpServer server,
        [Description("Queue name, or 'topic/subscription'.")] string entity,
        [Description("How many messages at most to delete, 1-10,000.")] int maxMessages,
        [Description(ReasonDescription)] string reason,
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("'dlq' (default) or 'transfer-dlq'.")] string subQueue = "dlq",
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            if (maxMessages is < 1 or > 10_000)
            {
                throw new McpException("maxMessages must be between 1 and 10,000.");
            }

            var queue = McpMapping.ParseSubQueue(subQueue);
            if (queue == ServiceBusSubQueue.Active)
            {
                throw new McpException("Only dead-letter queues can be purged.");
            }

            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var topology = await session.ReadAsync(profile,
                    (workspace, token) => workspace.GetTopologyAsync(forceRefresh: true, token), cancellationToken)
                .ConfigureAwait(false);
            var source = EntityResolver.Resolve(topology, entity, requireMessageSource: true);
            var decision = await RequestApprovalAsync(server, profile, "Purge a dead-letter queue",
                $"Up to {maxMessages:N0} message(s) from {McpMapping.EntityName(source)} ({McpMapping.SubQueueName(queue)}) " +
                "will be backed up locally and then permanently deleted, oldest first. New arrivals can be included up to the limit.",
                reason, cancellationToken).ConfigureAwait(false);
            if (!decision.Approved)
            {
                return Declined(profile, "Purge a dead-letter queue", decision);
            }

            var result = await session.WriteAsync(profile, (workspace, token) => workspace.PurgeDeadLettersAsync(
                    new DeadLetterPurgeRequest([new DeadLetterPurgeTarget(source, queue)], batchSize: 20, maximumMessagesPerSubQueue: maxMessages),
                    token), cancellationToken)
                .ConfigureAwait(false);
            var summary = $"{result.DeletedCount:N0} message(s) backed up and deleted" +
                          (result.HasFailures ? $"; {result.Sources.First(item => !item.IsSuccessful).Error}" : ".");
            session.Record(result.HasFailures ? "Error" : "Warning", "Purged dead letters",
                $"{McpMapping.EntityName(source)}: {summary} Reason: {reason}. Backup: {result.BackupDirectory}", profile, source);
            return new ChangeResult(profile.Name, true, summary, result.BackupDirectory, []);
        });

    [McpServerTool(Name = "send_message", Title = "Send a message", Destructive = false, Idempotent = false, OpenWorld = false)]
    [Description("Sends one message to a queue or topic. Requires approval by the user in QueueLoom.")]
    public Task<ChangeResult> SendMessageAsync(
        McpServer server,
        [Description("Queue or topic name.")] string destination,
        [Description("Message body.")] string body,
        [Description(ReasonDescription)] string reason,
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("'Text' (default), 'Json' or 'Base64'.")] string bodyFormat = "Text",
        [Description("Content type, e.g. application/json.")] string? contentType = null,
        [Description("Subject (label).")] string? subject = null,
        [Description("Correlation ID.")] string? correlationId = null,
        [Description("Message ID; a new one is generated when omitted.")] string? messageId = null,
        [Description("String application properties.")] Dictionary<string, string>? applicationProperties = null,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            if (!Enum.TryParse<MessageBodyFormat>(bodyFormat, ignoreCase: true, out var format))
            {
                throw new McpException("bodyFormat must be 'Text', 'Json' or 'Base64'.");
            }

            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var topology = await session.ReadAsync(profile,
                    (workspace, token) => workspace.GetTopologyAsync(forceRefresh: false, token), cancellationToken)
                .ConfigureAwait(false);
            var target = EntityResolver.Resolve(topology, destination, requireMessageSource: false);
            if (!target.CanSend)
            {
                throw new McpException($"'{destination}' is a subscription; send to its topic instead.");
            }

            var draft = new MessageDraft(
                new EditableMessageBody(body ?? string.Empty, format),
                new EditableMessageProperties(
                    MessageId: string.IsNullOrWhiteSpace(messageId) ? Guid.NewGuid().ToString("N") : messageId,
                    CorrelationId: correlationId,
                    ContentType: contentType ?? (format == MessageBodyFormat.Json ? "application/json" : null),
                    Subject: subject),
                applicationProperties?.Select(pair => new MessageApplicationProperty(pair.Key, ApplicationPropertyType.String, pair.Value)));
            var validation = MessageDraftValidator.Validate(draft);
            if (!validation.IsValid)
            {
                throw new McpException(string.Join(" ", validation.Errors.Select(error => error.Message)));
            }

            var preview = draft.Body.Content.Length > 600 ? draft.Body.Content[..600] + "…" : draft.Body.Content;
            var decision = await RequestApprovalAsync(server, profile, "Send a message",
                $"One message will be sent to {McpMapping.EntityName(target)}.\n" +
                $"Message ID: {draft.Properties.MessageId}\nSubject: {subject ?? "—"}\nCorrelation ID: {correlationId ?? "—"}\n" +
                $"Body ({format}, {Encoding.UTF8.GetByteCount(draft.Body.Content).ToString("N0", CultureInfo.InvariantCulture)} bytes):\n{preview}",
                reason, cancellationToken).ConfigureAwait(false);
            if (!decision.Approved)
            {
                return Declined(profile, "Send a message", decision);
            }

            await session.WriteAsync(profile, async (workspace, token) =>
            {
                await workspace.SendMessageAsync(new SendMessageRequest(target, draft), token).ConfigureAwait(false);
                return true;
            }, cancellationToken).ConfigureAwait(false);
            session.Record("Success", "Sent a message",
                $"{McpMapping.EntityName(target)} · Message ID {draft.Properties.MessageId}. Reason: {reason}", profile, target);
            return new ChangeResult(profile.Name, true,
                $"Sent message {draft.Properties.MessageId} to {McpMapping.EntityName(target)}.", null, []);
        });

    [McpServerTool(Name = "resend_dead_letters", Title = "Resend dead-letter messages",
        Destructive = true, Idempotent = false, OpenWorld = false)]
    [Description("Resends the listed dead-lettered messages (e.g. results of search_dead_letters) with new Message IDs by default: back to the queue " +
                 "or topic they came from, or to 'destination'. mode 'copy' leaves the originals in the dead-letter queue; " +
                 "mode 'move' sends first, then backs up and removes only the originals that were sent. " +
                 "Requires approval by the user in QueueLoom; at most 1,000 messages per call.")]
    public Task<ChangeResult> ResendDeadLettersAsync(
        McpServer server,
        [Description("Messages to resend: entity ('queue' or 'topic/subscription'), subQueue ('dlq' or 'transfer-dlq'), " +
                     "sequenceNumber and messageId exactly as returned by search_dead_letters or peek_messages.")]
        MessageSelection[] messages,
        [Description("'copy' (the originals stay in the dead-letter queue) or 'move' (the originals are backed up and removed after sending).")]
        string mode,
        [Description(ReasonDescription)] string reason,
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("Queue or topic to send to. When omitted, each message goes back to its queue, or to the topic of its subscription.")]
        string? destination = null,
        [Description("Send at most this many messages per second, 1-100; 0 (default) sends as fast as possible.")]
        int messagesPerSecond = 0,
        [Description("Preserve original Message IDs instead of assigning distinct new IDs. Duplicate detection may suppress delivery. Azure move requires new IDs.")]
        bool preserveMessageIds = false,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            if (messages is not { Length: > 0 })
            {
                throw new McpException("List at least one message to resend.");
            }
            if (messages.Length > DeadLetterResender.MaximumMessages)
            {
                throw new McpException($"At most {DeadLetterResender.MaximumMessages:N0} messages can be resent per call.");
            }
            var resendMode = mode?.Trim().ToLowerInvariant() switch
            {
                "copy" => ResendMode.Copy,
                "move" => ResendMode.Move,
                _ => throw new McpException("mode must be 'copy' or 'move'.")
            };
            if (messagesPerSecond is < 0 or > 100)
            {
                throw new McpException("messagesPerSecond must be between 0 and 100.");
            }

            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var (target, originals, missing) = await session.ReadAsync(profile, async (workspace, token) =>
            {
                var topology = await workspace.GetTopologyAsync(forceRefresh: false, token).ConfigureAwait(false);
                if (resendMode == ResendMode.Move && !topology.CanDeleteSelectedMessages)
                {
                    throw new McpException(
                        $"{profile.Provider.DisplayName()} cannot remove single messages, so 'move' is not available. Use mode 'copy'.");
                }
                ServiceBusEntityReference? to = null;
                if (!string.IsNullOrWhiteSpace(destination))
                {
                    to = EntityResolver.Resolve(topology, destination, requireMessageSource: false);
                    if (!to.CanSend)
                    {
                        throw new McpException($"'{destination}' is a subscription; send to its topic instead.");
                    }
                }

                DeadLetterMessageKey[] keys;
                try
                {
                    keys = messages.Select(message => new DeadLetterMessageKey(
                            EntityResolver.Resolve(topology, message.Entity, requireMessageSource: true),
                            McpMapping.ParseSubQueue(message.SubQueue),
                            message.SequenceNumber,
                            string.IsNullOrWhiteSpace(message.MessageId) ? null : message.MessageId))
                        .Distinct()
                        .ToArray();
                }
                catch (ArgumentException exception)
                {
                    throw new McpException(exception.Message);
                }
                if (keys.Any(key => key.SubQueue == ServiceBusSubQueue.Active))
                {
                    throw new McpException("Only dead-lettered messages ('dlq' or 'transfer-dlq') can be resent with this tool.");
                }

                var (found, notFound) = await FindMessagesAsync(workspace, keys, token).ConfigureAwait(false);
                return (to, found, notFound);
            }, cancellationToken).ConfigureAwait(false);

            if (originals.Count == 0)
            {
                throw new McpException(
                    "None of the listed messages is in its dead-letter queue any more. Run search_dead_letters or peek_messages again.");
            }
            var tooLarge = originals.Where(message => message.IsBodyTruncated).ToArray();
            if (tooLarge.Length > 0)
            {
                throw new McpException(
                    $"{tooLarge.Length} message(s) have bodies too large to resend here (for example #{tooLarge[0].SequenceNumber}). " +
                    "Resend them from the QueueLoom app instead.");
            }

            var items = originals
                .Select(message => new ResendItem(message, target ?? DeadLetterResender.OriginalDestination(message.Source), message.CreateDraft()))
                .Select(item => preserveMessageIds ? item : item.WithNewMessageId())
                .ToArray();
            DeadLetterResender.EnsureSafeMessageIds(profile.Provider, items, resendMode);
            var action = resendMode == ResendMode.Move ? "Move dead-letter messages" : "Resend dead-letter messages";
            var perDestination = items
                .GroupBy(item => McpMapping.EntityName(item.Destination), StringComparer.OrdinalIgnoreCase)
                .Select(group => $"• to {group.Key}: {group.Count()}")
                .ToArray();
            var examples = items.Take(10).Select(item =>
                $"  {McpMapping.EntityName(item.Original.Source)} #{item.Original.SequenceNumber} {item.Original.Properties.MessageId ?? "(no Message ID)"}");
            var decision = await RequestApprovalAsync(server, profile, action,
                $"{items.Length} dead-lettered message(s) will be sent again with " +
                (preserveMessageIds ? "preserved Message IDs (duplicate detection may suppress delivery)" : "distinct new Message IDs") +
                (messagesPerSecond > 0 ? $", at most {messagesPerSecond} per second" : string.Empty) + ".\n" +
                (resendMode == ResendMode.Move
                    ? "Move: after sending, each original that was sent is backed up locally and removed from its dead-letter queue."
                    : "Copy: the originals stay in their dead-letter queues.") +
                (missing.Count > 0 ? $"\n{missing.Count} listed message(s) were not found and will be skipped." : string.Empty) +
                "\n\n" + string.Join("\n", perDestination.Take(20)) +
                (perDestination.Length > 20 ? $"\n… and {perDestination.Length - 20} more destinations" : string.Empty) +
                "\n\nFirst messages:\n" + string.Join("\n", examples),
                reason, cancellationToken).ConfigureAwait(false);
            if (!decision.Approved)
            {
                return Declined(profile, action, decision);
            }

            var result = await session.WriteAsync(profile,
                    (workspace, token) => DeadLetterResender.ResendAsync(workspace, items, resendMode, messagesPerSecond, null, token),
                    cancellationToken)
                .ConfigureAwait(false);
            var summary = $"{result.SentCount} of {items.Length} sent" +
                          (resendMode == ResendMode.Move ? $", {result.MovedCount} original(s) removed" : string.Empty) +
                          (result.OriginalsKeptCount > 0 ? $", {result.OriginalsKeptCount} original(s) kept" : string.Empty) +
                          (result.FailedCount > 0 ? $", {result.FailedCount} failed" : string.Empty) +
                          (result.CancelledCount > 0 ? $", {result.CancelledCount} not sent" : string.Empty) +
                          (missing.Count > 0 ? $", {missing.Count} not found" : string.Empty) + ".";
            session.Record(result.FailedCount == 0 && result.OriginalsKeptCount == 0 ? "Success" : "Warning",
                resendMode == ResendMode.Move ? "Moved dead-letter messages" : "Resent dead-letter messages",
                $"{summary} Reason: {reason}" + (result.BackupDirectory is null ? "." : $". Backup: {result.BackupDirectory}"),
                profile, target);
            return new ChangeResult(profile.Name, true, summary, result.BackupDirectory,
                result.Items.Select(item =>
                        $"{McpMapping.EntityName(item.Item.Original.Source)} #{item.Item.Original.SequenceNumber} → " +
                        $"{McpMapping.EntityName(item.Item.Destination)}: {item.Outcome}" +
                        (item.Detail is null ? string.Empty : $" ({item.Detail})"))
                    .Concat(missing.Select(key => $"{McpMapping.EntityName(key.Source)} #{key.SequenceNumber}: NotFound"))
                    .ToArray());
        });

    /// <summary>
    /// Reads the complete messages behind the given keys. Azure pages by sequence number; SQS and Pub/Sub return one
    /// batch, whose sequence numbers are derived from the Message ID.
    /// </summary>
    private static async Task<(IReadOnlyList<BrowsedMessage> Found, IReadOnlyList<DeadLetterMessageKey> Missing)> FindMessagesAsync(
        IServiceBusWorkspace workspace,
        IReadOnlyList<DeadLetterMessageKey> keys,
        CancellationToken cancellationToken)
    {
        const int MaximumPages = 20;
        var found = new Dictionary<DeadLetterMessageKey, BrowsedMessage>();
        foreach (var group in keys.GroupBy(key => (key.Source, key.SubQueue)))
        {
            var wanted = group.ToDictionary(key => key.SequenceNumber);
            long? from = wanted.Keys.Min();
            for (var page = 0; page < MaximumPages && wanted.Count > 0; page++)
            {
                var batch = await workspace.BrowseMessagesAsync(
                        new BrowseMessagesRequest(group.Key.Source, group.Key.SubQueue, BrowseMessagesRequest.MaximumMaxMessages, from),
                        cancellationToken)
                    .ConfigureAwait(false);
                foreach (var message in batch)
                {
                    if (wanted.TryGetValue(message.SequenceNumber, out var key) &&
                        (key.MessageId is null || string.Equals(key.MessageId, message.Properties.MessageId, StringComparison.Ordinal)))
                    {
                        found[key] = message;
                        wanted.Remove(message.SequenceNumber);
                    }
                }

                if (batch.Count < BrowseMessagesRequest.MaximumMaxMessages || !batch[^1].HasSequenceNumber)
                {
                    break;
                }
                from = batch[^1].SequenceNumber + 1;
            }
        }

        return (keys.Where(found.ContainsKey).Select(key => found[key]).ToArray(),
            keys.Where(key => !found.ContainsKey(key)).ToArray());
    }

    private Task<ApprovalDecision> RequestApprovalAsync(
        McpServer server,
        ServiceBusProfile profile,
        string action,
        string details,
        string reason,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            throw new McpException("Explain the change in 'reason'; it is shown to the person who approves it.");
        }

        var client = server.ClientInfo is { } info ? $"{info.Name} {info.Version}".Trim() : "an MCP client";
        return approver.RequestAsync(
            new ApprovalRequest(
                action,
                profile.Name,
                profile.Environment == EnvironmentKind.Production,
                $"Requested by: {client}\nEnvironment: {profile.Name} ({profile.EnvironmentDisplayName})\n" +
                $"{profile.Provider.DisplayName()}: {profile.EndpointDisplay}\n\nReason given: {reason.Trim()}\n\n{details}"),
            server,
            cancellationToken);
    }

    private ChangeResult Declined(ServiceBusProfile profile, string action, ApprovalDecision decision)
    {
        session.Record("Info", $"{action} not approved", decision.Reason, profile);
        return new ChangeResult(profile.Name, false, $"Not done: {decision.Reason}", null, []);
    }
}
