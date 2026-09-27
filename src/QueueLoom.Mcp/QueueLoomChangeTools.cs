using System.ComponentModel;
using System.Globalization;
using System.Text;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;

namespace QueueLoom.Mcp;

/// <summary>
/// Tools that change Service Bus. Every call is shown to a person for approval before anything happens;
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
