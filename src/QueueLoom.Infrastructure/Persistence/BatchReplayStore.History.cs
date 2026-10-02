using System.Text.Json;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;

namespace QueueLoom.Infrastructure.Persistence;

public sealed partial class BatchReplayStore
{
    public IReadOnlyList<ReplayPlan> List()
    {
        if (!Directory.Exists(RootDirectory)) return [];
        var plans = new List<ReplayPlan>();
        foreach (var folder in Directory.EnumerateDirectories(RootDirectory))
        {
            if (!Guid.TryParseExact(Path.GetFileName(folder), "N", out var id) ||
                File.GetAttributes(folder).HasFlag(FileAttributes.ReparsePoint)) continue;
            var path = Path.Combine(folder, "plan.json");
            if (!File.Exists(path)) continue; // Unpublished preparation is never resumable.
            try
            {
                var plan = JsonSerializer.Deserialize<ReplayPlan>(File.ReadAllText(path));
                if (plan is not null && plan.Id == id) plans.Add(plan);
            }
            catch (JsonException) { /* Preserve damaged history, never execute it. */ }
        }
        return plans.OrderByDescending(p => p.CreatedAt).ToArray();
    }

    public OperationHistory ReadHistory(ReplayPlan plan)
    {
        var folder = DirectoryFor(plan.Id);
        var items = new List<OperationItem>();
        if (plan.Count is < 1 or > 1000) throw new InvalidDataException("Invalid operation count.");
        var activationBlocked = IsScheduleActivationBlocked(plan, folder);
        for (var index = 0; index < plan.Count; index++)
        {
            var metadata = Path.Combine(folder, $"{index:D6}.metadata.json");
            var item = File.Exists(metadata)
                ? JsonSerializer.Deserialize<OperationItem>(File.ReadAllText(metadata))
                : new OperationItem(index, "Legacy replay snapshot", null, plan.Destination.Path, "Pending");
            var detail = Path.Combine(folder, $"{index:D6}.detail");
            var state = ReadState(folder, index);
            items.Add((item ?? throw new InvalidDataException("Invalid operation metadata.")) with
            {
                State = activationBlocked && state == "Pending" ? "AwaitingScheduleClaim" : state,
                Detail = File.Exists(detail) ? File.ReadAllText(detail) : null
            });
        }
        return new OperationHistory(plan, items);
    }

    private static Task WriteItemMetadata(string folder, int index, ReplayPayload payload,
        ServiceBusEntityReference destination, CancellationToken token) => AtomicFile.WriteTextAsync(
        Path.Combine(folder, $"{index:D6}.metadata.json"), JsonSerializer.Serialize(
            new OperationItem(index, payload.Origin, payload.Properties.MessageId, destination.Path, "Pending")), token);

    public async Task<ReplayPlan> CreateResendAsync(Guid profileId, IReadOnlyList<ResendItem> items, ResendMode mode,
        int rate, string? ns, string configurationIdentity, string kind, CancellationToken token, bool deferActivation = false)
    {
        if (profileId == Guid.Empty || items.Count is < 1 or > 1000 || rate is < 0 or > 100 ||
            items.Any(i => !i.Destination.CanSend || (mode == ResendMode.Move && !i.Key.IsValid)))
            throw new ArgumentException("Invalid resend selection, destination or rate.");
        var plan = new ReplayPlan(Guid.NewGuid(), profileId, items[0].Destination, DateTimeOffset.UtcNow,
            items.Count, Math.Max(1, rate == 0 ? 50 : rate), true, ns, configurationIdentity)
        { Kind = kind, Mode = mode, RequiresScheduleActivation = deferActivation };
        var folder = DirectoryFor(plan.Id);
        Directory.CreateDirectory(folder);
        AtomicFile.RestrictDirectoryToCurrentUser(folder);
        long bytes = 0;
        for (var index = 0; index < items.Count; index++)
        {
            var item = items[index];
            bytes += item.Message.Body.GetBytes().Length;
            if (bytes > 32 * 1024 * 1024) throw new InvalidOperationException("Operation bodies exceed 32 MiB. Narrow the selection.");
            var validation = MessageDraftValidator.Validate(item.Message);
            if (!validation.IsValid) throw new InvalidOperationException(string.Join(" ", validation.Errors.Select(e => e.Message)));
            var payload = new ReplayPayload(item.Message.Body, item.Message.Properties, item.Message.ApplicationProperties.ToArray(),
                $"{item.Original.Source.Path} / {item.Original.SubQueue} / {item.Original.SequenceNumber} / {item.Original.Properties.MessageId}")
            { KafkaEnvelope = item.Message.KafkaEnvelope, Destination = item.Destination,
                Original = item.Key };
            await AtomicFile.WriteTextAsync(Path.Combine(folder, $"{index:D6}.message.json"), JsonSerializer.Serialize(payload), token);
            await WriteItemMetadata(folder, index, payload, item.Destination, token);
            if (deferActivation) await WriteStateAsync(Path.Combine(folder, $"{index:D6}.state"), "AwaitingScheduleClaim", token);
        }
        await AtomicFile.WriteTextAsync(Path.Combine(folder, "plan.json"), JsonSerializer.Serialize(plan), token);
        return plan;
    }

    /// <summary>Only the caller that atomically consumed the exact scheduled job may activate its snapshot.
    /// A crash before claim leaves blocked snapshots; they never authorize a send on startup or from history.</summary>
    public async Task ActivateScheduledAsync(ReplayPlan plan, CancellationToken token)
    {
        var folder = DirectoryFor(plan.Id);
        await using var ownership = await CrossProcessFileLock.AcquireAsync(Path.Combine(folder, ".lock"), token);
        if (JsonSerializer.Deserialize<ReplayPlan>(await File.ReadAllTextAsync(Path.Combine(folder, "plan.json"), token)) != plan ||
            plan.Count is < 1 or > 1000 || Enumerable.Range(0, plan.Count).Any(i => ReadState(folder, i) != "AwaitingScheduleClaim"))
            throw new InvalidOperationException("Scheduled snapshot changed or was already activated.");
        for (var index = 0; index < plan.Count; index++)
            await WriteStateAsync(Path.Combine(folder, $"{index:D6}.state"), "Pending", CancellationToken.None);
        // Publish completion last. A crash or write failure during activation must block the entire snapshot.
        await WriteStateAsync(Path.Combine(folder, ".schedule-activated"), plan.Id.ToString("N"), CancellationToken.None);
    }

    private static bool IsScheduleActivationBlocked(ReplayPlan plan, string folder)
    {
        // Legacy scheduled snapshots have no completion proof and remain blocked for manual inspection.
        if (!plan.RequiresScheduleActivation && plan.Kind != "Scheduled resend") return false;
        var marker = Path.Combine(folder, ".schedule-activated");
        return !File.Exists(marker) || File.ReadAllText(marker) != plan.Id.ToString("N");
    }

    private static void EnsureScheduleActivated(ReplayPlan plan, string folder)
    {
        if (IsScheduleActivationBlocked(plan, folder))
            throw new InvalidOperationException("Scheduled snapshot claim/activation was not completed. Recovery is blocked; manual inspection is required.");
    }

    private static void ValidateConnection(ReplayPlan plan, IServiceBusWorkspace workspace, Func<bool> canWrite)
    {
        if (workspace.ConnectedProfileId != plan.ProfileId || !canWrite())
            throw new InvalidOperationException("Reconnect and unlock the operation environment.");
        if (plan.Namespace is not null && plan.Namespace != workspace.ConnectedNamespace ||
            plan.ConfigurationIdentity is not null && plan.ConfigurationIdentity != workspace.ConnectedConfigurationIdentity)
            throw new InvalidOperationException("The environment configuration changed. Recovery is blocked.");
    }

    public async Task<ResendResult> RunItemsAsync(ReplayPlan plan, IReadOnlyList<int> indexes, bool retryRejected,
        IServiceBusWorkspace workspace, Func<bool> canWrite, IProgress<ResendProgress>? progress, CancellationToken token)
    {
        var folder = DirectoryFor(plan.Id);
        if (File.GetAttributes(folder).HasFlag(FileAttributes.ReparsePoint)) throw new InvalidDataException("Linked operation folders are blocked.");
        await using var ownership = await CrossProcessFileLock.AcquireAsync(Path.Combine(folder, ".lock"), token);
        if (JsonSerializer.Deserialize<ReplayPlan>(await File.ReadAllTextAsync(Path.Combine(folder, "plan.json"), token)) != plan)
            throw new InvalidOperationException("The saved operation changed. Review again.");
        EnsureScheduleActivated(plan, folder);
        if (plan.Count is < 1 or > 1000 || plan.MessagesPerSecond is < 1 or > 100 || indexes.Count == 0 ||
            indexes.Distinct().Count() != indexes.Count || indexes.Any(i => i < 0 || i >= plan.Count))
            throw new ArgumentException("Choose valid operation items.");
        ValidateConnection(plan, workspace, canWrite);
        if (plan.ConfigurationIdentity is null)
            throw new InvalidOperationException("Legacy operation has no configuration identity. Selective recovery is unsupported; review and prepare a new operation.");
        if (plan.Mode == ResendMode.Move && workspace.ConnectedProvider == QueueLoom.Core.Profiles.MessagingProvider.Kafka)
            throw new InvalidOperationException("Kafka cannot move individual messages. Recovery is blocked.");
        var prepared = new Dictionary<int, ResendItem>();
        long bytes = 0;
        // Validate the entire selection before any new effect, and never reinterpret unknown states as pending.
        foreach (var index in indexes)
        {
            var state = ReadState(folder, index);
            if (state != (retryRejected ? "Rejected" : "Pending"))
                throw new InvalidOperationException($"Item {index + 1} is {state}; it cannot be sent by this action.");
            var payload = await ReadPayload(folder, index, token);
            var draft = new MessageDraft(payload.Body, payload.Properties, payload.ApplicationProperties) { KafkaEnvelope = payload.KafkaEnvelope };
            bytes += draft.Body.GetBytes().Length;
            if (bytes > 32 * 1024 * 1024 || !MessageDraftValidator.Validate(draft).IsValid)
                throw new InvalidDataException("Operation payload exceeds limits or is invalid.");
            var destination = payload.Destination ?? plan.Destination;
            if (!destination.CanSend) throw new InvalidDataException("Invalid operation destination.");
            var original = payload.Original is { } key
                ? new BrowsedMessage(key.Source, key.SubQueue, key.SequenceNumber, ReadOnlyMemory<byte>.Empty, new EditableMessageProperties(MessageId: key.MessageId))
                : new BrowsedMessage(ServiceBusEntityReference.Queue("local-replay"), ServiceBusSubQueue.Active, index, ReadOnlyMemory<byte>.Empty, draft.Properties);
            if (plan.Mode == ResendMode.Move && (payload.Original is null || !payload.Original.IsValid))
                throw new InvalidDataException("Move source identity is missing or invalid.");
            prepared.Add(index, new ResendItem(original, destination, draft));
        }
        DeadLetterResender.EnsureSafeMessageIds(workspace.ConnectedProvider, prepared.Values.ToArray(), plan.Mode);
        var results = new List<ResendItemResult>();
        string? backup = null;
        foreach (var index in indexes)
        {
            if (token.IsCancellationRequested) break;
            // Space every attempted send, including a fast rejection or an unknown acknowledgement.
            if (results.Count > 0)
            {
                try { await DelayAsync(TimeSpan.FromSeconds(1d / plan.MessagesPerSecond), token); }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
            }
            ValidateConnection(plan, workspace, canWrite);
            var item = prepared[index];
            var stateFile = Path.Combine(folder, $"{index:D6}.state");
            await WriteStateAsync(stateFile, "Sending", token); // Must succeed BEFORE the transport call.
            try
            {
                await workspace.SendMessageAsync(new SendMessageRequest(item.Destination, item.Message), token);
            }
            catch (Exception exception)
            {
                var rejected = exception is DeliveryRejectedException;
                await WriteStateAsync(stateFile, rejected ? "Rejected" : "Uncertain", CancellationToken.None);
                await AtomicFile.WriteTextAsync(Path.Combine(folder, $"{index:D6}.detail"),
                    SensitiveDataRedactor.SummarizeException(exception), CancellationToken.None);
                results.Add(new ResendItemResult(item, ResendOutcome.Failed,
                    rejected ? "Provider proved the send was rejected." : "Delivery outcome unknown; retry is blocked."));
                progress?.Report(new ResendProgress(results.Count, indexes.Count, results.Count(r => r.Outcome == ResendOutcome.Failed)));
                continue;
            }
            // A failure saving acknowledgement leaves Sending: recovery must never resend it.
            await WriteStateAsync(stateFile, "Sent", CancellationToken.None);
            var result = new ResendItemResult(item, ResendOutcome.Sent);
            if (plan.Mode == ResendMode.Move)
            {
                ValidateConnection(plan, workspace, canWrite);
                await WriteStateAsync(stateFile, "Deleting", CancellationToken.None);
                try
                {
                    var deletion = await workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest([item.Key]), CancellationToken.None);
                    backup = deletion.BackupDirectory;
                    var outcome = deletion.Messages.SingleOrDefault(m => m.Message == item.Key);
                    var moved = outcome?.Outcome == DeadLetterMessageDeletionOutcome.Deleted;
                    await WriteStateAsync(stateFile, moved ? "Moved" : "SentOriginalKept", CancellationToken.None);
                    result = new ResendItemResult(item, moved ? ResendOutcome.Moved : ResendOutcome.SentOriginalKept, outcome?.Detail);
                }
                catch (Exception exception)
                {
                    await WriteStateAsync(stateFile, "DeleteUncertain", CancellationToken.None);
                    await AtomicFile.WriteTextAsync(Path.Combine(folder, $"{index:D6}.detail"),
                        SensitiveDataRedactor.SummarizeException(exception), CancellationToken.None);
                    result = new ResendItemResult(item, ResendOutcome.SentOriginalKept, "Send confirmed; deletion outcome unknown. Never resend this item.");
                }
            }
            results.Add(result);
            progress?.Report(new ResendProgress(results.Count, indexes.Count, results.Count(r => r.Outcome == ResendOutcome.Failed)));
        }
        foreach (var index in indexes.Skip(results.Count)) results.Add(new ResendItemResult(prepared[index], ResendOutcome.Cancelled, "Unattempted; continue from history."));
        return new ResendResult(results, backup);
    }
}
