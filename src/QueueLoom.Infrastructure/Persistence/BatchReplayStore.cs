using System.Text.Json;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;

namespace QueueLoom.Infrastructure.Persistence;

public sealed record ReplayPayload(EditableMessageBody Body, EditableMessageProperties Properties,
    MessageApplicationProperty[] ApplicationProperties, string Origin)
{
    public KafkaEnvelope? KafkaEnvelope { get; init; }
}

/// <summary>Durable, resumable copy operation. Never settles original messages.</summary>
public sealed class BatchReplayStore(string root) : IBatchReplayStore
{
    public string RootDirectory => Path.GetFullPath(root);
    private string DirectoryFor(Guid id) => Path.Combine(RootDirectory, id.ToString("N"));

    public async Task<ReplayPlan> CreateAsync(Guid profileId, ServiceBusEntityReference destination,
        IEnumerable<(MessageDraft Draft, string Origin)> drafts, bool preserveIds, int rate,
        CancellationToken token, string? fullyQualifiedNamespace = null, string? configurationIdentity = null)
    {
        if (!destination.CanSend || profileId == Guid.Empty) throw new ArgumentException("A connected profile and send destination are required.");
        if (rate is < 1 or > 50) throw new ArgumentOutOfRangeException(nameof(rate), "Use 1–50 messages per second.");
        var id = Guid.NewGuid();
        var folder = DirectoryFor(id);
        Directory.CreateDirectory(folder);
        AtomicFile.RestrictDirectoryToCurrentUser(folder);
        var count = 0;
        long bytes = 0;
        foreach (var (draft, origin) in drafts)
        {
            token.ThrowIfCancellationRequested();
            bytes += draft.Body.GetBytes().Length;
            if (++count > 1000 || bytes > 32 * 1024 * 1024)
                throw new InvalidOperationException("Replay is limited to 1,000 messages and 32 MiB of bodies. Narrow the selection.");
            var properties = draft.Properties with
            {
                MessageId = preserveIds && !string.IsNullOrWhiteSpace(draft.Properties.MessageId)
                    ? draft.Properties.MessageId : Guid.NewGuid().ToString("N"),
                // Replay means send now. TTL remains explicit; enqueue timestamps are broker-owned.
                ScheduledEnqueueTime = null
            };
            var prepared = new MessageDraft(draft.Body, properties, draft.ApplicationProperties) { KafkaEnvelope = draft.KafkaEnvelope };
            var validation = MessageDraftValidator.Validate(prepared);
            if (!validation.IsValid) throw new InvalidOperationException(string.Join(" ", validation.Errors.Select(e => e.Message)));
            var payload = new ReplayPayload(prepared.Body, properties, prepared.ApplicationProperties.ToArray(), origin) { KafkaEnvelope = prepared.KafkaEnvelope };
            await AtomicFile.WriteTextAsync(Path.Combine(folder, $"{count - 1:D6}.message.json"), JsonSerializer.Serialize(payload), token);
        }
        if (count == 0) throw new InvalidOperationException("Select at least one readable message.");
        var plan = new ReplayPlan(id, profileId, destination, DateTimeOffset.UtcNow, count, rate, preserveIds, fullyQualifiedNamespace, configurationIdentity);
        // Publishing the plan last prevents resuming a partially prepared batch.
        await AtomicFile.WriteTextAsync(Path.Combine(folder, "plan.json"), JsonSerializer.Serialize(plan), token);
        return plan;
    }

    public ReplayPlan? Latest(Guid profileId) => !Directory.Exists(RootDirectory) ? null :
        Directory.EnumerateFiles(RootDirectory, "plan.json", SearchOption.AllDirectories)
            .Select(file => JsonSerializer.Deserialize<ReplayPlan>(File.ReadAllText(file)))
            .Where(plan => plan?.ProfileId == profileId)
            .OrderByDescending(plan => plan!.CreatedAt).FirstOrDefault();

    public async Task<ReplayProgress> RunAsync(ReplayPlan plan, IServiceBusWorkspace workspace,
        Func<bool> canWrite, IProgress<ReplayProgress>? progress, CancellationToken token)
    {
        var folder = DirectoryFor(plan.Id);
        await using var gate = await CrossProcessFileLock.AcquireAsync(Path.Combine(folder, ".lock"), token);
        var storedPlan = JsonSerializer.Deserialize<ReplayPlan>(await File.ReadAllTextAsync(Path.Combine(folder, "plan.json"), token));
        if (storedPlan != plan) throw new InvalidOperationException("Replay plan changed. Review it again.");
        if (plan.Count is < 1 or > 1000 || plan.MessagesPerSecond is < 1 or > 50 || !plan.Destination.CanSend)
            throw new InvalidDataException("Replay plan has invalid limits or destination.");
        if (workspace.ConnectedProfileId != plan.ProfileId || !canWrite()) throw new InvalidOperationException("Reconnect and unlock the batch environment.");
        if (plan.Namespace is not null && !string.Equals(plan.Namespace, workspace.ConnectedNamespace, StringComparison.Ordinal))
            throw new InvalidOperationException("The profile namespace has changed since this batch was prepared. Resume is blocked.");
        if (plan.ConfigurationIdentity is not null && plan.ConfigurationIdentity != workspace.ConnectedConfigurationIdentity)
            throw new InvalidOperationException("The environment configuration changed since this batch was prepared. Resume is blocked.");
        var sent = 0;
        // Validate every pending payload and stop on an uncertain previous send before any new writes.
        for (var i = 0; i < plan.Count; i++)
        {
            var state = ReadState(folder, i);
            if (state == "Sent") { sent++; continue; }
            if (state != "Pending")
                throw new InvalidOperationException($"Batch {plan.Id:N}, item {i + 1}: previous delivery is uncertain. Inspect the destination before replaying; automatic retry is blocked.");
            var item = await ReadPayload(folder, i, token);
            var validation = MessageDraftValidator.Validate(new MessageDraft(item.Body, item.Properties, item.ApplicationProperties) { KafkaEnvelope = item.KafkaEnvelope });
            if (!validation.IsValid) throw new InvalidDataException($"Replay item {i + 1} is invalid.");
        }
        try
        {
            for (var i = 0; i < plan.Count; i++)
            {
                token.ThrowIfCancellationRequested();
                if (ReadState(folder, i) == "Sent") continue;
                if (workspace.ConnectedProfileId != plan.ProfileId || !canWrite())
                    throw new InvalidOperationException("Write access expired. Batch paused; completed sends are retained.");
                if (plan.ConfigurationIdentity is not null && plan.ConfigurationIdentity != workspace.ConnectedConfigurationIdentity)
                    throw new InvalidOperationException("The environment configuration changed. Batch paused.");
                var item = await ReadPayload(folder, i, token);
                var stateFile = Path.Combine(folder, $"{i:D6}.state");
                await AtomicFile.WriteTextAsync(stateFile, "Sending", token);
                try
                {
                    // Once sent, cancellation is handled between items to preserve the acknowledgement.
                    await workspace.SendMessageAsync(new SendMessageRequest(plan.Destination,
                        new MessageDraft(item.Body, item.Properties, item.ApplicationProperties) { KafkaEnvelope = item.KafkaEnvelope }), CancellationToken.None);
                    await AtomicFile.WriteTextAsync(stateFile, "Sent", CancellationToken.None);
                    sent++;
                }
                catch
                {
                    await AtomicFile.WriteTextAsync(stateFile, "Uncertain", CancellationToken.None);
                    throw;
                }
                progress?.Report(new ReplayProgress(plan.Id, sent, plan.Count, "Sending copies; originals retained"));
                await Task.Delay(TimeSpan.FromSeconds(1d / plan.MessagesPerSecond), token);
            }
            return new ReplayProgress(plan.Id, sent, plan.Count, "Completed");
        }
        finally
        {
            await AtomicFile.WriteTextAsync(Path.Combine(folder, "result.json"),
                JsonSerializer.Serialize(new ReplayProgress(plan.Id, sent, plan.Count, sent == plan.Count ? "Completed" : "Paused; inspect item states before resuming")), CancellationToken.None);
        }
    }

    private static string ReadState(string folder, int index)
    {
        var path = Path.Combine(folder, $"{index:D6}.state");
        return File.Exists(path) ? File.ReadAllText(path) : "Pending";
    }

    private static async Task<ReplayPayload> ReadPayload(string folder, int index, CancellationToken token) =>
        JsonSerializer.Deserialize<ReplayPayload>(await File.ReadAllTextAsync(Path.Combine(folder, $"{index:D6}.message.json"), token))
        ?? throw new InvalidDataException("Invalid replay payload");
}
