using System.Collections.Concurrent;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Messaging;

/// <summary>
/// Shared workflow for services that cannot peek (Amazon SQS, Google Pub/Sub). Reading means receiving
/// messages, holding them invisible for a short time and then releasing them unchanged. Deleting means
/// backing a held message up to disk first and only then settling it, the same guarantee QueueLoom gives
/// for Azure Service Bus.
/// </summary>
public abstract class LeasedMessagingWorkspace : IServiceBusWorkspace
{
    private static readonly TimeSpan TopologyCacheDuration = TimeSpan.FromMinutes(1);
    internal const int EmptyReceiveConfirmations = 2;
    internal const int LoadAllLimit = 5_000;

    private readonly DeadLetterJsonBackupStore _backupStore;
    private readonly AsyncOperationGate _operationGate = new();
    private readonly SemaphoreSlim _topologyGate = new(1, 1);
    private readonly ConcurrentDictionary<string, long> _previousDeadLetterCounts = new(StringComparer.Ordinal);
    private ServiceBusProfile? _profile;
    private ServiceBusTopology? _cachedTopology;
    private WorkspaceConnectionState _connectionState;
    private bool _disposed;

    protected LeasedMessagingWorkspace(DeadLetterJsonBackupStore? backupStore, TimeProvider? timeProvider)
    {
        _backupStore = backupStore ?? new DeadLetterJsonBackupStore(QueueLoomPaths.CreateDefault());
        TimeProvider = timeProvider ?? TimeProvider.System;
    }

    protected TimeProvider TimeProvider { get; }

    public abstract MessagingProvider Provider { get; }

    public WorkspaceConnectionState ConnectionState => _connectionState;

    public Guid? ConnectedProfileId => _profile?.Id;

    public string? ConnectedNamespace => _profile?.EndpointDisplay;

    /// <summary>Creates the service clients and makes one cheap call that proves the credentials work.</summary>
    protected abstract Task OpenAsync(ServiceBusProfile profile, CancellationToken cancellationToken);

    protected abstract ValueTask CloseAsync();

    protected abstract Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken cancellationToken);

    /// <summary>Resolves a queue or subscription (or its dead-letter destination) to something that can be received from.</summary>
    protected abstract ILeasedMessageChannel OpenChannel(
        ServiceBusTopology topology,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue);

    protected abstract Task SendCoreAsync(
        ServiceBusTopology topology,
        ServiceBusEntityReference destination,
        MessageDraft message,
        CancellationToken cancellationToken);

    public async Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ThrowIfDisposed();
        if (profile.Provider != Provider)
        {
            throw new ArgumentException($"This workspace connects to {Provider.DisplayName()} only.", nameof(profile));
        }

        using (await _operationGate.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false))
        {
            ThrowIfDisposed();
            _connectionState = WorkspaceConnectionState.Connecting;
            await CloseAsync().ConfigureAwait(false);
            _profile = null;
            try
            {
                await OpenAsync(profile, cancellationToken).ConfigureAwait(false);
                _profile = profile;
                _cachedTopology = null;
                _previousDeadLetterCounts.Clear();
                _connectionState = WorkspaceConnectionState.Connected;
            }
            catch
            {
                await CloseAsync().ConfigureAwait(false);
                _connectionState = WorkspaceConnectionState.Faulted;
                throw;
            }
        }
    }

    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        using (await _operationGate.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false))
        {
            await CloseAsync().ConfigureAwait(false);
            _profile = null;
            _cachedTopology = null;
            _connectionState = WorkspaceConnectionState.Disconnected;
        }
    }

    public Task SetAccessModeAsync(ProfileAccessMode accessMode, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        _profile = GetConnectedProfile() with { AccessMode = accessMode };
        return Task.CompletedTask;
    }

    public async Task<ServiceBusTopology> GetTopologyAsync(
        bool forceRefresh = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        return await GetTopologyCoreAsync(forceRefresh, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
        BrowseMessagesRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);

        // There is no position to continue from: every browse starts at whatever the service hands out
        // first. "Load more" therefore returns the first page again rather than duplicating messages.
        var topology = await GetTopologyCoreAsync(false, cancellationToken).ConfigureAwait(false);
        var channel = OpenChannel(topology, request.Source, request.SubQueue);
        var limit = request.LoadAll ? LoadAllLimit : request.MaxMessages;
        var held = new List<LeasedMessage>();
        try
        {
            var messages = await ReceiveUpToAsync(channel, limit, held, cancellationToken).ConfigureAwait(false);
            return Array.AsReadOnly(messages.Select(message => message.Message).ToArray());
        }
        finally
        {
            await ReleaseQuietlyAsync(channel, held).ConfigureAwait(false);
        }
    }

    public async Task<DeadLetterSearchResult> SearchDeadLettersAsync(
        DeadLetterSearchRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);

        var profile = GetConnectedProfile();
        var startedAt = TimeProvider.GetUtcNow();
        var topology = await GetTopologyCoreAsync(false, cancellationToken).ConfigureAwait(false);
        var results = new List<DeadLetterSearchSourceResult>();
        var matchCount = 0;
        var resultLimitReached = false;

        // Sources are scanned one at a time: two sources can share one SQS dead-letter queue, and a message
        // held for one scan would be invisible to the other.
        foreach (var target in request.Targets)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                results.Add(new DeadLetterSearchSourceResult(target.Source, target.SubQueue, 0, [],
                    Error: "Search was cancelled before this source was scanned."));
                continue;
            }
            if (matchCount >= request.MaximumResults)
            {
                resultLimitReached = true;
                break;
            }

            var held = new List<LeasedMessage>();
            ILeasedMessageChannel? channel = null;
            try
            {
                channel = OpenChannel(topology, target.Source, target.SubQueue);
                var scanned = await ReceiveUpToAsync(channel, request.MaximumMessagesPerTarget, held, cancellationToken)
                    .ConfigureAwait(false);
                var matches = new List<BrowsedMessage>();
                foreach (var message in scanned)
                {
                    if (!DeadLetterSearchMatcher.IsMatch(message.Message, request.Query))
                    {
                        continue;
                    }
                    if (matchCount >= request.MaximumResults)
                    {
                        resultLimitReached = true;
                        break;
                    }
                    matches.Add(message.Message);
                    matchCount++;
                }

                results.Add(new DeadLetterSearchSourceResult(
                    target.Source,
                    target.SubQueue,
                    scanned.Count,
                    matches,
                    ScanLimitReached: scanned.Count >= request.MaximumMessagesPerTarget));
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                results.Add(new DeadLetterSearchSourceResult(target.Source, target.SubQueue, 0, [],
                    Error: "Search timed out before this source completed."));
            }
            catch (Exception exception)
            {
                results.Add(new DeadLetterSearchSourceResult(target.Source, target.SubQueue, 0, [],
                    Error: exception.GetBaseException().Message));
            }
            finally
            {
                if (channel is not null)
                {
                    await ReleaseQuietlyAsync(channel, held).ConfigureAwait(false);
                }
            }
        }

        return new DeadLetterSearchResult(profile.Id, startedAt, TimeProvider.GetUtcNow(), results, resultLimitReached);
    }

    public async Task SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        EnsureWriteAllowed();
        var topology = await GetTopologyCoreAsync(false, cancellationToken).ConfigureAwait(false);
        await SendCoreAsync(topology, request.Destination, request.Message, cancellationToken).ConfigureAwait(false);
        _cachedTopology = null;
    }

    public Task ResubmitDeadLetterAsync(ResubmitDeadLetterRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Disposition != DeadLetterDisposition.KeepOriginal)
        {
            throw new NotSupportedException(
                "QueueLoom only resends a copy. Delete the original afterwards with Delete messages, which backs it up first.");
        }

        return SendMessageAsync(new SendMessageRequest(request.Destination, request.Message), cancellationToken);
    }

    public async Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(
        DeadLetterPurgeRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterPurgeProgress>? progress = null)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        EnsureWriteAllowed();

        var profile = GetConnectedProfile();
        var startedAt = TimeProvider.GetUtcNow();
        var topology = await GetTopologyCoreAsync(false, cancellationToken).ConfigureAwait(false);
        // Resolve every target before deleting anything, so a bad target fails the whole request up front.
        var targets = request.Targets
            .Where(target => target.SubQueue == ServiceBusSubQueue.DeadLetter)
            .Select(target => (Target: target, Channel: OpenChannel(topology, target.Source, target.SubQueue)))
            .ToArray();
        var backupSession = await _backupStore.CreateSessionAsync(profile, startedAt, cancellationToken)
            .ConfigureAwait(false);
        var results = new List<DeadLetterPurgeSourceResult>(targets.Length);

        for (var index = 0; index < targets.Length; index++)
        {
            var (target, channel) = targets[index];
            if (cancellationToken.IsCancellationRequested)
            {
                results.Add(new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, 0,
                    "Cancelled before this source was processed."));
                continue;
            }

            results.Add(await PurgeTargetAsync(
                    target,
                    channel,
                    request.BatchSize,
                    request.MaximumMessagesPerSubQueue,
                    backupSession,
                    index + 1,
                    targets.Length,
                    progress,
                    cancellationToken)
                .ConfigureAwait(false));
        }

        _cachedTopology = null;
        return new DeadLetterPurgeResult(profile.Id, startedAt, TimeProvider.GetUtcNow(), results, backupSession.RootDirectory);
    }

    public async Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(
        DeleteDeadLetterMessagesRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterMessageDeletionProgress>? progress = null)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        EnsureWriteAllowed();

        var profile = GetConnectedProfile();
        var startedAt = TimeProvider.GetUtcNow();
        var topology = await GetTopologyCoreAsync(false, cancellationToken).ConfigureAwait(false);
        var groups = request.BySubQueue
            .Select(group => (Group: group, Channel: OpenChannel(topology, group.Key.Source, group.Key.SubQueue)))
            .ToArray();
        var backupSession = await _backupStore.CreateSessionAsync(profile, startedAt, cancellationToken)
            .ConfigureAwait(false);
        var results = new List<DeadLetterMessageDeletionResult>(request.Messages.Count);

        for (var index = 0; index < groups.Length; index++)
        {
            var (group, channel) = groups[index];
            if (cancellationToken.IsCancellationRequested)
            {
                results.AddRange(group.Select(key => new DeadLetterMessageDeletionResult(
                    key, DeadLetterMessageDeletionOutcome.Cancelled)));
                continue;
            }

            results.AddRange(await DeleteFromChannelAsync(
                    group.Key.Source,
                    group.Key.SubQueue,
                    group.ToArray(),
                    channel,
                    request.MaximumScannedPerSubQueue,
                    backupSession,
                    index + 1,
                    groups.Length,
                    progress,
                    cancellationToken)
                .ConfigureAwait(false));
        }

        _cachedTopology = null;
        return new DeleteDeadLetterMessagesResult(profile.Id, startedAt, TimeProvider.GetUtcNow(), results,
            backupSession.RootDirectory);
    }

    public virtual QueueManagementCapabilities? QueueManagement => null;

    public virtual Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} does not support queue management in QueueLoom.");

    public virtual Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} does not support queue management in QueueLoom.");

    public virtual Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} does not support queue management in QueueLoom.");

    public virtual Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} does not support queue management in QueueLoom.");

    /// <summary>Runs a queue management change: allowed only with the environment's permission and write access.</summary>
    protected async Task ManageAsync(Func<CancellationToken, Task> change, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        GetConnectedProfile().EnsureQueueManagementAllowed();
        await change(cancellationToken).ConfigureAwait(false);
        _cachedTopology = null;
    }

    public async Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(
        DeadLetterMonitorScope scope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        var profile = GetConnectedProfile();
        var topology = await GetTopologyCoreAsync(true, cancellationToken).ConfigureAwait(false);

        var sources = topology.Queues
            .Where(queue => queue.HasDeadLetterQueue)
            .Select(queue => (queue.Reference, queue.Runtime))
            .Concat(topology.Topics.SelectMany(topic => topic.Subscriptions)
                .Where(subscription => subscription.HasDeadLetterQueue)
                .Select(subscription => (subscription.Reference, subscription.Runtime)))
            .Where(source => scope.Kind == DeadLetterMonitorScopeKind.AllMessageSources || source.Reference == scope.Entity);

        var snapshots = new List<DeadLetterEntitySnapshot>();
        // One source at a time: sampling holds messages, and sources can share a dead-letter destination.
        foreach (var source in sources)
        {
            long count;
            if (source.Runtime.CountsUnavailable)
            {
                try
                {
                    count = await SampleDeadLetterCountAsync(topology, source.Reference, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    snapshots.Add(new DeadLetterEntitySnapshot(source.Reference, null, null, exception.GetBaseException().Message));
                    continue;
                }
            }
            else
            {
                count = source.Runtime.MessageCounts.DeadLetter;
            }

            var key = source.Reference.Path;
            long? previous = _previousDeadLetterCounts.TryGetValue(key, out var value) ? value : null;
            _previousDeadLetterCounts[key] = count;
            snapshots.Add(new DeadLetterEntitySnapshot(source.Reference, count, previous));
        }

        return new DeadLetterSnapshot(profile.Id, TimeProvider.GetUtcNow(), snapshots);
    }

    /// <summary>
    /// Counts a dead-letter destination the service reports no number for (Pub/Sub) by reading it and releasing
    /// every message again, up to <see cref="SampleLimit"/>. Messages stay where they are.
    /// </summary>
    private async Task<long> SampleDeadLetterCountAsync(
        ServiceBusTopology topology,
        ServiceBusEntityReference source,
        CancellationToken cancellationToken)
    {
        var channel = OpenChannel(topology, source, ServiceBusSubQueue.DeadLetter);
        var held = new List<LeasedMessage>();
        try
        {
            var messages = await ReceiveUpToAsync(channel, SampleLimit, held, cancellationToken).ConfigureAwait(false);
            return messages.Count;
        }
        finally
        {
            await ReleaseQuietlyAsync(channel, held).ConfigureAwait(false);
        }
    }

    internal const int SampleLimit = 1_000;

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await CloseAsync().ConfigureAwait(false);
        _topologyGate.Dispose();
        GC.SuppressFinalize(this);
    }

    protected ServiceBusProfile GetConnectedProfile() =>
        _profile ?? throw new InvalidOperationException("Connect to an environment first.");

    /// <summary>Receives until <paramref name="limit"/> messages of this source are held or the channel runs dry.</summary>
    /// <param name="held">Every received message, including other sources' ones; the caller releases them.</param>
    internal static async Task<List<LeasedMessage>> ReceiveUpToAsync(
        ILeasedMessageChannel channel,
        int limit,
        List<LeasedMessage> held,
        CancellationToken cancellationToken)
    {
        var result = new List<LeasedMessage>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        // A shared dead-letter queue can hold many messages of other sources; stop scanning at some point.
        var heldLimit = (int)Math.Min(20_000L, Math.Max(1_000L, limit * 2L));
        var emptyReceives = 0;
        while (result.Count < limit && emptyReceives < EmptyReceiveConfirmations && held.Count < heldLimit)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var batch = await channel.ReceiveAsync(
                    Math.Min(channel.MaximumBatchSize, limit - result.Count),
                    cancellationToken)
                .ConfigureAwait(false);
            if (batch.Count == 0)
            {
                emptyReceives++;
                continue;
            }

            emptyReceives = 0;
            held.AddRange(batch);
            foreach (var message in batch)
            {
                // Standard SQS queues deliver at least once, so the same message can come back twice.
                if (message.BelongsToSource && seen.Add(message.Identity) && result.Count < limit)
                {
                    result.Add(message);
                }
            }
        }

        return result;
    }

    private async Task<DeadLetterPurgeSourceResult> PurgeTargetAsync(
        DeadLetterPurgeTarget target,
        ILeasedMessageChannel channel,
        int batchSize,
        int maximumMessages,
        DeadLetterJsonBackupSession backupSession,
        int targetNumber,
        int targetCount,
        IProgress<DeadLetterPurgeProgress>? progress,
        CancellationToken cancellationToken)
    {
        long backedUp = 0;
        long deleted = 0;
        var emptyReceives = 0;
        var skipped = new List<LeasedMessage>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        progress?.Report(new DeadLetterPurgeProgress(target.Source, target.SubQueue, targetNumber, targetCount, 0, 0,
            DeadLetterPurgeStage.Starting));
        try
        {
            while (deleted < maximumMessages && emptyReceives < EmptyReceiveConfirmations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var wanted = (int)Math.Min(Math.Min(batchSize, channel.MaximumBatchSize), maximumMessages - deleted);
                var batch = await channel.ReceiveAsync(wanted, cancellationToken).ConfigureAwait(false);
                if (batch.Count == 0)
                {
                    emptyReceives++;
                    continue;
                }

                emptyReceives = 0;
                var toSettle = new List<LeasedMessage>(batch.Count);
                foreach (var message in batch)
                {
                    if (!message.BelongsToSource || !seen.Add(message.Identity) || deleted + toSettle.Count >= maximumMessages)
                    {
                        // Another source's message, a duplicate delivery or over the limit: leave it alone.
                        skipped.Add(message);
                        continue;
                    }

                    await backupSession.BackupAsync(message.Message, cancellationToken).ConfigureAwait(false);
                    backedUp++;
                    toSettle.Add(message);
                }

                progress?.Report(new DeadLetterPurgeProgress(target.Source, target.SubQueue, targetNumber, targetCount,
                    backedUp, deleted, DeadLetterPurgeStage.Deleting));
                var failed = await channel.SettleAsync(toSettle, CancellationToken.None).ConfigureAwait(false);
                deleted += toSettle.Count - failed.Count;
                if (failed.Count > 0)
                {
                    return new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, deleted,
                        $"{failed.Count} message(s) could not be deleted from {channel.PhysicalName}. Their backups are kept.");
                }
            }

            progress?.Report(new DeadLetterPurgeProgress(target.Source, target.SubQueue, targetNumber, targetCount,
                backedUp, deleted, DeadLetterPurgeStage.Completed));
            return new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, deleted,
                LimitReached: deleted >= maximumMessages);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, deleted, "Cancelled.");
        }
        catch (Exception exception)
        {
            return new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, deleted,
                exception.GetBaseException().Message);
        }
        finally
        {
            await ReleaseQuietlyAsync(channel, skipped).ConfigureAwait(false);
        }
    }

    private static async Task<IReadOnlyList<DeadLetterMessageDeletionResult>> DeleteFromChannelAsync(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        IReadOnlyList<DeadLetterMessageKey> keys,
        ILeasedMessageChannel channel,
        int maximumScanned,
        DeadLetterJsonBackupSession backupSession,
        int queueNumber,
        int queueCount,
        IProgress<DeadLetterMessageDeletionProgress>? progress,
        CancellationToken cancellationToken)
    {
        var outcomes = keys.ToDictionary(key => key, _ => (Outcome: DeadLetterMessageDeletionOutcome.NotFound, Detail: (string?)null));
        var pending = keys.ToList();
        var held = new List<LeasedMessage>();
        var scanned = 0;
        var deleted = 0;
        var emptyReceives = 0;
        try
        {
            while (pending.Count > 0 && scanned < maximumScanned && emptyReceives < EmptyReceiveConfirmations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var batch = await channel.ReceiveAsync(channel.MaximumBatchSize, cancellationToken).ConfigureAwait(false);
                if (batch.Count == 0)
                {
                    emptyReceives++;
                    continue;
                }

                emptyReceives = 0;
                foreach (var message in batch)
                {
                    scanned++;
                    var key = message.BelongsToSource ? pending.FirstOrDefault(candidate => Matches(candidate, message.Message)) : null;
                    if (key is null)
                    {
                        held.Add(message);
                        continue;
                    }

                    pending.Remove(key);
                    try
                    {
                        await backupSession.BackupAsync(message.Message, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        held.Add(message);
                        outcomes[key] = (DeadLetterMessageDeletionOutcome.Failed, $"Backup failed: {exception.GetBaseException().Message}");
                        continue;
                    }

                    var failed = await channel.SettleAsync([message], CancellationToken.None).ConfigureAwait(false);
                    if (failed.Count == 0)
                    {
                        deleted++;
                        outcomes[key] = (DeadLetterMessageDeletionOutcome.Deleted, null);
                    }
                    else
                    {
                        held.Add(message);
                        outcomes[key] = (DeadLetterMessageDeletionOutcome.Failed, "The message was backed up but could not be deleted.");
                    }
                }

                progress?.Report(new DeadLetterMessageDeletionProgress(source, subQueue, queueNumber, queueCount, scanned, deleted));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            foreach (var key in pending)
            {
                outcomes[key] = (DeadLetterMessageDeletionOutcome.Cancelled, null);
            }
        }
        catch (Exception exception)
        {
            foreach (var key in pending)
            {
                outcomes[key] = (DeadLetterMessageDeletionOutcome.Failed, exception.GetBaseException().Message);
            }
        }
        finally
        {
            await ReleaseQuietlyAsync(channel, held).ConfigureAwait(false);
        }

        return keys.Select(key => new DeadLetterMessageDeletionResult(key, outcomes[key].Outcome, outcomes[key].Detail)).ToArray();

        static bool Matches(DeadLetterMessageKey key, BrowsedMessage message) =>
            key.MessageId is { Length: > 0 } messageId
                ? string.Equals(messageId, message.Properties.MessageId, StringComparison.Ordinal)
                : key.SequenceNumber == message.SequenceNumber;
    }

    private static async Task ReleaseQuietlyAsync(ILeasedMessageChannel channel, List<LeasedMessage> held)
    {
        if (held.Count == 0)
        {
            return;
        }

        try
        {
            await channel.ReleaseAsync(held, CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Releasing is a courtesy: a held message becomes visible again by itself once its hold expires.
        }
        held.Clear();
    }

    private async Task<ServiceBusTopology> GetTopologyCoreAsync(bool forceRefresh, CancellationToken cancellationToken)
    {
        GetConnectedProfile();
        await _topologyGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!forceRefresh && _cachedTopology is { } cached &&
                TimeProvider.GetUtcNow() - cached.FetchedAt < TopologyCacheDuration)
            {
                return cached;
            }

            _cachedTopology = await ReadTopologyAsync(cancellationToken).ConfigureAwait(false);
            return _cachedTopology;
        }
        finally
        {
            _topologyGate.Release();
        }
    }

    private void EnsureWriteAllowed()
    {
        var profile = GetConnectedProfile();
        if (!profile.CanWrite)
        {
            throw new InvalidOperationException(
                $"Environment '{profile.Name}' is read-only. Unlock write access before changing messages.");
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
}
