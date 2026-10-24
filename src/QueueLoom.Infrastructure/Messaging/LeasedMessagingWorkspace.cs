using System.Collections.Concurrent;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
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
public abstract class LeasedMessagingWorkspace : IServiceBusWorkspace, ICleanupWarningSource, IWorkspaceNoticeSource
{
    private static readonly TimeSpan TopologyCacheDuration = TimeSpan.FromMinutes(1);
    internal const int EmptyReceiveConfirmations = 2;
    internal const int LoadAllLimit = 5_000;

    private readonly DeadLetterJsonBackupStore _backupStore;
    private readonly AsyncOperationGate _operationGate = new();
    private readonly SemaphoreSlim _topologyGate = new(1, 1);
    private readonly ConcurrentDictionary<string, DeadLetterMeasurement> _previousDeadLetterCounts = new(StringComparer.Ordinal);
    private ServiceBusProfile? _profile;
    private ServiceBusTopology? _cachedTopology;
    private WorkspaceConnectionState _connectionState;
    private volatile bool _disposed;
    private readonly TaskCompletionSource _disposalCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _disposeStarted;

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
    public string? ConnectedConfigurationIdentity => _profile is null ? null : ScheduledResend.IdentityFor(_profile);
    public MessagingProvider? ConnectedProvider => _profile?.Provider;

    /// <summary>Creates the service clients and makes one cheap call that proves the credentials work.</summary>
    protected abstract Task OpenAsync(ServiceBusProfile profile, CancellationToken cancellationToken);

    protected abstract ValueTask CloseAsync();

    protected abstract Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken cancellationToken);

    /// <summary>Resolves a queue or subscription (or its dead-letter destination) to something that can be received from.</summary>
    protected abstract ILeasedMessageChannel OpenChannel(
        ServiceBusTopology topology,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue);

    /// <summary>The channel for browsing; services that keep a log (Kafka) start where the request asks.</summary>
    protected virtual ILeasedMessageChannel OpenBrowseChannel(ServiceBusTopology topology, BrowseMessagesRequest request) =>
        OpenChannel(topology, request.Source, request.SubQueue);

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
                // Observations are kept per environment across reconnects (a global sweep reconnects each one): a
                // delayed point fetched after a reconnect must not overrule what was already seen.
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
            ThrowIfDisposed();
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
        ThrowIfDisposed();
        return await GetTopologyCoreAsync(forceRefresh, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
        BrowseMessagesRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        if (request.SessionId is not null)
        {
            throw new NotSupportedException("Only Azure Service Bus queues and subscriptions have sessions to open by id.");
        }

        // There is no position to continue from: every browse starts at whatever the service hands out
        // first. "Load more" therefore returns the first page again rather than duplicating messages.
        var topology = await GetTopologyCoreAsync(false, cancellationToken).ConfigureAwait(false);
        var channel = OpenBrowseChannel(topology, request);
        var limit = request.LoadAll ? LoadAllLimit : request.MaxMessages;
        var held = new List<LeasedMessage>();
        try
        {
            var messages = await ReceiveUpToAsync(channel, limit, held, cancellationToken, TimeProvider).ConfigureAwait(false);
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
        ThrowIfDisposed();

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
                var received = await ReceivePageAsync(channel, request.MaximumMessagesPerTarget, held, cancellationToken, TimeProvider)
                    .ConfigureAwait(false);
                var scanned = received.Messages;
                var matches = new List<BrowsedMessage>();
                var undecided = 0;
                var incomplete = 0;
                foreach (var message in scanned)
                {
                    // Outside the "enqueued within" window: never a match, so it cannot use up the result cap.
                    if (!request.IsInWindow(message.Message.EnqueuedAt))
                    {
                        continue;
                    }
                    bool isMatch;
                    try
                    {
                        isMatch = DeadLetterSearchMatcher.IsMatch(message.Message, request.Search);
                    }
                    catch (System.Text.RegularExpressions.RegexMatchTimeoutException)
                    {
                        // Not a "no": the source is reported as incomplete, and the scan goes on.
                        undecided++;
                        continue;
                    }
                    catch (IncompleteBodySearchException)
                    {
                        incomplete++;
                        continue;
                    }
                    if (!isMatch)
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
                    // SQS FIFO hands out no more of a group while the scan holds some of it: the rest stays unseen.
                    ScanLimitReached: received.LimitReached ||
                                      (channel.ReadsOneBatchPerMessageGroup && held.Count > 0),
                    Error: DeadLetterSearchSourceResult.UndecidedError(undecided, incomplete)));
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
        ThrowIfDisposed();
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
        ThrowIfDisposed();
        EnsureWriteAllowed();

        var profile = GetConnectedProfile();
        var startedAt = TimeProvider.GetUtcNow();
        var topology = await GetTopologyCoreAsync(false, cancellationToken).ConfigureAwait(false);
        // Resolve every target before deleting anything, so a bad target fails the whole request up front.
        var targets = request.Targets
            .Where(target => target.SubQueue == ServiceBusSubQueue.DeadLetter)
            .Select(target => (Target: target, Channel: OpenChannel(topology, target.Source, target.SubQueue)))
            .ToArray();
        // Transfer targets are skipped (the default request names both kinds); a request for nothing else must not look done.
        if (targets.Length == 0)
            throw new NotSupportedException($"{Provider.DisplayName()} has no transfer dead-letter queues.");
        foreach (var target in targets)
            if (target.Channel.SourceAttributionError is { } error) throw new InvalidOperationException(error);
        await ValidateDeletionAsync(targets.Select(target => new DeletionTarget(target.Target.Source, target.Target.SubQueue, target.Channel)).ToArray(), cancellationToken)
            .ConfigureAwait(false);
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

    public async Task EnsureDeletableAsync(
        IReadOnlyCollection<(ServiceBusEntityReference Source, ServiceBusSubQueue SubQueue)> sources,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sources);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        EnsureWriteAllowed();
        // Only the provider's own check against the service: no topology read and no channel, so a provider without
        // such a check (Kafka, whose topology read lists consumer groups) is not touched at all.
        await ValidateDeletionAsync(sources.Distinct().Select(source => new DeletionTarget(source.Source, source.SubQueue, null)).ToArray(), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Checks with the service, not the cached topology, that dead letters of these sources may be deleted, before
    /// anything is backed up, deleted or published. Throws when one may not.
    /// </summary>
    protected virtual Task ValidateDeletionAsync(IReadOnlyList<DeletionTarget> targets, CancellationToken cancellationToken) =>
        Task.CompletedTask;

    /// <summary>
    /// A source whose dead letters are about to be deleted, with the channel the deletion will read and settle through
    /// (none for a move's check before publishing, which opens its channel later).
    /// </summary>
    protected readonly record struct DeletionTarget(ServiceBusEntityReference Source, ServiceBusSubQueue SubQueue, ILeasedMessageChannel? Channel);

    public async Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(
        DeleteDeadLetterMessagesRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterMessageDeletionProgress>? progress = null)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        EnsureWriteAllowed();

        // AMQP application IDs are optional and repeatable; delivery tags expire with their channel.
        // A later receive cannot prove which previously reviewed delivery this selection meant.
        if (Provider == MessagingProvider.RabbitMq)
            throw new NotSupportedException("RabbitMQ cannot safely identify a previously reviewed delivery for selected deletion. Use a backed-up purge of the reviewed source instead.");

        var profile = GetConnectedProfile();
        var startedAt = TimeProvider.GetUtcNow();
        var topology = await GetTopologyCoreAsync(false, cancellationToken).ConfigureAwait(false);
        var groups = request.BySubQueue
            .Select(group => (Group: group, Channel: OpenChannel(topology, group.Key.Source, group.Key.SubQueue)))
            .ToArray();
        foreach (var group in groups)
            if (group.Channel.SourceAttributionError is { } error) throw new InvalidOperationException(error);
        await ValidateDeletionAsync(groups.Select(group => new DeletionTarget(group.Group.Key.Source, group.Group.Key.SubQueue, group.Channel)).ToArray(), cancellationToken)
            .ConfigureAwait(false);
        var backupSession = await _backupStore.CreateSessionAsync(profile, startedAt, cancellationToken)
            .ConfigureAwait(false);
        var results = new List<DeadLetterMessageDeletionResult>(request.Messages.Count);
        var warnings = new List<string>();

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
                    cancellationToken,
                    warnings.Add)
                .ConfigureAwait(false));
        }

        _cachedTopology = null;
        return new DeleteDeadLetterMessagesResult(profile.Id, startedAt, TimeProvider.GetUtcNow(), results,
            backupSession.RootDirectory) { Warnings = warnings.Distinct(StringComparer.Ordinal).ToArray() };
    }

    public virtual bool SupportsNativeRedrive => false;

    public virtual Task<NativeRedrivePlan> PlanNativeRedriveAsync(ServiceBusEntityReference source, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("Only Amazon SQS moves a dead-letter queue back to its source queues itself.");

    public virtual Task<string> StartNativeRedriveAsync(NativeRedrivePlan plan, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("Only Amazon SQS moves a dead-letter queue back to its source queues itself.");

    public virtual QueueManagementCapabilities? QueueManagement => null;

    public virtual Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} does not support queue management in QueueLoom.");

    public virtual Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} does not support queue management in QueueLoom.");

    public virtual Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} does not support queue management in QueueLoom.");

    public virtual Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} does not support queue management in QueueLoom.");

    public virtual bool SupportsSubscriptionRules => false;

    public virtual RoutingService RoutingService => RoutingService.ServiceBus;

    public virtual string? RuleEditingNote => null;

    public virtual Task<IReadOnlyList<SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} has no subscription rules.");

    public virtual Task SaveSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule, bool replace,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} cannot change subscription rules in QueueLoom.");

    public virtual Task DeleteSubscriptionRuleAsync(string topic, string subscription, string rule, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{Provider.DisplayName()} cannot change subscription rules in QueueLoom.");

    public virtual Task DeleteSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule,
        CancellationToken cancellationToken = default) =>
        DeleteSubscriptionRuleAsync(topic, subscription, (rule ?? throw new ArgumentNullException(nameof(rule))).Name, cancellationToken);

    /// <summary>Reads what rules need; the topology is refreshed afterwards since a change may be visible in it.</summary>
    protected async Task<T> ReadRulesAsync<T>(Func<CancellationToken, Task<T>> read, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        GetConnectedProfile();
        return await read(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Runs a queue management change: allowed only with the environment's permission and write access.</summary>
    protected async Task ManageAsync(Func<CancellationToken, Task> change, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
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
        ThrowIfDisposed();
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
            if (source.Runtime.DeadLetterCountError is { } countError)
            {
                snapshots.Add(new DeadLetterEntitySnapshot(source.Reference, null, null, countError)
                    { MeasuredFrom = source.Runtime.DeadLetterCountSource, CountsOnlyThisSource = source.Runtime.CountsUnavailable });
                continue;
            }
            long count;
            // Sampled by reading: what was seen, never the queue's proven size (nor, when nothing was seen, proof of empty).
            var sampled = source.Runtime.CountsUnavailable;
            if (sampled)
            {
                try
                {
                    count = await SampleDeadLetterCountAsync(topology, source.Reference, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    snapshots.Add(new DeadLetterEntitySnapshot(source.Reference, null, null, exception.GetBaseException().Message)
                        { MeasuredFrom = source.Runtime.DeadLetterCountSource, CountsOnlyThisSource = true });
                    continue;
                }
            }
            else
            {
                count = source.Runtime.MessageCounts.DeadLetter;
            }

            // Per environment and its configuration: an edited profile (another project, region or account) keeps
            // its ID but is not the place the earlier observations came from.
            var key = $"{ConnectedConfigurationIdentity}|{source.Reference.Path}";
            DeadLetterMeasurement? previous = _previousDeadLetterCounts.TryGetValue(key, out var value) ? value : null;
            var quality = sampled ? DeadLetterCountQuality.LowerBound
                : source.Runtime.CountsAreEstimates ? DeadLetterCountQuality.Estimated
                : DeadLetterCountQuality.Exact;
            // A sample is true now; a reported count as of its own time (a Cloud Monitoring point can be minutes old).
            var current = new DeadLetterMeasurement(count, quality)
            {
                MeasuredAt = sampled ? TimeProvider.GetUtcNow() : source.Runtime.DeadLetterCountMeasuredAt,
                MeasuredFrom = source.Runtime.DeadLetterCountSource,
                // A sample keeps only this source's messages of the shared destination: it is no shared count, and a
                // fresh sample never stands in for the destination's whole count (nor the reverse).
                OfSourceOnly = sampled
            };
            var stale = false;
            if (previous is { } newer && DeadLetterMeasurement.SameTarget(current, newer) && DeadLetterMeasurement.IsOlder(current, newer))
            {
                // Older than what this source already showed: the newer observation stands, it is not overruled.
                current = newer;
                stale = true;
            }
            _previousDeadLetterCounts[key] = current;
            snapshots.Add(new DeadLetterEntitySnapshot(source.Reference, current.Count, previous?.Count)
            {
                CountQuality = current.Quality,
                PreviousQuality = previous?.Quality ?? DeadLetterCountQuality.Exact,
                MeasuredAt = current.MeasuredAt,
                MeasuredFrom = current.MeasuredFrom,
                PreviousMeasuredFrom = previous?.MeasuredFrom,
                // What the standing observation is: this read, or a newer one of the same kind it did not overrule.
                CountsOnlyThisSource = current.OfSourceOnly,
                // Only with this read's own count: an older observation that was rejected keeps no checkpoint of this read.
                Offsets = stale ? null : source.Runtime.DeadLetterOffsets
            });
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
            var messages = await ReceiveUpToAsync(channel, SampleLimit, held, cancellationToken, TimeProvider).ConfigureAwait(false);
            return messages.Count;
        }
        finally
        {
            await ReleaseQuietlyAsync(channel, held).ConfigureAwait(false);
        }
    }

    internal const int SampleLimit = DeadLetterCountText.SampleLimit;

    /// <summary>Read-only provider management calls also retain clients until their entire scope finishes.</summary>
    protected async ValueTask<IDisposable> EnterReadOperationAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        try { ThrowIfDisposed(); return operation; }
        catch { operation.Dispose(); throw; }
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) == 0)
        {
            _disposed = true;
            _ = CompleteDisposalAsync();
        }
        return new ValueTask(_disposalCompletion.Task);
    }

    private async Task CompleteDisposalAsync()
    {
        try
        {
            // Held-message release and connection cleanup use clients even after caller
            // cancellation. Lifecycle admission waits until those entire scopes finish.
            using (await _operationGate.EnterLifecycleAsync().ConfigureAwait(false))
            {
                await CloseAsync().ConfigureAwait(false);
                _profile = null;
                _cachedTopology = null;
                _connectionState = WorkspaceConnectionState.Disconnected;
                _topologyGate.Dispose();
            }
            GC.SuppressFinalize(this);
            _disposalCompletion.TrySetResult();
        }
        catch (Exception error) { _disposalCompletion.TrySetException(error); }
    }

    /// <summary>
    /// Reads the topology again inside an operation that already holds the operation lease (the public
    /// <see cref="GetTopologyAsync"/> would wait for that lease itself).
    /// </summary>
    protected Task<ServiceBusTopology> RefreshTopologyInOperationAsync(CancellationToken cancellationToken) =>
        GetTopologyCoreAsync(true, cancellationToken);

    protected ServiceBusProfile GetConnectedProfile() =>
        _profile ?? throw new InvalidOperationException("Connect to an environment first.");

    /// <summary>Receives until <paramref name="limit"/> messages of this source are held or the channel runs dry.</summary>
    /// <param name="held">Every received message, including other sources' ones; the caller releases them.</param>
    internal static async Task<List<LeasedMessage>> ReceiveUpToAsync(
        ILeasedMessageChannel channel,
        int limit,
        List<LeasedMessage> held,
        CancellationToken cancellationToken,
        TimeProvider? time = null) =>
        (await ReceivePageAsync(channel, limit, held, cancellationToken, time).ConfigureAwait(false)).Messages;

    // Source messages and total held receipts have separate caps. Only confirmed empty receives establish exhaustion:
    // a source that held exactly `limit` messages and then ran dry is still reported as limit-reached (incomplete),
    // because the loop stops at the cap without confirming that nothing more is there.
    private static async Task<(List<LeasedMessage> Messages, bool LimitReached)> ReceivePageAsync(
        ILeasedMessageChannel channel,
        int limit,
        List<LeasedMessage> held,
        CancellationToken cancellationToken,
        TimeProvider? time = null)
    {
        var renewal = new HoldRenewal(channel, time ?? TimeProvider.System);
        if (channel.SourceAttributionError is { } error) throw new InvalidOperationException(error);
        var result = new List<LeasedMessage>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        // A shared dead-letter queue can hold many messages of other sources; stop scanning at some point.
        var heldLimit = (int)Math.Min(20_000L, Math.Max(1_000L, limit * 2L));
        var emptyReceives = 0;
        while (result.Count < limit && emptyReceives < EmptyReceiveConfirmations && held.Count < heldLimit)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await renewal.RenewIfDueAsync(held, cancellationToken).ConfigureAwait(false);
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

        return (result, result.Count >= limit || held.Count >= heldLimit);
    }

    /// <summary>How many messages of other sources (or repeated deliveries) a purge holds before it stops.</summary>
    internal const int MaximumHeldDuringPurge = 20_000;

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
        DeadLetterPurgeSourceResult? result = null;
        string? cleanupError = null;
        long backedUp = 0;
        long deleted = 0;
        var emptyReceives = 0;
        var outstanding = new List<LeasedMessage>();
        var renewal = new HoldRenewal(channel, TimeProvider);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        progress?.Report(new DeadLetterPurgeProgress(target.Source, target.SubQueue, targetNumber, targetCount, 0, 0,
            DeadLetterPurgeStage.Starting));
        try
        {
            while (deleted < maximumMessages && emptyReceives < EmptyReceiveConfirmations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var wanted = (int)Math.Min(Math.Min(batchSize, channel.MaximumBatchSize), maximumMessages - deleted);
                await renewal.RenewIfDueAsync(outstanding, cancellationToken).ConfigureAwait(false);
                var requestedAt = renewal.Now();
                var batch = await channel.ReceiveAsync(wanted, cancellationToken).ConfigureAwait(false);
                outstanding.AddRange(batch);
                renewal.Received(batch, requestedAt);
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
                        continue;
                    }

                    await backupSession.BackupAsync(message.Message, cancellationToken).ConfigureAwait(false);
                    backedUp++;
                    toSettle.Add(message);
                }

                progress?.Report(new DeadLetterPurgeProgress(target.Source, target.SubQueue, targetNumber, targetCount,
                    backedUp, deleted, DeadLetterPurgeStage.Deleting));
                // A message whose hold may have ended (a slow backup, a renewal that failed) is not settled: the broker may
                // have handed it to another consumer, and Pub/Sub acknowledges an expired ack ID with success although the
                // message stays. Counted as deleted, its backup would later be restored as a duplicate.
                var lapsed = toSettle.Where(renewal.HasLapsed).ToList();
                toSettle.RemoveAll(lapsed.Contains);
                var failed = await channel.SettleAsync(toSettle, CancellationToken.None).ConfigureAwait(false);
                var settledNow = toSettle.Except(failed).ToList();
                foreach (var settled in settledNow) outstanding.Remove(settled);
                renewal.Forget(settledNow);
                deleted += toSettle.Count - failed.Count;
                if (failed.Count > 0 || lapsed.Count > 0)
                {
                    var errors = new List<string>(2);
                    if (failed.Count > 0)
                        errors.Add($"{failed.Count} message(s) could not be deleted from {channel.PhysicalName}.");
                    if (lapsed.Count > 0)
                        errors.Add($"{lapsed.Count} message(s) were held in {channel.PhysicalName} longer than their hold lasts, so they " +
                                   "were not deleted: they may have been delivered again.");
                    result = new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, deleted,
                        string.Join(" ", errors) + " Their backups are kept.");
                    break;
                }
                if (outstanding.Count >= MaximumHeldDuringPurge)
                {
                    // A shared dead-letter queue full of other sources' messages: held ones come back after their
                    // hold ends, so the receive never runs dry. Stop instead of scanning forever.
                    result = new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, deleted,
                        $"Stopped after {outstanding.Count:N0} messages of other sources in {channel.PhysicalName}; run the purge again for the rest.");
                    break;
                }
            }

            progress?.Report(new DeadLetterPurgeProgress(target.Source, target.SubQueue, targetNumber, targetCount,
                backedUp, deleted, DeadLetterPurgeStage.Completed));
            // SQS FIFO hands out no more of a group while other sources' messages of it are held here, so running dry is
            // not the end: this source's messages behind them were never seen.
            if (result is null && deleted < maximumMessages && channel.ReadsOneBatchPerMessageGroup && outstanding.Count > 0)
            {
                result = new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, deleted,
                    $"Stopped early: SQS FIFO hands out no more of a message group while messages of other sources in it are held, so messages " +
                    $"of this source may remain in {channel.PhysicalName}. Move or delete those messages, then purge again.");
            }
            result ??= new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, deleted,
                LimitReached: deleted >= maximumMessages);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            result ??= new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, deleted, "Cancelled.");
        }
        catch (Exception exception)
        {
            result ??= new DeadLetterPurgeSourceResult(target.Source, target.SubQueue, deleted,
                exception.GetBaseException().Message);
        }
        finally
        {
            cleanupError = await ReleaseQuietlyAsync(channel, outstanding).ConfigureAwait(false);
        }

        return cleanupError is null ? result! : result! with
        {
            Error = string.IsNullOrWhiteSpace(result!.Error) ? cleanupError : result.Error + " " + cleanupError
        };
    }

    private async Task<IReadOnlyList<DeadLetterMessageDeletionResult>> DeleteFromChannelAsync(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        IReadOnlyList<DeadLetterMessageKey> keys,
        ILeasedMessageChannel channel,
        int maximumScanned,
        DeadLetterJsonBackupSession backupSession,
        int queueNumber,
        int queueCount,
        IProgress<DeadLetterMessageDeletionProgress>? progress,
        CancellationToken cancellationToken,
        Action<string>? cleanupWarning = null)
    {
        var outcomes = keys.ToDictionary(key => key, _ => (Outcome: DeadLetterMessageDeletionOutcome.NotFound, Detail: (string?)null));
        var pending = keys.ToList();
        var attempted = new HashSet<DeadLetterMessageKey>();
        var settlementAttempted = new HashSet<DeadLetterMessageKey>();
        var held = new List<LeasedMessage>();
        var renewal = new HoldRenewal(channel, TimeProvider);
        var scanned = 0;
        var deleted = 0;
        var emptyReceives = 0;
        try
        {
            while (pending.Any(key => !attempted.Contains(key)) && scanned < maximumScanned && emptyReceives < EmptyReceiveConfirmations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await renewal.RenewIfDueAsync(held, cancellationToken).ConfigureAwait(false);
                var requestedAt = renewal.Now();
                var batch = await channel.ReceiveAsync(channel.MaximumBatchSize, cancellationToken).ConfigureAwait(false);
                renewal.Received(batch, requestedAt);
                if (batch.Count == 0)
                {
                    emptyReceives++;
                    continue;
                }

                emptyReceives = 0;
                // Track the complete received batch before backup, cancellation or settlement can interrupt it.
                held.AddRange(batch);
                foreach (var message in batch)
                {
                    scanned++;
                    var key = message.BelongsToSource ? pending.FirstOrDefault(candidate => !attempted.Contains(candidate) && Matches(candidate, message.Message)) : null;
                    if (key is null)
                    {
                        continue;
                    }

                    attempted.Add(key);
                    try
                    {
                        await backupSession.BackupAsync(message.Message, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        outcomes[key] = (DeadLetterMessageDeletionOutcome.Failed, $"Backup failed: {exception.GetBaseException().Message}");
                        continue;
                    }

                    if (renewal.HasLapsed(message))
                    {
                        // Its hold may have ended: settling it could report a deletion that did not happen (see the purge).
                        outcomes[key] = (DeadLetterMessageDeletionOutcome.Failed,
                            "The message was backed up but held longer than its hold lasts, so it was not deleted: it may have been " +
                            "delivered again. Inspect the source and backup before retrying.");
                        continue;
                    }

                    settlementAttempted.Add(key);
                    var failed = await channel.SettleAsync([message], CancellationToken.None).ConfigureAwait(false);
                    if (failed.Count == 0)
                    {
                        deleted++;
                        held.Remove(message);
                        renewal.Forget([message]);
                        pending.Remove(key);
                        outcomes[key] = (DeadLetterMessageDeletionOutcome.Deleted, null);
                    }
                    else
                    {
                        outcomes[key] = (DeadLetterMessageDeletionOutcome.Failed, "The message was backed up but could not be deleted.");
                    }
                }

                progress?.Report(new DeadLetterMessageDeletionProgress(source, subQueue, queueNumber, queueCount, scanned, deleted));
            }
            // SQS FIFO hands out no more of a group while this scan holds some of it (as the search accounts for): running
            // dry then does not mean the selected messages are gone, only that they were not reached.
            if (channel.ReadsOneBatchPerMessageGroup && held.Count > 0)
            {
                foreach (var key in pending.Where(key => !attempted.Contains(key)))
                {
                    outcomes[key] = (DeadLetterMessageDeletionOutcome.NotFound,
                        "Not reached: SQS FIFO hands out no more of a message group while earlier messages of it are held, so it may still be " +
                        "in the queue. Delete or move the messages before it, then try again.");
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            foreach (var key in pending)
            {
                if (outcomes[key].Outcome != DeadLetterMessageDeletionOutcome.Failed)
                    outcomes[key] = settlementAttempted.Contains(key)
                        ? (DeadLetterMessageDeletionOutcome.Failed, "Deletion outcome is unknown: the message may already have been deleted. Inspect the source and backup before retrying.")
                        : (DeadLetterMessageDeletionOutcome.Cancelled, null);
            }
        }
        catch (Exception exception)
        {
            foreach (var key in pending)
            {
                if (outcomes[key].Outcome != DeadLetterMessageDeletionOutcome.Failed)
                    outcomes[key] = (DeadLetterMessageDeletionOutcome.Failed, exception.GetBaseException().Message);
            }
        }
        finally
        {
            if (await ReleaseQuietlyAsync(channel, held).ConfigureAwait(false) is { } warning)
                cleanupWarning?.Invoke(warning);
        }

        return keys.Select(key => new DeadLetterMessageDeletionResult(key, outcomes[key].Outcome, outcomes[key].Detail)).ToArray();

        // The sequence number is the broker's own; the message ID is the producer's and need not be unique (copies resent
        // with their IDs share it), so it is checked as well, never instead.
        static bool Matches(DeadLetterMessageKey key, BrowsedMessage message) =>
            key.SequenceNumber == message.SequenceNumber &&
            (key.MessageId is not { Length: > 0 } messageId || string.Equals(messageId, message.Properties.MessageId, StringComparison.Ordinal));
    }

    /// <inheritdoc />
    public event EventHandler<string>? CleanupWarning;

    /// <summary>Announces a cleanup failure without allowing a listener to replace the operation's result.</summary>
    protected void ReportCleanupWarning(string error)
    {
        try
        {
            CleanupWarning?.Invoke(this, error);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            // A listener's failure must not turn the operation's result into an error.
        }
    }

    /// <inheritdoc />
    public event EventHandler<WorkspaceNotice>? Notice;

    /// <summary>Announces a notice without allowing a listener to replace the operation's result.</summary>
    protected void ReportNotice(string title, string details)
    {
        try
        {
            Notice?.Invoke(this, new WorkspaceNotice(title, details));
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            // A listener's failure must not turn the operation's result into an error.
        }
    }

    /// <summary>Releases what is still held; a cleanup failure is returned and announced, never thrown.</summary>
    private async Task<string?> ReleaseQuietlyAsync(ILeasedMessageChannel channel, List<LeasedMessage> held)
    {
        var error = await ReleaseQuietlyCoreAsync(channel, held).ConfigureAwait(false);
        if (error is not null)
        {
            ReportCleanupWarning(error);
        }
        return error;
    }

    private static async Task<string?> ReleaseQuietlyCoreAsync(ILeasedMessageChannel channel, List<LeasedMessage> held)
    {
        string? error = null;
        try
        {
            if (held.Count > 0) await channel.ReleaseAsync(held, CancellationToken.None).ConfigureAwait(false);
        }
        catch (IOException exception)
        {
            // The channel says what it could not release and what that means (for example SQS: visible again later).
            error = exception.Message;
        }
        catch
        {
            error = "Channel cleanup failed; verify remaining deliveries before retrying.";
        }
        finally
        {
            held.Clear();
            try
            {
                if (channel is IAsyncDisposable asynchronous)
                    await asynchronous.DisposeAsync().ConfigureAwait(false);
                else if (channel is IDisposable synchronous)
                    synchronous.Dispose();
            }
            catch { error ??= "Channel cleanup failed; verify remaining deliveries before retrying."; }
        }

        return error;
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

            _cachedTopology = (await ReadTopologyAsync(cancellationToken).ConfigureAwait(false)) with { EntityNamesCaseSensitive = true };
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
