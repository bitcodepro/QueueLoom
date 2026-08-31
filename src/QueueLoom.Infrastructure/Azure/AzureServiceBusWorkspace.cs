using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Azure;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Azure;

public sealed class AzureServiceBusWorkspace : IServiceBusWorkspace
{
    private static readonly TimeSpan TopologyCacheDuration = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan InteractiveTryTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan InteractiveRetryDelay = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan InteractiveMaximumRetryDelay = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan PurgeReceiveWaitTime = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan PurgeVerificationTimeout = TimeSpan.FromSeconds(2);
    private const int MonitorConcurrency = 6;
    private const int SearchConcurrency = 12;
    private const int BrowseBatchSize = 250;
    private const int InteractiveMaximumRetries = 2;
    private const int PurgeEmptyReceiveConfirmations = 2;
    private const int BackupWriteConcurrency = 4;
    private const string SessionEnabledEntityError =
        "Session-enabled queues and subscriptions are not supported by the current safe message workflow. No messages were changed.";

    private readonly ISecretVault _secretVault;
    private readonly DeadLetterJsonBackupStore _backupStore;
    private readonly TimeProvider _timeProvider;
    private readonly AsyncOperationGate _operationGate = new();
    private readonly SemaphoreSlim _topologyGate = new(1, 1);
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _previousDeadLetterCounts = new(StringComparer.Ordinal);

    private ServiceBusClient? _client;
    private ServiceBusAdministrationClient? _administration;
    private ServiceBusProfile? _profile;
    private ServiceBusTopology? _cachedTopology;
    private WorkspaceConnectionState _connectionState;
    private bool _disposed;

    public AzureServiceBusWorkspace(
        ISecretVault secretVault,
        TimeProvider? timeProvider = null,
        DeadLetterJsonBackupStore? backupStore = null)
    {
        ArgumentNullException.ThrowIfNull(secretVault);
        _secretVault = secretVault;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _backupStore = backupStore ?? new DeadLetterJsonBackupStore(QueueLoomPaths.CreateDefault());
    }

    public WorkspaceConnectionState ConnectionState => _connectionState;

    public Guid? ConnectedProfileId => _profile?.Id;

    public async Task ConnectAsync(
        ServiceBusProfile profile,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ThrowIfDisposed();

        using (await _operationGate.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false))
        {
            ThrowIfDisposed();
            _connectionState = WorkspaceConnectionState.Connecting;
            await DisposeClientsAsync().ConfigureAwait(false);

            ServiceBusClient? client = null;
            ServiceBusAdministrationClient? administration = null;
            try
            {
                var clientOptions = CreateMessagingClientOptions();
                var administrationOptions = CreateAdministrationClientOptions();

                if (profile.Authentication.Kind == AuthenticationKind.ConnectionString)
                {
                    var connectionString = await _secretVault.RetrieveAsync(
                        ProfileSecretKey.ConnectionString(profile.Id),
                        cancellationToken).ConfigureAwait(false);
                    if (string.IsNullOrWhiteSpace(connectionString))
                    {
                        throw new InvalidOperationException("This environment has no saved connection string.");
                    }

                    var parsed = ServiceBusConnectionStringProperties.Parse(connectionString);
                    if (!string.IsNullOrWhiteSpace(parsed.EntityPath))
                    {
                        throw new InvalidOperationException(
                            "QueueLoom requires a namespace-level connection string without EntityPath to list all queues and topics.");
                    }

                    client = new ServiceBusClient(connectionString, clientOptions);
                    administration = new ServiceBusAdministrationClient(connectionString, administrationOptions);
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(profile.FullyQualifiedNamespace))
                    {
                        throw new InvalidOperationException("A fully qualified Service Bus namespace is required for Entra ID.");
                    }

                    var settings = profile.Authentication.EntraId
                        ?? throw new InvalidOperationException("Entra ID settings are missing.");
                    TokenCredential credential = AzureCredentialFactory.Create(settings);
                    client = new ServiceBusClient(profile.FullyQualifiedNamespace, credential, clientOptions);
                    administration = new ServiceBusAdministrationClient(
                        profile.FullyQualifiedNamespace,
                        credential,
                        administrationOptions);
                }

                // This validates both the endpoint and the Manage/Data Owner permission needed
                // for topology discovery, without receiving or locking any messages.
                await administration.GetNamespacePropertiesAsync(cancellationToken).ConfigureAwait(false);

                _client = client;
                _administration = administration;
                _profile = profile;
                _cachedTopology = null;
                _previousDeadLetterCounts.Clear();
                _connectionState = WorkspaceConnectionState.Connected;
            }
            catch
            {
                if (client is not null)
                {
                    await client.DisposeAsync().ConfigureAwait(false);
                }
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
            await DisposeClientsAsync().ConfigureAwait(false);
            _connectionState = WorkspaceConnectionState.Disconnected;
        }
    }

    public Task SetAccessModeAsync(
        ProfileAccessMode accessMode,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        var profile = GetConnectedProfile();
        _profile = profile with { AccessMode = accessMode };
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

    private async Task<ServiceBusTopology> GetTopologyCoreAsync(
        bool forceRefresh,
        CancellationToken cancellationToken)
    {
        var cached = _cachedTopology;
        if (!forceRefresh && cached is not null &&
            _timeProvider.GetUtcNow() - cached.FetchedAt < TopologyCacheDuration)
        {
            return cached;
        }

        await _topologyGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            cached = _cachedTopology;
            if (!forceRefresh && cached is not null &&
                _timeProvider.GetUtcNow() - cached.FetchedAt < TopologyCacheDuration)
            {
                return cached;
            }

            var administration = GetAdministrationClient();
            var queuePropertiesTask = ReadAllAsync(administration.GetQueuesAsync(cancellationToken), cancellationToken);
            var queueRuntimeTask = ReadAllAsync(administration.GetQueuesRuntimePropertiesAsync(cancellationToken), cancellationToken);
            var topicPropertiesTask = ReadAllAsync(administration.GetTopicsAsync(cancellationToken), cancellationToken);
            var topicRuntimeTask = ReadAllAsync(administration.GetTopicsRuntimePropertiesAsync(cancellationToken), cancellationToken);

            await Task.WhenAll(queuePropertiesTask, queueRuntimeTask, topicPropertiesTask, topicRuntimeTask)
                .ConfigureAwait(false);

            var queueRuntime = queueRuntimeTask.Result.ToDictionary(item => item.Name, StringComparer.Ordinal);
            var queues = queuePropertiesTask.Result
                .Select(properties => MapQueue(properties, queueRuntime.GetValueOrDefault(properties.Name)))
                .OrderBy(queue => queue.Name, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var topicRuntime = topicRuntimeTask.Result.ToDictionary(item => item.Name, StringComparer.Ordinal);
            using var limiter = new SemaphoreSlim(MonitorConcurrency, MonitorConcurrency);
            var topicTasks = topicPropertiesTask.Result.Select(async topic =>
            {
                await limiter.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    return await MapTopicAsync(
                        administration,
                        topic,
                        topicRuntime.GetValueOrDefault(topic.Name),
                        cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    limiter.Release();
                }
            });

            var topics = (await Task.WhenAll(topicTasks).ConfigureAwait(false))
                .OrderBy(topic => topic.Name, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            return _cachedTopology = new ServiceBusTopology(_timeProvider.GetUtcNow(), queues, topics);
        }
        finally
        {
            _topologyGate.Release();
        }
    }

    public async Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
        BrowseMessagesRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();

        await EnsureSessionlessMessageSourceAsync(request.Source, cancellationToken).ConfigureAwait(false);
        var client = GetMessagingClient();
        var options = new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            PrefetchCount = 0,
            SubQueue = request.SubQueue switch
            {
                ServiceBusSubQueue.Active => SubQueue.None,
                ServiceBusSubQueue.DeadLetter => SubQueue.DeadLetter,
                ServiceBusSubQueue.TransferDeadLetter => SubQueue.TransferDeadLetter,
                _ => throw new ArgumentOutOfRangeException(nameof(request), request.SubQueue, "Unsupported subqueue.")
            }
        };

        await using var receiver = request.Source.Kind switch
        {
            ServiceBusEntityKind.Queue => client.CreateReceiver(request.Source.Name, options),
            ServiceBusEntityKind.Subscription => client.CreateReceiver(
                request.Source.TopicName!,
                request.Source.Name,
                options),
            _ => throw new ArgumentException("Only queues and subscriptions can be browsed.", nameof(request))
        };

        var result = new List<BrowsedMessage>();
        var seenSequenceNumbers = new HashSet<long>();
        var nextSequenceNumber = request.FromSequenceNumber;
        var remaining = request.LoadAll ? int.MaxValue : request.MaxMessages;

        while (remaining > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var pageSize = Math.Min(BrowseBatchSize, remaining);
            var page = await receiver.PeekMessagesAsync(
                    pageSize,
                    nextSequenceNumber,
                    cancellationToken)
                .ConfigureAwait(false);
            if (page.Count == 0)
            {
                break;
            }

            foreach (var message in page)
            {
                if (seenSequenceNumbers.Add(message.SequenceNumber))
                {
                    result.Add(AzureMessageMapper.FromAzure(message, request.Source, request.SubQueue));
                    remaining--;
                    if (remaining == 0)
                    {
                        break;
                    }
                }
            }

            var lastSequenceNumber = page[^1].SequenceNumber;
            if (lastSequenceNumber == long.MaxValue)
            {
                break;
            }

            var candidate = lastSequenceNumber + 1;
            if (nextSequenceNumber.HasValue && candidate <= nextSequenceNumber.Value)
            {
                break;
            }
            nextSequenceNumber = candidate;
        }

        return Array.AsReadOnly(result.ToArray());
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
        var startedAt = _timeProvider.GetUtcNow();
        var acceptedMatches = 0;
        var resultLimitReached = 0;
        using var limiter = new SemaphoreSlim(SearchConcurrency, SearchConcurrency);
        // Counters are not trusted as an exclusion filter, but they are valuable for
        // scheduling: likely non-empty sources return useful matches before stale-zero
        // probes when a namespace contains hundreds of subscriptions.
        var targetTasks = request.Targets
            .OrderByDescending(target => target.KnownMessageCount)
            .Select(target => (Target: target, Task: SearchAsync(target)))
            .ToArray();
        var sourceResults = Array.Empty<DeadLetterSearchSourceResult>();
        try
        {
            sourceResults = await Task.WhenAll(targetTasks.Select(item => item.Task)).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Preserve every source that completed before a per-environment timeout.
            // Incomplete sources remain explicit failures instead of making all useful
            // matches from the environment disappear with Task.WhenAll cancellation.
            sourceResults = targetTasks.Select(item => item.Task.IsCompletedSuccessfully
                    ? item.Task.Result
                    : new DeadLetterSearchSourceResult(
                        item.Target.Source,
                        item.Target.SubQueue,
                        0,
                        [],
                        Error: "Search timed out before this source completed."))
                .ToArray();
        }

        return new DeadLetterSearchResult(
            profile.Id,
            startedAt,
            _timeProvider.GetUtcNow(),
            sourceResults,
            resultLimitReached != 0);

        async Task<DeadLetterSearchSourceResult> SearchAsync(DeadLetterSearchTarget target)
        {
            await limiter.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await SearchTargetAsync(
                        target,
                        request,
                        message =>
                        {
                            var position = Interlocked.Increment(ref acceptedMatches);
                            if (position <= request.MaximumResults)
                            {
                                return AzureMessageMapper.FromAzure(message, target.Source, target.SubQueue);
                            }

                            Interlocked.Exchange(ref resultLimitReached, 1);
                            return null;
                        },
                        () =>
                        {
                            if (Volatile.Read(ref acceptedMatches) < request.MaximumResults)
                            {
                                return false;
                            }

                            Interlocked.Exchange(ref resultLimitReached, 1);
                            return true;
                        },
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                limiter.Release();
            }
        }
    }

    private async Task<DeadLetterSearchSourceResult> SearchTargetAsync(
        DeadLetterSearchTarget target,
        DeadLetterSearchRequest request,
        Func<ServiceBusReceivedMessage, BrowsedMessage?> addMatch,
        Func<bool> shouldStop,
        CancellationToken cancellationToken)
    {
        try
        {
            await EnsureSessionlessMessageSourceAsync(target.Source, cancellationToken).ConfigureAwait(false);
            var options = new ServiceBusReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                PrefetchCount = 0,
                SubQueue = target.SubQueue switch
                {
                    ServiceBusSubQueue.DeadLetter => SubQueue.DeadLetter,
                    ServiceBusSubQueue.TransferDeadLetter => SubQueue.TransferDeadLetter,
                    _ => throw new ArgumentOutOfRangeException(nameof(target), target.SubQueue, "Unsupported search subqueue.")
                }
            };

            await using var receiver = target.Source.Kind switch
            {
                ServiceBusEntityKind.Queue => GetMessagingClient().CreateReceiver(target.Source.Name, options),
                ServiceBusEntityKind.Subscription => GetMessagingClient().CreateReceiver(
                    target.Source.TopicName!,
                    target.Source.Name,
                    options),
                _ => throw new ArgumentException("Only queues and subscriptions can be searched.", nameof(target))
            };

            return await SearchTargetPagesAsync(
                    target,
                    request,
                    (batchSize, fromSequenceNumber, token) => receiver.PeekMessagesAsync(
                        batchSize,
                        fromSequenceNumber,
                        token),
                    addMatch,
                    shouldStop,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return new DeadLetterSearchSourceResult(
                target.Source,
                target.SubQueue,
                0,
                [],
                Error: exception.GetBaseException().Message);
        }
    }

    internal static async Task<DeadLetterSearchSourceResult> SearchTargetPagesAsync(
        DeadLetterSearchTarget target,
        DeadLetterSearchRequest request,
        Func<int, long?, CancellationToken, Task<IReadOnlyList<ServiceBusReceivedMessage>>> peekAsync,
        Func<ServiceBusReceivedMessage, BrowsedMessage?> addMatch,
        Func<bool> shouldStop,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(peekAsync);
        ArgumentNullException.ThrowIfNull(addMatch);
        ArgumentNullException.ThrowIfNull(shouldStop);

        var matches = new List<BrowsedMessage>();
        var scanned = 0;
        try
        {
            var scan = await WalkSearchPagesAsync(
                    request.BatchSize,
                    GetSearchSafetyLimit(request, target),
                    peekAsync,
                    azureMessage =>
                    {
                        scanned = checked(scanned + 1);
                        if (!MatchesSearch(azureMessage, request.Query))
                        {
                            return;
                        }

                        var match = addMatch(azureMessage);
                        if (match is not null)
                        {
                            matches.Add(match);
                        }
                    },
                    shouldStop,
                    cancellationToken)
                .ConfigureAwait(false);
            scanned = scan.ScannedMessageCount;

            return new DeadLetterSearchSourceResult(
                target.Source,
                target.SubQueue,
                scanned,
                Array.AsReadOnly(matches.ToArray()),
                scan.SafetyLimitReached);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return new DeadLetterSearchSourceResult(
                target.Source,
                target.SubQueue,
                scanned,
                Array.AsReadOnly(matches.ToArray()),
                Error: $"Search was cancelled or timed out after inspecting {scanned:N0} messages in this source.");
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return new DeadLetterSearchSourceResult(
                target.Source,
                target.SubQueue,
                scanned,
                Array.AsReadOnly(matches.ToArray()),
                Error: exception.GetBaseException().Message);
        }
    }

    internal static async Task<SearchPageWalkResult> WalkSearchPagesAsync(
        int batchSize,
        int safetyLimit,
        Func<int, long?, CancellationToken, Task<IReadOnlyList<ServiceBusReceivedMessage>>> peekAsync,
        Action<ServiceBusReceivedMessage> visitMessage,
        Func<bool> shouldStop,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(safetyLimit, 1);
        ArgumentNullException.ThrowIfNull(peekAsync);
        ArgumentNullException.ThrowIfNull(visitMessage);
        ArgumentNullException.ThrowIfNull(shouldStop);

        var scanned = 0;
        var stopped = false;
        var exhausted = false;
        long? fromSequenceNumber = null;
        var seenSequenceNumbers = new HashSet<long>();

        while (scanned < safetyLimit)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (shouldStop())
            {
                stopped = true;
                break;
            }

            var requested = Math.Min(batchSize, safetyLimit - scanned);
            var messages = await peekAsync(requested, fromSequenceNumber, cancellationToken)
                .ConfigureAwait(false);
            if (messages.Count == 0)
            {
                exhausted = true;
                break;
            }
            if (messages.Count > requested)
            {
                throw new InvalidOperationException("Azure Service Bus returned more peeked messages than requested.");
            }

            var lastSequenceNumber = messages.Max(message => message.SequenceNumber);
            if (fromSequenceNumber.HasValue && lastSequenceNumber < fromSequenceNumber.Value)
            {
                throw new InvalidOperationException("Azure Service Bus peek pagination did not advance.");
            }

            foreach (var message in messages)
            {
                if (!seenSequenceNumbers.Add(message.SequenceNumber))
                {
                    continue;
                }

                visitMessage(message);
                scanned = checked(scanned + 1);
                if (shouldStop())
                {
                    stopped = true;
                    break;
                }
            }

            if (stopped)
            {
                break;
            }
            if (lastSequenceNumber == long.MaxValue)
            {
                exhausted = true;
                break;
            }

            fromSequenceNumber = lastSequenceNumber + 1;
        }

        var safetyLimitReached = false;
        if (!stopped && !exhausted && scanned >= safetyLimit)
        {
            var probe = await peekAsync(1, fromSequenceNumber, cancellationToken).ConfigureAwait(false);
            safetyLimitReached = probe.Count > 0;
        }

        return new SearchPageWalkResult(scanned, safetyLimitReached);
    }

    internal static int GetSearchSafetyLimit(
        DeadLetterSearchRequest request,
        DeadLetterSearchTarget target)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(target);

        // Runtime counters are eventually consistent. They are useful for progress text,
        // but a stale low count must never truncate a content search.
        return request.MaximumMessagesPerTarget;
    }

    internal readonly record struct SearchPageWalkResult(
        int ScannedMessageCount,
        bool SafetyLimitReached);

    private static bool MatchesSearch(ServiceBusReceivedMessage message, string query)
    {
        if (Contains(message.CorrelationId, query) ||
            Contains(message.MessageId, query) ||
            Contains(message.Subject, query) ||
            Contains(message.SessionId, query) ||
            Contains(message.ContentType, query) ||
            Contains(message.DeadLetterReason, query) ||
            Contains(message.DeadLetterErrorDescription, query))
        {
            return true;
        }

        foreach (var property in message.ApplicationProperties)
        {
            if (Contains(property.Key, query) ||
                Contains(Convert.ToString(property.Value, CultureInfo.InvariantCulture), query))
            {
                return true;
            }
        }

        var body = message.Body.ToMemory();
        var searchableLength = Math.Min(body.Length, AzureMessageMapper.MaxRetainedBodyBytes);
        return searchableLength > 0 && Encoding.UTF8.GetString(body.Span[..searchableLength])
            .Contains(query, StringComparison.OrdinalIgnoreCase);
    }

    private static bool Contains(string? value, string query) =>
        !string.IsNullOrEmpty(value) && value.Contains(query, StringComparison.OrdinalIgnoreCase);

    public async Task SendMessageAsync(
        SendMessageRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        EnsureWriteAllowed();

        var sender = _senders.GetOrAdd(
            request.Destination.Name,
            name => GetMessagingClient().CreateSender(name));
        var message = AzureMessageMapper.ToAzure(request.Message);

        using var batch = await sender.CreateMessageBatchAsync(cancellationToken).ConfigureAwait(false);
        if (!batch.TryAddMessage(message))
        {
            throw new InvalidOperationException(
                "The message is larger than the maximum batch/message size allowed by this Service Bus namespace.");
        }

        await sender.SendMessagesAsync(batch, cancellationToken).ConfigureAwait(false);
    }

    public Task ResubmitDeadLetterAsync(
        ResubmitDeadLetterRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Disposition != DeadLetterDisposition.KeepOriginal)
        {
            throw new NotSupportedException(
                "The safe MVP only resends a copy. Removing the original requires a bounded PeekLock repair workflow and is intentionally disabled.");
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
        var startedAt = _timeProvider.GetUtcNow();
        foreach (var source in request.Targets.Select(target => target.Source).Distinct())
        {
            // Validate the entire request before creating a backup session or deleting
            // from an earlier target. Mixed session/non-session scopes are all-or-none.
            await EnsureSessionlessMessageSourceAsync(source, cancellationToken).ConfigureAwait(false);
        }
        var backupSession = await _backupStore.CreateSessionAsync(profile, startedAt, cancellationToken)
            .ConfigureAwait(false);
        var results = new List<DeadLetterPurgeSourceResult>(request.Targets.Count);

        for (var index = 0; index < request.Targets.Count; index++)
        {
            var target = request.Targets[index];
            if (cancellationToken.IsCancellationRequested)
            {
                foreach (var pending in request.Targets.Skip(index))
                {
                    results.Add(new DeadLetterPurgeSourceResult(
                        pending.Source,
                        pending.SubQueue,
                        0,
                        "Cancelled before this source was processed."));
                }
                break;
            }
            results.Add(await PurgeSubQueueAsync(
                    target.Source,
                    target.SubQueue,
                    request.BatchSize,
                    request.MaximumMessagesPerSubQueue,
                    backupSession,
                    index + 1,
                    request.Targets.Count,
                    progress,
                    cancellationToken)
                .ConfigureAwait(false));
        }

        _cachedTopology = null;
        foreach (var result in results.Where(result => result.IsSuccessful))
        {
            _previousDeadLetterCounts[$"{result.Source.Path}|{result.SubQueue}"] = 0;
        }

        return new DeadLetterPurgeResult(
            profile.Id,
            startedAt,
            _timeProvider.GetUtcNow(),
            results,
            backupSession.RootDirectory);
    }

    private async Task<DeadLetterPurgeSourceResult> PurgeSubQueueAsync(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        int batchSize,
        int maximumMessages,
        DeadLetterJsonBackupSession backupSession,
        int targetNumber,
        int targetCount,
        IProgress<DeadLetterPurgeProgress>? progress,
        CancellationToken cancellationToken)
    {
        var options = new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            // Keep at most one small work batch locked ahead. This hides receive latency
            // without building a large lock-expiry or memory backlog while JSON is written.
            PrefetchCount = 0,
            SubQueue = subQueue switch
            {
                ServiceBusSubQueue.DeadLetter => SubQueue.DeadLetter,
                ServiceBusSubQueue.TransferDeadLetter => SubQueue.TransferDeadLetter,
                _ => throw new ArgumentOutOfRangeException(nameof(subQueue), subQueue, "Unsupported purge subqueue.")
            }
        };

        long deleted = 0;
        long backedUp = 0;
        var consecutiveEmptyReceives = 0;
        try
        {
            progress?.Report(new DeadLetterPurgeProgress(
                source, subQueue, targetNumber, targetCount, backedUp, deleted, DeadLetterPurgeStage.Starting));
            await using var receiver = source.Kind switch
            {
                ServiceBusEntityKind.Queue => GetMessagingClient().CreateReceiver(source.Name, options),
                ServiceBusEntityKind.Subscription => GetMessagingClient().CreateReceiver(
                    source.TopicName!,
                    source.Name,
                    options),
                _ => throw new ArgumentException(
                    "Only queues and subscriptions can have dead letters purged.",
                    nameof(source))
            };

            while (deleted < maximumMessages)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var remaining = maximumMessages - deleted;
                var receiveCount = (int)Math.Min(batchSize, remaining);
                var messages = await receiver.ReceiveMessagesAsync(
                        receiveCount,
                        PurgeReceiveWaitTime,
                        cancellationToken)
                    .ConfigureAwait(false);
                if (messages.Count == 0)
                {
                    consecutiveEmptyReceives++;
                    if (HasConfirmedEmptyPurge(consecutiveEmptyReceives))
                    {
                        progress?.Report(new DeadLetterPurgeProgress(
                            source, subQueue, targetNumber, targetCount, backedUp, deleted, DeadLetterPurgeStage.Verifying));
                        var remainingCount = await TryReadPurgeCountAsync(source, subQueue, cancellationToken)
                            .ConfigureAwait(false);
                        progress?.Report(new DeadLetterPurgeProgress(
                            source, subQueue, targetNumber, targetCount, backedUp, deleted, DeadLetterPurgeStage.Completed));
                        // Management-plane counters are eventually consistent. A nonzero
                        // or unavailable value after two empty receive polls is advisory,
                        // not proof that settlement failed. The UI prompts a later rescan.
                        return new DeadLetterPurgeSourceResult(
                            source,
                            subQueue,
                            deleted,
                            VerificationPending: remainingCount is null or > 0);
                    }
                    continue;
                }
                consecutiveEmptyReceives = 0;

                progress?.Report(new DeadLetterPurgeProgress(
                    source, subQueue, targetNumber, targetCount, backedUp, deleted, DeadLetterPurgeStage.BackingUp));
                foreach (var backupBatch in messages.Chunk(BackupWriteConcurrency))
                {
                    await Task.WhenAll(backupBatch.Select(message =>
                            backupSession.BackupAsync(message, source, subQueue, cancellationToken)))
                        .ConfigureAwait(false);
                }
                backedUp = checked(backedUp + messages.Count);

                progress?.Report(new DeadLetterPurgeProgress(
                    source, subQueue, targetNumber, targetCount, backedUp, deleted, DeadLetterPurgeStage.Deleting));
                var settlements = await Task.WhenAll(messages.Select(async message =>
                {
                    try
                    {
                        // Once every message in this batch has a durable backup, finish settlement
                        // independently from a UI cancellation. Cancellation is honored before the
                        // next batch, preventing an unknowable half-settled batch.
                        await receiver.CompleteMessageAsync(message, CancellationToken.None).ConfigureAwait(false);
                        return (Exception?)null;
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        return exception;
                    }
                })).ConfigureAwait(false);
                deleted = checked(deleted + settlements.Count(exception => exception is null));
                var settlementError = settlements.FirstOrDefault(exception => exception is not null);
                if (settlementError is not null)
                {
                    return new DeadLetterPurgeSourceResult(source, subQueue, deleted, settlementError.Message);
                }
            }

            return new DeadLetterPurgeSourceResult(
                source,
                subQueue,
                deleted,
                Error: $"Safety limit of {maximumMessages:N0} messages was reached.",
                LimitReached: true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return new DeadLetterPurgeSourceResult(
                source,
                subQueue,
                deleted,
                $"Cancelled safely after backing up {backedUp:N0} and deleting {deleted:N0} messages.");
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return new DeadLetterPurgeSourceResult(source, subQueue, deleted, exception.Message);
        }
    }

    internal static bool HasConfirmedEmptyPurge(int consecutiveEmptyReceives) =>
        consecutiveEmptyReceives >= PurgeEmptyReceiveConfirmations;

    private async Task<long?> TryReadPurgeCountAsync(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        CancellationToken cancellationToken)
    {
        using var verification = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        verification.CancelAfter(PurgeVerificationTimeout);
        try
        {
            var counts = await GetDeadLetterCountsAsync(source, verification.Token).ConfigureAwait(false);
            return counts.Single(item => item.SubQueue == subQueue).Count;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _ = exception;
            return null;
        }
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
        var sources = scope.Kind switch
        {
            DeadLetterMonitorScopeKind.SingleEntity => [scope.Entity!],
            DeadLetterMonitorScopeKind.AllMessageSources =>
                (await GetTopologyCoreAsync(forceRefresh: false, cancellationToken).ConfigureAwait(false))
                .MessageSources.ToArray(),
            _ => throw new ArgumentOutOfRangeException(nameof(scope), scope.Kind, "Unsupported monitor scope.")
        };

        using var limiter = new SemaphoreSlim(MonitorConcurrency, MonitorConcurrency);
        var tasks = sources.Select(async source =>
        {
            await limiter.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await GetDeadLetterCountsAsync(source, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                return new[]
                {
                    CreateSnapshot(source, ServiceBusSubQueue.DeadLetter, null, exception.Message),
                    CreateSnapshot(source, ServiceBusSubQueue.TransferDeadLetter, null, exception.Message)
                };
            }
            finally
            {
                limiter.Release();
            }
        });

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        return new DeadLetterSnapshot(profile.Id, _timeProvider.GetUtcNow(), results.SelectMany(items => items));
    }

    private async Task<DeadLetterEntitySnapshot[]> GetDeadLetterCountsAsync(
        ServiceBusEntityReference source,
        CancellationToken cancellationToken)
    {
        var administration = GetAdministrationClient();
        long deadLetters;
        long transferDeadLetters;

        if (source.Kind == ServiceBusEntityKind.Queue)
        {
            var response = await administration.GetQueueRuntimePropertiesAsync(source.Name, cancellationToken)
                .ConfigureAwait(false);
            deadLetters = response.Value.DeadLetterMessageCount;
            transferDeadLetters = response.Value.TransferDeadLetterMessageCount;
        }
        else if (source.Kind == ServiceBusEntityKind.Subscription)
        {
            var response = await administration.GetSubscriptionRuntimePropertiesAsync(
                    source.TopicName!,
                    source.Name,
                    cancellationToken)
                .ConfigureAwait(false);
            deadLetters = response.Value.DeadLetterMessageCount;
            transferDeadLetters = response.Value.TransferDeadLetterMessageCount;
        }
        else
        {
            throw new ArgumentException("DLQ counters only exist for queues and subscriptions.", nameof(source));
        }

        return
        [
            CreateSnapshot(source, ServiceBusSubQueue.DeadLetter, deadLetters, null),
            CreateSnapshot(source, ServiceBusSubQueue.TransferDeadLetter, transferDeadLetters, null)
        ];
    }

    private DeadLetterEntitySnapshot CreateSnapshot(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        long? count,
        string? error)
    {
        var key = $"{source.Path}|{subQueue}";
        long? previous = null;
        if (_previousDeadLetterCounts.TryGetValue(key, out var value))
        {
            previous = value;
        }
        if (count.HasValue)
        {
            _previousDeadLetterCounts[key] = count.Value;
        }

        return new DeadLetterEntitySnapshot(source, count, previous, error, subQueue);
    }

    private async Task EnsureSessionlessMessageSourceAsync(
        ServiceBusEntityReference source,
        CancellationToken cancellationToken)
    {
        var requiresSession = TryGetRequiresSession(_cachedTopology, source);
        if (!requiresSession.HasValue)
        {
            var administration = GetAdministrationClient();
            requiresSession = source.Kind switch
            {
                ServiceBusEntityKind.Queue =>
                    (await administration.GetQueueAsync(source.Name, cancellationToken).ConfigureAwait(false))
                    .Value.RequiresSession,
                ServiceBusEntityKind.Subscription =>
                    (await administration.GetSubscriptionAsync(
                            source.TopicName!,
                            source.Name,
                            cancellationToken)
                        .ConfigureAwait(false))
                    .Value.RequiresSession,
                _ => throw new ArgumentException(
                    "Only queues and subscriptions can be used as message sources.",
                    nameof(source))
            };
        }

        if (requiresSession.Value)
        {
            throw new NotSupportedException(SessionEnabledEntityError);
        }
    }

    internal static bool? TryGetRequiresSession(
        ServiceBusTopology? topology,
        ServiceBusEntityReference source)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (topology is null)
        {
            return null;
        }

        if (source.Kind == ServiceBusEntityKind.Queue)
        {
            return topology.Queues.FirstOrDefault(queue =>
                string.Equals(queue.Name, source.Name, StringComparison.OrdinalIgnoreCase))?.RequiresSession;
        }

        if (source.Kind == ServiceBusEntityKind.Subscription)
        {
            var topic = topology.Topics.FirstOrDefault(item =>
                string.Equals(item.Name, source.TopicName, StringComparison.OrdinalIgnoreCase));
            return topic?.Subscriptions.FirstOrDefault(subscription =>
                string.Equals(subscription.Name, source.Name, StringComparison.OrdinalIgnoreCase))?.RequiresSession;
        }

        return null;
    }

    private static async Task<ServiceBusTopic> MapTopicAsync(
        ServiceBusAdministrationClient administration,
        TopicProperties properties,
        TopicRuntimeProperties? runtime,
        CancellationToken cancellationToken)
    {
        var subscriptionPropertiesTask = ReadAllAsync(
            administration.GetSubscriptionsAsync(properties.Name, cancellationToken),
            cancellationToken);
        var subscriptionRuntimeTask = ReadAllAsync(
            administration.GetSubscriptionsRuntimePropertiesAsync(properties.Name, cancellationToken),
            cancellationToken);
        await Task.WhenAll(subscriptionPropertiesTask, subscriptionRuntimeTask).ConfigureAwait(false);

        var runtimeByName = subscriptionRuntimeTask.Result
            .ToDictionary(item => item.SubscriptionName, StringComparer.Ordinal);
        var subscriptions = subscriptionPropertiesTask.Result
            .Select(subscription => MapSubscription(
                subscription,
                runtimeByName.GetValueOrDefault(subscription.SubscriptionName)))
            .OrderBy(subscription => subscription.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var topicRuntime = runtime is null
            ? ServiceBusEntityRuntime.Empty
            : new ServiceBusEntityRuntime(
                new ServiceBusMessageCounts(scheduled: runtime.ScheduledMessageCount),
                runtime.SizeInBytes,
                runtime.CreatedAt,
                runtime.UpdatedAt,
                runtime.AccessedAt);

        return new ServiceBusTopic(
            properties.Name,
            topicRuntime,
            subscriptions,
            MapStatus(properties.Status.ToString()));
    }

    private static ServiceBusQueue MapQueue(QueueProperties properties, QueueRuntimeProperties? runtime) =>
        new(
            properties.Name,
            runtime is null ? ServiceBusEntityRuntime.Empty : MapRuntime(runtime),
            MapStatus(properties.Status.ToString()),
            properties.RequiresSession);

    private static ServiceBusSubscription MapSubscription(
        SubscriptionProperties properties,
        SubscriptionRuntimeProperties? runtime) =>
        new(
            properties.TopicName,
            properties.SubscriptionName,
            runtime is null ? ServiceBusEntityRuntime.Empty : MapRuntime(runtime),
            MapStatus(properties.Status.ToString()),
            properties.RequiresSession);

    private static ServiceBusEntityRuntime MapRuntime(QueueRuntimeProperties runtime) =>
        new(
            new ServiceBusMessageCounts(
                runtime.ActiveMessageCount,
                runtime.DeadLetterMessageCount,
                runtime.ScheduledMessageCount,
                runtime.TransferMessageCount,
                runtime.TransferDeadLetterMessageCount),
            runtime.SizeInBytes,
            runtime.CreatedAt,
            runtime.UpdatedAt,
            runtime.AccessedAt);

    private static ServiceBusEntityRuntime MapRuntime(SubscriptionRuntimeProperties runtime) =>
        new(
            new ServiceBusMessageCounts(
                runtime.ActiveMessageCount,
                runtime.DeadLetterMessageCount,
                scheduled: 0,
                runtime.TransferMessageCount,
                runtime.TransferDeadLetterMessageCount),
            sizeInBytes: 0,
            runtime.CreatedAt,
            runtime.UpdatedAt,
            runtime.AccessedAt);

    private static ServiceBusEntityStatus MapStatus(string status) => status switch
    {
        "Active" => ServiceBusEntityStatus.Active,
        "Disabled" => ServiceBusEntityStatus.Disabled,
        "SendDisabled" => ServiceBusEntityStatus.SendDisabled,
        "ReceiveDisabled" => ServiceBusEntityStatus.ReceiveDisabled,
        "Creating" => ServiceBusEntityStatus.Creating,
        "Deleting" => ServiceBusEntityStatus.Deleting,
        "Renaming" => ServiceBusEntityStatus.Renaming,
        "Restoring" => ServiceBusEntityStatus.Restoring,
        _ => ServiceBusEntityStatus.Unknown
    };

    private static async Task<List<T>> ReadAllAsync<T>(
        AsyncPageable<T> pageable,
        CancellationToken cancellationToken)
        where T : notnull
    {
        var items = new List<T>();
        await foreach (var item in pageable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            items.Add(item);
        }
        return items;
    }

    internal static ServiceBusClientOptions CreateMessagingClientOptions() => new()
    {
        TransportType = ServiceBusTransportType.AmqpTcp,
        EnableCrossEntityTransactions = false,
        RetryOptions = new ServiceBusRetryOptions
        {
            Mode = ServiceBusRetryMode.Exponential,
            MaxRetries = InteractiveMaximumRetries,
            Delay = InteractiveRetryDelay,
            MaxDelay = InteractiveMaximumRetryDelay,
            TryTimeout = InteractiveTryTimeout
        }
    };

    internal static ServiceBusAdministrationClientOptions CreateAdministrationClientOptions()
    {
        var options = new ServiceBusAdministrationClientOptions();
        options.Retry.Mode = RetryMode.Exponential;
        options.Retry.MaxRetries = InteractiveMaximumRetries;
        options.Retry.Delay = InteractiveRetryDelay;
        options.Retry.MaxDelay = InteractiveMaximumRetryDelay;
        options.Retry.NetworkTimeout = InteractiveTryTimeout;
        return options;
    }

    private void EnsureWriteAllowed()
    {
        var profile = GetConnectedProfile();
        if (!profile.CanWrite)
        {
            throw new InvalidOperationException(
                $"Environment '{profile.Name}' is read-only. Unlock write access before sending messages.");
        }
    }

    private ServiceBusProfile GetConnectedProfile() =>
        _profile ?? throw new InvalidOperationException("Connect to an environment first.");

    private ServiceBusClient GetMessagingClient() =>
        _client ?? throw new InvalidOperationException("Connect to an environment first.");

    private ServiceBusAdministrationClient GetAdministrationClient() =>
        _administration ?? throw new InvalidOperationException("Connect to an environment first.");

    private async Task DisposeClientsAsync()
    {
        foreach (var sender in _senders.Values)
        {
            await sender.DisposeAsync().ConfigureAwait(false);
        }
        _senders.Clear();

        if (_client is not null)
        {
            await _client.DisposeAsync().ConfigureAwait(false);
        }

        _client = null;
        _administration = null;
        _profile = null;
        _cachedTopology = null;
        _previousDeadLetterCounts.Clear();
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        using var lifecycle = await _operationGate.EnterLifecycleAsync().ConfigureAwait(false);
        if (_disposed)
        {
            return;
        }

        await DisposeClientsAsync().ConfigureAwait(false);
        _connectionState = WorkspaceConnectionState.Disconnected;
        _disposed = true;
        _topologyGate.Dispose();
    }
}
