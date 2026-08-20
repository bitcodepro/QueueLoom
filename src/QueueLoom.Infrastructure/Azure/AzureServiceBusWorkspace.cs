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
    private const int MonitorConcurrency = 6;
    private const int SearchConcurrency = 12;

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
                var clientOptions = new ServiceBusClientOptions
                {
                    TransportType = ServiceBusTransportType.AmqpTcp,
                    EnableCrossEntityTransactions = false
                };

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
                    administration = new ServiceBusAdministrationClient(connectionString);
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
                    administration = new ServiceBusAdministrationClient(profile.FullyQualifiedNamespace, credential);
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

        const int pageSize = 100;
        var messages = new List<BrowsedMessage>(request.MaxMessages);
        var fromSequenceNumber = request.FromSequenceNumber;
        while (messages.Count < request.MaxMessages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await receiver.PeekMessagesAsync(
                    Math.Min(pageSize, request.MaxMessages - messages.Count),
                    fromSequenceNumber,
                    cancellationToken)
                .ConfigureAwait(false);
            if (page.Count == 0)
            {
                break;
            }

            messages.AddRange(page.Select(message =>
                AzureMessageMapper.FromAzure(message, request.Source, request.SubQueue)));
            var lastSequenceNumber = page.Max(message => message.SequenceNumber);
            if (lastSequenceNumber == long.MaxValue ||
                (fromSequenceNumber.HasValue && lastSequenceNumber < fromSequenceNumber.Value))
            {
                break;
            }
            fromSequenceNumber = lastSequenceNumber + 1;
        }

        return Array.AsReadOnly(messages
            .OrderBy(message => message.EnqueuedAt ?? DateTimeOffset.MaxValue)
            .ThenBy(message => message.SequenceNumber)
            .ToArray());
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
        var tasks = request.Targets.Select(async target =>
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
        }).ToArray();
        var sourceResults = await Task.WhenAll(tasks).ConfigureAwait(false);

        return new DeadLetterSearchResult(
            profile.Id,
            startedAt,
            _timeProvider.GetUtcNow(),
            sourceResults,
            resultLimitReached != 0);
    }

    private async Task<DeadLetterSearchSourceResult> SearchTargetAsync(
        DeadLetterSearchTarget target,
        DeadLetterSearchRequest request,
        Func<ServiceBusReceivedMessage, BrowsedMessage?> addMatch,
        Func<bool> shouldStop,
        CancellationToken cancellationToken)
    {
        var matches = new List<BrowsedMessage>();
        var scanned = 0;
        var scanLimit = (int)Math.Min(target.KnownMessageCount, request.MaximumMessagesPerTarget);
        try
        {
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

            long? fromSequenceNumber = null;
            while (scanned < scanLimit)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (shouldStop())
                {
                    break;
                }
                var batchSize = Math.Min(request.BatchSize, scanLimit - scanned);
                var messages = await receiver.PeekMessagesAsync(
                        batchSize,
                        fromSequenceNumber,
                        cancellationToken)
                    .ConfigureAwait(false);
                if (messages.Count == 0)
                {
                    break;
                }

                scanned = checked(scanned + messages.Count);
                foreach (var azureMessage in messages)
                {
                    if (MatchesSearch(azureMessage, request.Query))
                    {
                        var match = addMatch(azureMessage);
                        if (match is not null)
                        {
                            matches.Add(match);
                        }
                    }
                }

                var lastSequenceNumber = messages.Max(message => message.SequenceNumber);
                if (lastSequenceNumber == long.MaxValue ||
                    (fromSequenceNumber.HasValue && lastSequenceNumber < fromSequenceNumber.Value))
                {
                    break;
                }
                fromSequenceNumber = lastSequenceNumber + 1;
            }

            return new DeadLetterSearchSourceResult(
                target.Source,
                target.SubQueue,
                scanned,
                Array.AsReadOnly(matches.ToArray()),
                target.KnownMessageCount > scanned);
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
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        EnsureWriteAllowed();

        var profile = GetConnectedProfile();
        var startedAt = _timeProvider.GetUtcNow();
        var backupSession = await _backupStore.CreateSessionAsync(profile, startedAt, cancellationToken)
            .ConfigureAwait(false);
        var results = new List<DeadLetterPurgeSourceResult>(request.Targets.Count);

        foreach (var target in request.Targets)
        {
            cancellationToken.ThrowIfCancellationRequested();
            results.Add(await PurgeSubQueueAsync(
                    target.Source,
                    target.SubQueue,
                    request.BatchSize,
                    request.MaximumMessagesPerSubQueue,
                    backupSession,
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
        CancellationToken cancellationToken)
    {
        var options = new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            PrefetchCount = 0,
            SubQueue = subQueue switch
            {
                ServiceBusSubQueue.DeadLetter => SubQueue.DeadLetter,
                ServiceBusSubQueue.TransferDeadLetter => SubQueue.TransferDeadLetter,
                _ => throw new ArgumentOutOfRangeException(nameof(subQueue), subQueue, "Unsupported purge subqueue.")
            }
        };

        long deleted = 0;
        try
        {
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
                        TimeSpan.FromMilliseconds(500),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (messages.Count == 0)
                {
                    return new DeadLetterPurgeSourceResult(source, subQueue, deleted);
                }

                foreach (var message in messages)
                {
                    await backupSession.BackupAsync(message, source, subQueue, cancellationToken)
                        .ConfigureAwait(false);
                }
                foreach (var message in messages)
                {
                    await receiver.CompleteMessageAsync(message, cancellationToken).ConfigureAwait(false);
                    deleted = checked(deleted + 1);
                }
            }

            return new DeadLetterPurgeSourceResult(
                source,
                subQueue,
                deleted,
                Error: $"Safety limit of {maximumMessages:N0} messages was reached.",
                LimitReached: true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return new DeadLetterPurgeSourceResult(source, subQueue, deleted, exception.Message);
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
