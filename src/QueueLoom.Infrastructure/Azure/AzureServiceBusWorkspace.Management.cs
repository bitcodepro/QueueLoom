using Azure;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Azure;

/// <summary>Creating, changing and deleting queues through the administration API.</summary>
public sealed partial class AzureServiceBusWorkspace
{
    private const QueueSettingFlags AzureSettings = QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount |
                                                    QueueSettingFlags.LockDuration | QueueSettingFlags.DeadLetterOnExpiration;

    // CreateQueueOptions: LockDuration "Max value is 5 minutes" (and must be positive); MaxDeliveryCount "Minimum value is 1".
    internal static readonly QueueSettingLimits AzureLimits = new("Azure Service Bus")
    {
        MinDeliveryCount = 1,
        MaxLock = TimeSpan.FromMinutes(5),
        LockMustBePositive = true
    };

    public QueueManagementCapabilities? QueueManagement => new("queue", AzureSettings, AzureSettings, CanCreateDeadLetterQueue: false)
    {
        Limits = AzureLimits
    };

    public async Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        var properties = (await GetAdministrationClient().GetQueueAsync(queue, cancellationToken).ConfigureAwait(false)).Value;
        return new QueueSettings(
            properties.DefaultMessageTimeToLive == TimeSpan.MaxValue ? null : properties.DefaultMessageTimeToLive,
            properties.MaxDeliveryCount,
            properties.LockDuration,
            properties.DeadLetteringOnMessageExpiration);
    }

    public async Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ThrowIfDisposed();
        GetConnectedProfile().EnsureQueueManagementAllowed();
        RefuseOutOfRange(definition.Settings);
        var options = new CreateQueueOptions(definition.Name);
        Apply(definition.Settings, options);
        await Administer(() => GetAdministrationClient().CreateQueueAsync(options, cancellationToken)).ConfigureAwait(false);
        _cachedTopology = null;
    }

    public async Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ThrowIfDisposed();
        GetConnectedProfile().EnsureQueueManagementAllowed();
        RefuseOutOfRange(settings);
        var administration = GetAdministrationClient();
        var properties = (await administration.GetQueueAsync(queue, cancellationToken).ConfigureAwait(false)).Value;
        if (settings.MessageTimeToLive is { } timeToLive)
        {
            properties.DefaultMessageTimeToLive = timeToLive;
        }
        if (settings.MaxDeliveryCount is { } maxDeliveryCount)
        {
            properties.MaxDeliveryCount = maxDeliveryCount;
        }
        if (settings.LockDuration is { } lockDuration)
        {
            properties.LockDuration = lockDuration;
        }
        if (settings.DeadLetterOnExpiration is { } deadLetter)
        {
            properties.DeadLetteringOnMessageExpiration = deadLetter;
        }
        await Administer(() => administration.UpdateQueueAsync(properties, cancellationToken)).ConfigureAwait(false);
        _cachedTopology = null;
    }

    public async Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        GetConnectedProfile().EnsureQueueManagementAllowed();
        await Administer(() => GetAdministrationClient().DeleteQueueAsync(queue, cancellationToken)).ConfigureAwait(false);
        _cachedTopology = null;
    }

    private static void RefuseOutOfRange(QueueSettings settings)
    {
        if (AzureLimits.Check(settings) is { } error)
        {
            throw new InvalidOperationException(error);
        }
    }

    private static void Apply(QueueSettings settings, CreateQueueOptions options)
    {
        if (settings.MessageTimeToLive is { } timeToLive)
        {
            options.DefaultMessageTimeToLive = timeToLive;
        }
        if (settings.MaxDeliveryCount is { } maxDeliveryCount)
        {
            options.MaxDeliveryCount = maxDeliveryCount;
        }
        if (settings.LockDuration is { } lockDuration)
        {
            options.LockDuration = lockDuration;
        }
        if (settings.DeadLetterOnExpiration is { } deadLetter)
        {
            options.DeadLetteringOnMessageExpiration = deadLetter;
        }
    }

    private static async Task Administer(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (RequestFailedException exception) when (exception.Status is 401 or 403)
        {
            throw new InvalidOperationException(
                "Managing queues needs the Azure Service Bus Data Owner role (or a Manage connection string).", exception);
        }
        catch (RequestFailedException exception) when (exception.Status == 409)
        {
            throw new InvalidOperationException("A queue or topic with this name already exists.", exception);
        }
    }
}
