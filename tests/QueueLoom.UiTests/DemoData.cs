using System.Text;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

/// <summary>Deterministic, in-memory Service Bus data used to drive the real window.</summary>
internal static class DemoData
{
    public static readonly ServiceBusProfile Development = new(
        Guid.Parse("11111111-1111-1111-1111-111111111111"),
        "Local emulator",
        EnvironmentKind.Development,
        null,
        "localhost",
        AuthenticationSettings.ConnectionString(),
        ProfileAccessMode.ReadWrite);

    public static readonly ServiceBusProfile Production = new(
        Guid.Parse("22222222-2222-2222-2222-222222222222"),
        "Orders production",
        EnvironmentKind.Production,
        null,
        "orders-prod.servicebus.windows.net",
        AuthenticationSettings.Entra(),
        ProfileAccessMode.ReadOnly);

    public static readonly ServiceBusProfile AwsStaging = new ServiceBusProfile(
        Guid.Parse("33333333-3333-3333-3333-333333333333"),
        "Payments on AWS",
        EnvironmentKind.Test,
        null,
        null,
        new AuthenticationSettings(AuthenticationKind.AwsAccessKey),
        ProfileAccessMode.ReadWrite)
    {
        Provider = MessagingProvider.AmazonSqsSns,
        Aws = new AwsSettings("eu-central-1")
    };

    public static readonly ServiceBusProfile GoogleDevelopment = new ServiceBusProfile(
        Guid.Parse("44444444-4444-4444-4444-444444444444"),
        "Shipping on GCP",
        EnvironmentKind.Development,
        null,
        null,
        new AuthenticationSettings(AuthenticationKind.GoogleApplicationDefault),
        ProfileAccessMode.ReadWrite)
    {
        Provider = MessagingProvider.GooglePubSub,
        GooglePubSub = new GooglePubSubSettings("shipping-dev-2231")
    };

    public static readonly ServiceBusProfile RabbitStaging = new ServiceBusProfile(
        Guid.Parse("55555555-5555-5555-5555-555555555555"),
        "Billing on RabbitMQ",
        EnvironmentKind.Test,
        null,
        null,
        new AuthenticationSettings(AuthenticationKind.RabbitMqPassword),
        ProfileAccessMode.ReadWrite)
    {
        Provider = MessagingProvider.RabbitMq,
        RabbitMq = new RabbitMqSettings("rabbit.internal", "queueloom")
    };

    public static readonly ServiceBusProfile KafkaDevelopment = new ServiceBusProfile(
        Guid.Parse("66666666-6666-6666-6666-666666666666"),
        "Events on Kafka",
        EnvironmentKind.Development,
        null,
        null,
        new AuthenticationSettings(AuthenticationKind.KafkaNone),
        ProfileAccessMode.ReadWrite)
    {
        Provider = MessagingProvider.Kafka,
        Kafka = new KafkaSettings("kafka-1:9092,kafka-2:9092")
    };

    public static ServiceBusTopology Topology { get; } = new(
        new DateTimeOffset(2026, 9, 26, 9, 30, 0, TimeSpan.Zero),
        [
            new ServiceBusQueue("orders", Runtime(active: 1_284, deadLetter: 17, scheduled: 3), ServiceBusEntityStatus.Active),
            new ServiceBusQueue("payments", Runtime(active: 42, deadLetter: 0), ServiceBusEntityStatus.Active),
            new ServiceBusQueue("invoices-retry", Runtime(active: 0, deadLetter: 2_431, transferDeadLetter: 4), ServiceBusEntityStatus.Active)
        ],
        [
            new ServiceBusTopic(
                "customer-events",
                Runtime(),
                [
                    new ServiceBusSubscription("customer-events", "crm-sync", Runtime(active: 12, deadLetter: 5), ServiceBusEntityStatus.Active),
                    new ServiceBusSubscription("customer-events", "analytics", Runtime(active: 90_210, deadLetter: 0), ServiceBusEntityStatus.Active)
                ],
                ServiceBusEntityStatus.Active)
        ]);

    public static IReadOnlyList<BrowsedMessage> DeadLetters { get; } =
    [
        Message(ServiceBusEntityReference.Queue("orders"), 101, "order-1001", "corr-42",
            """{"orderId":1001,"customer":{"id":"C-77","tier":"gold"},"total":129.95,"paid":false,"notes":null}""",
            "MaxDeliveryCountExceeded", "Message could not be processed after 10 attempts."),
        Message(ServiceBusEntityReference.Queue("orders"), 102, "order-1002", "corr-43",
            """{"orderId":1002,"total":18.5,"paid":true}""",
            "SchemaValidationFailed", "Property 'currency' is required."),
        Message(ServiceBusEntityReference.Subscription("customer-events", "crm-sync"), 7, "evt-7", "corr-99",
            """{"event":"CustomerUpdated","id":"C-77"}""",
            "TTLExpiredException", "The message expired before it was delivered.")
    ];

    public static DeadLetterSnapshot Snapshot(Guid profileId) => new(
        profileId,
        new DateTimeOffset(2026, 9, 26, 9, 31, 0, TimeSpan.Zero),
        [
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 17, 12),
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("invoices-retry"), 2_431, 2_431),
            new DeadLetterEntitySnapshot(ServiceBusEntityReference.Subscription("customer-events", "crm-sync"), 5, 5)
        ]);

    /// <summary>A day of monitor checks: a failed deployment dead-letters invoices, then a fix slowly drains them.</summary>
    public static IEnumerable<DeadLetterHistorySample> History(ServiceBusProfile profile, DateTimeOffset now)
    {
        for (var step = 144; step >= 0; step--)
        {
            var at = now.AddMinutes(-10 * step);
            var hour = 24 - step / 6.0;
            var invoices = hour < 9 ? 1_880 + (long)(hour * 6)
                : hour < 11 ? 1_934 + (long)((hour - 9) * 380)
                : hour < 17 ? 2_694 - (long)((hour - 11) * 60)
                : 2_334 + (long)((hour - 17) * 14);
            var orders = step is 60 or 61 ? 140 : 12 + step % 7;
            var crm = hour > 20 ? 5 : 2;
            yield return new DeadLetterHistorySample(at, profile.Id, profile.Name, invoices + orders + crm,
                new Dictionary<string, long> { ["invoices-retry"] = invoices, ["orders"] = orders, ["customer-events/crm-sync"] = crm });
        }
    }

    public static IReadOnlyList<DeadLetterBackupSummary> Backups { get; } =
    [
        new(Path.Combine(Path.GetTempPath(), "queueloom-ui", "backup-101.json"), Development.Id, Development.Name,
            "Development", "localhost", ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter,
            101, "order-1001", "corr-42", "OrderPlaced",
            new DateTimeOffset(2026, 9, 25, 18, 4, 0, TimeSpan.Zero), new DateTimeOffset(2026, 9, 26, 8, 0, 0, TimeSpan.Zero), 96),
        new(Path.Combine(Path.GetTempPath(), "queueloom-ui", "backup-7.json"), Production.Id, Production.Name,
            "Production", "orders-prod.servicebus.windows.net", ServiceBusEntityReference.Subscription("customer-events", "crm-sync"),
            ServiceBusSubQueue.DeadLetter, 7, "evt-7", "corr-99", "CustomerUpdated",
            new DateTimeOffset(2026, 9, 24, 11, 15, 0, TimeSpan.Zero), new DateTimeOffset(2026, 9, 26, 8, 5, 0, TimeSpan.Zero), 40)
    ];

    private static ServiceBusEntityRuntime Runtime(
        long active = 0,
        long deadLetter = 0,
        long scheduled = 0,
        long transferDeadLetter = 0) =>
        new(new ServiceBusMessageCounts(active, deadLetter, scheduled, transferDeadLetter: transferDeadLetter));

    private static BrowsedMessage Message(
        ServiceBusEntityReference source,
        long sequence,
        string messageId,
        string correlationId,
        string body,
        string reason,
        string description) =>
        new(
            source,
            ServiceBusSubQueue.DeadLetter,
            sequence,
            Encoding.UTF8.GetBytes(body),
            new EditableMessageProperties(messageId, correlationId, "application/json", "OrderPlaced"),
            deliveryCount: 10,
            enqueuedAt: new DateTimeOffset(2026, 9, 25, 18, 4, 0, TimeSpan.Zero).AddMinutes(sequence),
            deadLetterReason: reason,
            deadLetterErrorDescription: description);
}

internal sealed class DemoWorkspace : IServiceBusWorkspace
{
    public WorkspaceConnectionState ConnectionState { get; private set; }

    public Guid? ConnectedProfileId { get; private set; }

    public Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
    {
        ConnectionState = WorkspaceConnectionState.Connected;
        ConnectedProfileId = profile.Id;
        return Task.CompletedTask;
    }

    public Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        ConnectionState = WorkspaceConnectionState.Disconnected;
        ConnectedProfileId = null;
        return Task.CompletedTask;
    }

    public Task SetAccessModeAsync(ProfileAccessMode accessMode, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    public Task<ServiceBusTopology> GetTopologyAsync(bool forceRefresh = false, CancellationToken cancellationToken = default) =>
        Task.FromResult(DemoData.Topology);

    public Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
        BrowseMessagesRequest request,
        CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<BrowsedMessage>>(DemoData.DeadLetters
            .Where(message => message.Source == request.Source && message.SequenceNumber >= (request.FromSequenceNumber ?? 0))
            .Take(request.MaxMessages)
            .ToArray());

    public Task<DeadLetterSearchResult> SearchDeadLettersAsync(
        DeadLetterSearchRequest request,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("Search is not part of the UI demo.");

    public Task SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    public Task ResubmitDeadLetterAsync(ResubmitDeadLetterRequest request, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    public Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(
        DeadLetterPurgeRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterPurgeProgress>? progress = null) =>
        throw new NotSupportedException("Purge is not part of the UI demo.");

    public Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(
        DeleteDeadLetterMessagesRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterMessageDeletionProgress>? progress = null) =>
        Task.FromResult(new DeleteDeadLetterMessagesResult(
            ConnectedProfileId ?? throw new InvalidOperationException("Not connected."),
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow,
            request.Messages.Select(key => new DeadLetterMessageDeletionResult(key, DeadLetterMessageDeletionOutcome.Deleted)),
            Path.Combine(Path.GetTempPath(), "queueloom-ui", "backups")));

    public Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(
        DeadLetterMonitorScope scope,
        CancellationToken cancellationToken = default) =>
        Task.FromResult(DemoData.Snapshot(ConnectedProfileId ?? throw new InvalidOperationException("Not connected.")));

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class InMemoryProfileRepository(params ServiceBusProfile[] profiles) : IProfileRepository
{
    private readonly List<ServiceBusProfile> _profiles = [.. profiles];
    private Guid? _selected = profiles.FirstOrDefault()?.Id;

    public Task<IReadOnlyList<ServiceBusProfile>> ListAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<ServiceBusProfile>>(_profiles.ToArray());

    public Task<ServiceBusProfile?> GetAsync(Guid profileId, CancellationToken cancellationToken = default) =>
        Task.FromResult(_profiles.FirstOrDefault(profile => profile.Id == profileId));

    public Task UpsertAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
    {
        _profiles.RemoveAll(item => item.Id == profile.Id);
        _profiles.Add(profile);
        return Task.CompletedTask;
    }

    public Task<bool> DeleteAsync(Guid profileId, CancellationToken cancellationToken = default) =>
        Task.FromResult(_profiles.RemoveAll(profile => profile.Id == profileId) > 0);

    public Task<Guid?> GetSelectedProfileIdAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(_selected);

    public Task SetSelectedProfileIdAsync(Guid? profileId, CancellationToken cancellationToken = default)
    {
        _selected = profileId;
        return Task.CompletedTask;
    }
}

internal sealed class InMemorySecretVault : ISecretVault
{
    private readonly Dictionary<ProfileSecretKey, string> _secrets = [];

    public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken cancellationToken = default)
    {
        _secrets[key] = secret;
        return ValueTask.CompletedTask;
    }

    public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(_secrets.GetValueOrDefault(key));

    public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(_secrets.ContainsKey(key));

    public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(_secrets.Remove(key));
}

internal sealed class InMemoryBackupRepository : IDeadLetterBackupRepository
{
    public string RootDirectory => Path.Combine(Path.GetTempPath(), "queueloom-ui", "backups");

    public Task<IReadOnlyList<DeadLetterBackupSummary>> ListAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult(DemoData.Backups);

    public Task<BrowsedMessage> LoadAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default) =>
        Task.FromResult(DemoData.DeadLetters.First(message => message.SequenceNumber == summary.SequenceNumber));

    public Task DeleteAsync(DeadLetterBackupSummary summary, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}
