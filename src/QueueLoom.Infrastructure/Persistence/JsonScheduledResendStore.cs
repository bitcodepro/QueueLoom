using System.Text.Json;
using System.Text.Json.Serialization;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Persistence;

/// <summary>
/// Scheduled resends in "scheduled-resends.v2.json" in the data folder, so they survive a restart. The file holds
/// message bodies, like backups do, and is readable by the current user only.
/// </summary>
public sealed class JsonScheduledResendStore(QueueLoomPaths paths) : IScheduledResendStore
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    // Serializes this window's own reads and writes; held across an await, so it is a semaphore, not a lock.
    private readonly SemaphoreSlim _gate = new(1, 1);

    public string FilePath => Path.Combine(paths.RootDirectory, "scheduled-resends.v2.json");

    public IReadOnlyList<ScheduledResend> Load()
    {
        _gate.Wait();
        try
        {
            using var ownership = OwnFile();
            return LoadCore();
        }
        finally { _gate.Release(); }
    }

    // The cross-process lock is awaited, and the file work runs on the thread pool, so a window's thread is never held
    // while another window owns the list.
    public Task<IReadOnlyList<ScheduledResend>> LoadAsync(CancellationToken cancellationToken = default) =>
        OwnedAsync(LoadCore, cancellationToken);

    public Task AddAsync(ScheduledResend resend, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(resend);
        return OwnedAsync(() => { AddCore(resend); return true; }, cancellationToken);
    }

    public Task<bool> TryRemoveAsync(ScheduledResend expected, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(expected);
        return OwnedAsync(() => TryRemoveCore(expected), cancellationToken);
    }

    private Task<T> OwnedAsync<T>(Func<T> work, CancellationToken cancellationToken) => Task.Run(async () =>
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var ownership = await CrossProcessFileLock.AcquireAsync(FilePath + ".lock", cancellationToken).ConfigureAwait(false);
            return work();
        }
        finally { _gate.Release(); }
    }, cancellationToken);

    private IReadOnlyList<ScheduledResend> LoadCore()
    {
        // Move once, so older versions cannot execute schedules whose configuration or raw envelope
        // they do not understand. Legacy jobs have no configuration identity and remain blocked.
        var legacyPath = Path.Combine(paths.RootDirectory, "scheduled-resends.v1.json");
        if (!File.Exists(FilePath) && File.Exists(legacyPath)) File.Move(legacyPath, FilePath);
        try
        {
            return File.Exists(FilePath)
                ? (JsonSerializer.Deserialize<List<ResendDocument?>>(File.ReadAllText(FilePath), Options) ?? []).Select(ToModel).ToArray()
                : [];
        }
        catch (Exception exception) when (exception is JsonException or InvalidDataException or ArgumentException)
        {
            // A damaged file is kept aside rather than overwritten, so nothing scheduled is silently lost, and the
            // app still starts.
            var aside = FilePath + $".damaged-{DateTime.UtcNow:yyyyMMddHHmmssfff}-{Guid.NewGuid():N}";
            File.Move(FilePath, aside);
            _setAside = aside;
            return [];
        }
    }

    private string? _setAside;

    public string? TakeSetAsideFile() => Interlocked.Exchange(ref _setAside, null);

    public void Save(IReadOnlyList<ScheduledResend> resends)
    {
        ArgumentNullException.ThrowIfNull(resends);
        _gate.Wait();
        try
        {
            using var ownership = OwnFile();
            SaveCore(resends);
        }
        finally { _gate.Release(); }
    }

    public void Add(ScheduledResend resend)
    {
        ArgumentNullException.ThrowIfNull(resend);
        _gate.Wait();
        try
        {
            using var ownership = OwnFile();
            AddCore(resend);
        }
        finally { _gate.Release(); }
    }

    private void AddCore(ScheduledResend resend)
    {
        var current = LoadCore();
        if (current.Count >= ScheduledResend.MaximumPending || current.Any(item => item.Id == resend.Id))
            throw new InvalidOperationException("The scheduled list is full or this job already exists. Refresh the pending jobs.");
        SaveCore(current.Append(resend).ToArray());
    }

    public bool TryRemove(ScheduledResend expected)
    {
        ArgumentNullException.ThrowIfNull(expected);
        _gate.Wait();
        try
        {
            using var ownership = OwnFile();
            return TryRemoveCore(expected);
        }
        finally { _gate.Release(); }
    }

    private bool TryRemoveCore(ScheduledResend expected)
    {
        var current = LoadCore();
        var job = current.FirstOrDefault(item => item.Id == expected.Id);
        if (job is null || JsonSerializer.Serialize(ToDocument(job), Options) != JsonSerializer.Serialize(ToDocument(expected), Options))
            return false;
        SaveCore(current.Where(item => item.Id != expected.Id).ToArray());
        return true;
    }

    private CrossProcessFileLock OwnFile() =>
        CrossProcessFileLock.AcquireAsync(FilePath + ".lock", CancellationToken.None).GetAwaiter().GetResult();

    private void SaveCore(IReadOnlyList<ScheduledResend> resends)
    {
        paths.EnsureCreated();
        if (resends.Count == 0)
        {
            File.Delete(FilePath);
            return;
        }
        var document = JsonSerializer.Serialize(resends.Select(ToDocument).ToList(), Options);
        AtomicFile.WriteTextAsync(FilePath, document, CancellationToken.None).GetAwaiter().GetResult();
    }

    private static ResendDocument ToDocument(ScheduledResend resend) => new(
        resend.Id, resend.ProfileId, resend.EnvironmentName, resend.CreatedAt, resend.DueAt, resend.Mode, resend.MessagesPerSecond,
        resend.DestinationDisplay,
        resend.Items.Select(item => new ItemDocument(item.Source, item.SubQueue, item.SequenceNumber, item.MessageId, item.Destination,
            item.Message.Body, item.Message.Properties, item.Message.ApplicationProperties.ToList())
            { KafkaEnvelope = item.Message.KafkaEnvelope, HasSeparatedAmqpMetadata = !item.Message.LegacyAmqpMetadata }).ToList())
        { ConfigurationIdentity = resend.ConfigurationIdentity };

    private static ScheduledResend ToModel(ResendDocument? document)
    {
        // Valid JSON can still miss parts a job needs, for example after a hand edit or a partial write.
        if (document is null || document.EnvironmentName is null || document.DestinationDisplay is null || document.Items is null ||
            document.Items.Any(item => item is null || item.Source is null || item.Destination is null || item.Body is null))
        {
            throw new InvalidDataException("A scheduled resend is incomplete.");
        }
        return ToModelCore(document);
    }

    private static ScheduledResend ToModelCore(ResendDocument document) => new(
        document.Id, document.ProfileId, document.EnvironmentName, document.CreatedAt, document.DueAt, document.Mode,
        document.MessagesPerSecond, document.DestinationDisplay,
        document.Items.Select(item => new ScheduledResendItem(item.Source, item.SubQueue, item.SequenceNumber, item.MessageId,
            item.Destination, new MessageDraft(item.Body, item.Properties, item.ApplicationProperties?.Where(property => property is not null))
            { KafkaEnvelope = item.KafkaEnvelope, LegacyAmqpMetadata = !item.HasSeparatedAmqpMetadata })).ToArray())
        { ConfigurationIdentity = document.ConfigurationIdentity };

    private sealed record ResendDocument(
        Guid Id,
        Guid ProfileId,
        string EnvironmentName,
        DateTimeOffset CreatedAt,
        DateTimeOffset DueAt,
        ResendMode Mode,
        int MessagesPerSecond,
        string DestinationDisplay,
        List<ItemDocument> Items)
    {
        public string? ConfigurationIdentity { get; init; }
    }

    private sealed record ItemDocument(
        ServiceBusEntityReference Source,
        ServiceBusSubQueue SubQueue,
        long SequenceNumber,
        string? MessageId,
        ServiceBusEntityReference Destination,
        EditableMessageBody Body,
        EditableMessageProperties Properties,
        List<MessageApplicationProperty> ApplicationProperties)
    {
        public KafkaEnvelope? KafkaEnvelope { get; init; }
        public bool HasSeparatedAmqpMetadata { get; init; }
    }
}
