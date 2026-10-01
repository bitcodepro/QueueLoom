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

    private readonly object _gate = new();

    public string FilePath => Path.Combine(paths.RootDirectory, "scheduled-resends.v2.json");

    public IReadOnlyList<ScheduledResend> Load()
    {
        lock (_gate)
        {
            // Move once, so older versions cannot execute schedules whose configuration or raw envelope
            // they do not understand. Legacy jobs have no configuration identity and remain blocked.
            var legacyPath = Path.Combine(paths.RootDirectory, "scheduled-resends.v1.json");
            if (!File.Exists(FilePath) && File.Exists(legacyPath)) File.Move(legacyPath, FilePath);
            try
            {
                return File.Exists(FilePath)
                    ? (JsonSerializer.Deserialize<List<ResendDocument>>(File.ReadAllText(FilePath), Options) ?? []).Select(ToModel).ToArray()
                    : [];
            }
            catch (JsonException)
            {
                // A damaged file is kept aside rather than overwritten, so nothing scheduled is silently lost.
                File.Move(FilePath, FilePath + $".damaged-{DateTime.UtcNow:yyyyMMddHHmmss}", overwrite: true);
                return [];
            }
        }
    }

    public void Save(IReadOnlyList<ScheduledResend> resends)
    {
        ArgumentNullException.ThrowIfNull(resends);
        lock (_gate)
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
    }

    private static ResendDocument ToDocument(ScheduledResend resend) => new(
        resend.Id, resend.ProfileId, resend.EnvironmentName, resend.CreatedAt, resend.DueAt, resend.Mode, resend.MessagesPerSecond,
        resend.DestinationDisplay,
        resend.Items.Select(item => new ItemDocument(item.Source, item.SubQueue, item.SequenceNumber, item.MessageId, item.Destination,
            item.Message.Body, item.Message.Properties, item.Message.ApplicationProperties.ToList())).ToList())
        { ConfigurationIdentity = resend.ConfigurationIdentity };

    private static ScheduledResend ToModel(ResendDocument document) => new(
        document.Id, document.ProfileId, document.EnvironmentName, document.CreatedAt, document.DueAt, document.Mode,
        document.MessagesPerSecond, document.DestinationDisplay,
        document.Items.Select(item => new ScheduledResendItem(item.Source, item.SubQueue, item.SequenceNumber, item.MessageId,
            item.Destination, new MessageDraft(item.Body, item.Properties, item.ApplicationProperties))).ToArray())
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
        List<MessageApplicationProperty> ApplicationProperties);
}
