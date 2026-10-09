using System.Globalization;
using System.Text.Json;
using System.Security.Cryptography;
using System.Text;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Persistence;

public sealed class JsonDeadLetterBackupRepository : IDeadLetterBackupRepository
{
    private readonly string _rootWithSeparator;
    private readonly StringComparison _pathComparison = OperatingSystem.IsWindows()
        ? StringComparison.OrdinalIgnoreCase
        : StringComparison.Ordinal;

    public JsonDeadLetterBackupRepository(QueueLoomPaths paths)
    {
        ArgumentNullException.ThrowIfNull(paths);
        RootDirectory = Path.GetFullPath(paths.BackupsDirectory);
        _rootWithSeparator = RootDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) +
                             Path.DirectorySeparatorChar;
    }

    public string RootDirectory { get; }
    // Next to the backups folder; a backups folder at a drive root has no parent, so the cache goes inside it
    // (it holds .cache files, which are never listed as backups).
    private string MetadataDirectory => Path.Combine(
        Path.GetDirectoryName(RootDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)) ?? RootDirectory,
        "backup-metadata-cache");

    private string CachePathFor(string path) =>
        Path.Combine(MetadataDirectory, Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(path))) + ".cache");
    private sealed record MetadataCache(long Length, long LastWriteTicks, DeadLetterBackupSummary? Summary);
    private static readonly JsonSerializerOptions CacheJsonOptions = new() { RespectRequiredConstructorParameters = true };

    public async Task<IReadOnlyList<DeadLetterBackupSummary>> ListAsync(
        CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(RootDirectory))
        {
            return [];
        }

        var summaries = new List<DeadLetterBackupSummary>();
        var listedCaches = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var enumerationOptions = new EnumerationOptions
        {
            RecurseSubdirectories = true,
            IgnoreInaccessible = true,
            AttributesToSkip = FileAttributes.ReparsePoint
        };
        foreach (var file in Directory.EnumerateFiles(RootDirectory, "*.json", enumerationOptions)
                     .Where(path => !string.Equals(Path.GetFileName(path), "session.json", StringComparison.OrdinalIgnoreCase)))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var path = ValidateMessagePath(file);
                var info = new FileInfo(path);
                var cachePath = CachePathFor(path);
                listedCaches.Add(cachePath);
                DeadLetterBackupSummary? summary = null;
                try
                {
                    if (File.Exists(cachePath) && new FileInfo(cachePath).Length <= 256 * 1024)
                    {
                        var cache = JsonSerializer.Deserialize<MetadataCache>(await File.ReadAllTextAsync(cachePath, cancellationToken), CacheJsonOptions);
                        if (cache?.Length == info.Length && cache.LastWriteTicks == info.LastWriteTimeUtc.Ticks &&
                            cache.Summary is { IsReadable: true, Source.CanBrowse: true, ProfileName: not null, Environment: not null, BodySize: >= 0 } cached &&
                            cached.FilePath == path && Enum.IsDefined(cached.SubQueue) &&
                            (cached.Source.Kind != ServiceBusEntityKind.Subscription || !string.IsNullOrWhiteSpace(cached.Source.TopicName)))
                            summary = cached;
                    }
                }
                catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or JsonException or ArgumentException)
                { /* Optional cache content is never evidence that the durable backup is unreadable. */ }
                if (summary is null)
                {
                    summary = await Task.Run(() =>
                    {
                        using var document = BackupMetadataReader.Read(path, cancellationToken);
                        return ParseSummary(path, document.RootElement);
                    }, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        await AtomicFile.WriteTextAsync(cachePath, JsonSerializer.Serialize(new MetadataCache(info.Length, info.LastWriteTimeUtc.Ticks, summary)), cancellationToken);
                    }
                    catch (Exception exception) when (exception is IOException or UnauthorizedAccessException) { /* A cache is never a durable backup. */ }
                }
                summaries.Add(summary);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                long length;
                DateTime lastWrite;
                try
                {
                    // Deleted by another window or by retention cleanup since it was listed: nothing to show.
                    var info = new FileInfo(file);
                    if (!info.Exists)
                    {
                        continue;
                    }
                    length = info.Length;
                    lastWrite = info.LastWriteTimeUtc;
                }
                catch (Exception infoException) when (infoException is IOException or UnauthorizedAccessException)
                {
                    continue;
                }
                summaries.Add(new DeadLetterBackupSummary(
                    Path.GetFullPath(file),
                    Guid.Empty,
                    "Unknown environment",
                    "UNKNOWN",
                    null,
                    ServiceBusEntityReference.Queue("unreadable-backup"),
                    ServiceBusSubQueue.DeadLetter,
                    0,
                    Path.GetFileNameWithoutExtension(file),
                    null,
                    null,
                    null,
                    lastWrite,
                    length,
                    $"Unreadable backup: {exception.GetBaseException().Message}"));
            }
        }

        RemoveOrphanCaches(listedCaches);
        return summaries
            .OrderByDescending(summary => summary.EnqueuedAt ?? summary.BackedUpAt)
            .ThenBy(summary => summary.Source.Path, StringComparer.OrdinalIgnoreCase)
            .ThenBy(summary => summary.SequenceNumber)
            .ToArray();
    }

    public async Task<BrowsedMessage> LoadAsync(
        DeadLetterBackupSummary summary,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(summary);
        if (!summary.IsReadable)
        {
            throw new InvalidDataException(summary.Error);
        }

        var path = ValidateMessagePath(summary.FilePath);
        if (new FileInfo(path).Length > 64L * 1024 * 1024)
            throw new InvalidDataException("This backup is too large for the body viewer (64 MiB file limit). The durable backup is retained; inspect it externally.");
        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            64 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return ParseMessage(document.RootElement);
    }

    public Task DeleteAsync(
        DeadLetterBackupSummary summary,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(summary);
        cancellationToken.ThrowIfCancellationRequested();
        var path = ValidateMessagePath(summary.FilePath);
        if (!File.Exists(path))
        {
            throw new FileNotFoundException("The backup message file no longer exists.", path);
        }

        File.Delete(path);
        // The cache repeats the message's metadata (ids, reasons), so it goes with the backup.
        TryDelete(CachePathFor(path));
        RemoveEmptyParentDirectories(Path.GetDirectoryName(path));
        return Task.CompletedTask;
    }

    public Task<int> RemoveFinishedEmptySessionsAsync(CancellationToken cancellationToken = default) =>
        Task.Run(() => RemoveFinishedEmptySessions(cancellationToken), cancellationToken);

    /// <summary>
    /// Deleting the last message of a session younger than <see cref="SessionQuietPeriod"/> keeps its folder (a purge
    /// may still be writing into it), and nothing else came back for it. This sweep, run at start-up and with retention,
    /// removes such folders once the session.json and every folder in the session have been quiet for the period.
    /// Any other file in the session (a message, a .tmp file being written, anything unknown) or a link keeps it.
    /// </summary>
    private int RemoveFinishedEmptySessions(CancellationToken cancellationToken)
    {
        if (!Directory.Exists(RootDirectory) || File.GetAttributes(RootDirectory).HasFlag(FileAttributes.ReparsePoint))
        {
            return 0;
        }

        var removed = 0;
        var sessions = Directory.EnumerateFiles(RootDirectory, "session.json", new EnumerationOptions
        {
            RecurseSubdirectories = true,
            IgnoreInaccessible = true,
            AttributesToSkip = FileAttributes.ReparsePoint,
            MatchCasing = MatchCasing.CaseInsensitive
        }).Select(Path.GetDirectoryName).OfType<string>().Distinct(StringComparer.Ordinal).ToArray();
        foreach (var session in sessions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (TryRemoveQuietEmptySession(session))
            {
                removed++;
                try
                {
                    RemoveEmptyParentDirectories(Path.GetDirectoryName(session));
                }
                catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
                {
                    // An empty date folder is only clutter; the next sweep tries again.
                }
            }
        }
        return removed;
    }

    private bool TryRemoveQuietEmptySession(string session)
    {
        try
        {
            var full = Path.GetFullPath(session);
            if (!full.StartsWith(_rootWithSeparator, _pathComparison))
            {
                return false;
            }
            EnsureNoReparsePointParents(full);
            var now = DateTime.UtcNow;
            var sessionFile = new FileInfo(Path.Combine(full, "session.json"));
            if (!sessionFile.Exists || sessionFile.Attributes.HasFlag(FileAttributes.ReparsePoint) ||
                now - sessionFile.LastWriteTimeUtc < SessionQuietPeriod)
            {
                return false;
            }

            // Hidden files count too: a .tmp file is how a backup is being written.
            var everything = new EnumerationOptions { RecurseSubdirectories = true, IgnoreInaccessible = false, AttributesToSkip = 0 };
            var folders = new List<DirectoryInfo> { new(full) };
            foreach (var entry in new DirectoryInfo(full).EnumerateFileSystemInfos("*", everything))
            {
                if (entry.Attributes.HasFlag(FileAttributes.ReparsePoint))
                {
                    return false;
                }
                if (entry is DirectoryInfo folder)
                {
                    folders.Add(folder);
                }
                else if (!string.Equals(entry.FullName, sessionFile.FullName, _pathComparison))
                {
                    return false;
                }
            }
            if (folders.Any(folder => now - folder.LastWriteTimeUtc < SessionQuietPeriod))
            {
                return false;
            }

            // Empty entity folders deepest first, then session.json, then the session folder, and never recursively:
            // anything written meanwhile makes a delete fail and keeps the rest.
            foreach (var folder in folders.Skip(1).OrderByDescending(folder => folder.FullName.Length))
            {
                folder.Delete();
            }
            sessionFile.Delete();
            Directory.Delete(full);
            return true;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or InvalidOperationException)
        {
            // Left in place; the next sweep tries again.
            return false;
        }
    }

    /// <summary>Removes cache entries of backups that are gone, for example deleted by hand or by another version.</summary>
    private void RemoveOrphanCaches(HashSet<string> listedCaches)
    {
        try
        {
            if (!Directory.Exists(MetadataDirectory))
            {
                return;
            }
            foreach (var cache in Directory.EnumerateFiles(MetadataDirectory, "*.cache"))
            {
                if (!listedCaches.Contains(cache))
                {
                    TryDelete(cache);
                }
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // A cache is never a durable backup; it is pruned again on the next list.
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // A cache is never a durable backup.
        }
    }

    private DeadLetterBackupSummary ParseSummary(string file, JsonElement root)
    {
        EnsureSchema(root);
        var source = ParseSource(root);
        return new DeadLetterBackupSummary(
            Path.GetFullPath(file),
            ReadGuid(root, "profileId"),
            ReadRequiredString(root, "profileName"),
            ReadRequiredString(root, "environment"),
            ReadOptionalString(root, "fullyQualifiedNamespace"),
            source,
            ReadEnum<ServiceBusSubQueue>(root, "subQueue"),
            ReadInt64(root, "sequenceNumber"),
            ReadOptionalString(root, "messageId"),
            ReadOptionalString(root, "correlationId"),
            ReadOptionalString(root, "subject"),
            ReadOptionalDateTimeOffset(root, "enqueuedTimeUtc"),
            ReadRequiredDateTimeOffset(root, "backedUpAtUtc"),
            ReadInt64(root, "bodySize"));
    }

    private static BrowsedMessage ParseMessage(JsonElement root)
    {
        EnsureSchema(root);
        var source = ParseSource(root);
        var properties = new EditableMessageProperties(
            ReadOptionalString(root, "messageId"),
            ReadOptionalString(root, "correlationId"),
            ReadOptionalString(root, "contentType"),
            ReadOptionalString(root, "subject"),
            ReadOptionalString(root, "to"),
            ReadOptionalString(root, "replyTo"),
            ReadOptionalString(root, "sessionId"),
            ReadOptionalString(root, "replyToSessionId"),
            ReadOptionalString(root, "partitionKey"),
            ReadOptionalString(root, "transactionPartitionKey"),
            // Azure backups store the SDK's defaults as values: TimeSpan.MaxValue for "no TTL" and 0001-01-01 for "not
            // scheduled". A live browse shows both as absent, so a restored message must too; otherwise a resend
            // carries a TTL no editor or other broker can take and a schedule in year 1.
            ReadOptionalTimeSpan(root, "timeToLive") is { } ttl && ttl != TimeSpan.MaxValue ? ttl : null,
            ReadOptionalDateTimeOffset(root, "scheduledEnqueueTimeUtc") is { } scheduled && scheduled != default ? scheduled : null,
            ReadOptionalString(root, "amqpType"),
            ReadOptionalString(root, "amqpAppId"),
            ReadOptionalString(root, "amqpContentEncoding"),
            root.TryGetProperty("amqpPriority", out var priority) && priority.ValueKind == JsonValueKind.Number &&
            priority.TryGetByte(out var amqpPriority) ? amqpPriority : null) { NativeSubject = ReadOptionalString(root, "nativeSubject") };
        var applicationProperties = root.TryGetProperty("applicationProperties", out var values) &&
                                    values.ValueKind == JsonValueKind.Array
            ? values.EnumerateArray().Select(value => new MessageApplicationProperty(
                ReadRequiredString(value, "name"),
                ReadEnum<ApplicationPropertyType>(value, "type"),
                ReadRequiredString(value, "value")) { WireType = ReadOptionalString(value, "wireType") }).ToArray()
            : [];
        if (root.GetProperty("schemaVersion").GetInt32() < 3 && ReadOptionalString(root, "provider") == "RabbitMq")
        {
            // Legacy files appended basic metadata after user headers. Preserve their old single-value
            // interpretation; when both were saved, retain the earlier header independently.
            string? Extract(string name)
            {
                var last = Array.FindLastIndex(applicationProperties, p => p.Name == name);
                if (last < 0) return null;
                var value = applicationProperties[last].Value;
                applicationProperties = applicationProperties.Where((_, index) => index != last).ToArray();
                return value;
            }
            properties = properties with { AmqpType = Extract("amqp-type"), AmqpAppId = Extract("amqp-app-id") };
        }
        var body = root.GetProperty("bodyBase64").GetBytesFromBase64();
        if (body.LongLength != ReadInt64(root, "bodySize")) throw new InvalidDataException("Backup body size does not match the actual data.");

        return new BrowsedMessage(
            source,
            ReadEnum<ServiceBusSubQueue>(root, "subQueue"),
            ReadInt64(root, "sequenceNumber"),
            body,
            properties,
            applicationProperties,
            ReadOptionalEnum(root, "state", ServiceBusMessageState.Unknown),
            ReadOptionalInt64(root, "enqueuedSequenceNumber"),
            (int)ReadOptionalInt64(root, "deliveryCount"),
            ReadOptionalDateTimeOffset(root, "enqueuedTimeUtc"),
            ReadOptionalDateTimeOffset(root, "expiresAtUtc"),
            deadLetterReason: ReadOptionalString(root, "deadLetterReason"),
            deadLetterErrorDescription: ReadOptionalString(root, "deadLetterErrorDescription"),
            originalBodySize: ReadInt64(root, "bodySize"))
        {
            KafkaEnvelope = root.TryGetProperty("kafkaEnvelope", out var envelope) ? envelope.Deserialize<KafkaEnvelope>() : null,
            // Older backups carry no queue ownership evidence; their counters may be producer-owned.
            BrokerOwnedHeaders = root.TryGetProperty("brokerOwnedHeaders", out var owned) && owned.ValueKind == JsonValueKind.Array
                ? owned.EnumerateArray().Select(name => name.GetString() ?? throw new InvalidDataException("Invalid broker header name."))
                    .ToHashSet(StringComparer.Ordinal)
                : new HashSet<string>(StringComparer.Ordinal)
        };
    }

    private string ValidateMessagePath(string path)
    {
        var fullPath = Path.GetFullPath(path);
        if (!fullPath.StartsWith(_rootWithSeparator, _pathComparison) ||
            !string.Equals(Path.GetExtension(fullPath), ".json", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(Path.GetFileName(fullPath), "session.json", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("The selected file is not a backup message inside the QueueLoom backup directory.");
        }
        EnsureNoReparsePointParents(fullPath);
        return fullPath;
    }

    private void EnsureNoReparsePointParents(string fullPath)
    {
        var relative = Path.GetRelativePath(RootDirectory, fullPath);
        var segments = relative.Split(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
            StringSplitOptions.RemoveEmptyEntries);
        var current = RootDirectory;
        if (File.GetAttributes(current).HasFlag(FileAttributes.ReparsePoint))
            throw new InvalidOperationException("Linked backup roots cannot be opened or deleted.");
        foreach (var segment in segments)
        {
            current = Path.Combine(current, segment);
            if ((Directory.Exists(current) || File.Exists(current)) &&
                File.GetAttributes(current).HasFlag(FileAttributes.ReparsePoint))
            {
                throw new InvalidOperationException("Backup files reached through links or junctions cannot be opened or deleted.");
            }
        }
    }

    private void RemoveEmptyParentDirectories(string? directory)
    {
        while (!string.IsNullOrWhiteSpace(directory) &&
               directory.StartsWith(_rootWithSeparator, _pathComparison) &&
               !string.Equals(directory, RootDirectory, _pathComparison))
        {
            if (!Directory.EnumerateFileSystemEntries(directory).Any())
            {
                Directory.Delete(directory);
            }
            else if (!TryRemoveFinishedEmptySession(directory))
            {
                return;
            }
            directory = Path.GetDirectoryName(directory);
        }
    }

    /// <summary>How long a session must have been quiet before its folder may go: a purge writes its files as it goes.</summary>
    private static readonly TimeSpan SessionQuietPeriod = TimeSpan.FromHours(1);

    /// <summary>
    /// A backup session folder whose last message file is gone keeps only its session.json; it goes too, unless the
    /// session started so recently that a purge may still be writing into it.
    /// </summary>
    private static bool TryRemoveFinishedEmptySession(string directory)
    {
        try
        {
            var entries = Directory.EnumerateFileSystemEntries(directory).Take(2).ToArray();
            if (entries.Length != 1 ||
                !string.Equals(Path.GetFileName(entries[0]), "session.json", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
            var session = new FileInfo(entries[0]);
            if (!session.Exists || session.Attributes.HasFlag(FileAttributes.ReparsePoint) ||
                DateTime.UtcNow - session.LastWriteTimeUtc < SessionQuietPeriod)
            {
                return false;
            }
            session.Delete();
            Directory.Delete(directory);
            return true;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // The message file is already gone; a leftover session folder is only clutter, so the delete still succeeds.
            return false;
        }
    }

    private static void EnsureSchema(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty("schemaVersion", out var schema) ||
            schema.GetInt32() is not (1 or 2 or 3))
        {
            throw new InvalidDataException("Unsupported backup JSON schema.");
        }
    }

    private static ServiceBusEntityReference ParseSource(JsonElement root)
    {
        var kind = ReadEnum<ServiceBusEntityKind>(root, "sourceKind");
        // Current backups carry exact components; rendered paths may contain the delimiter themselves.
        if (root.TryGetProperty("sourceName", out _) || root.TryGetProperty("topicName", out _))
        {
            var name = ReadRequiredString(root, "sourceName");
            return kind switch
            {
                ServiceBusEntityKind.Queue => ServiceBusEntityReference.Queue(name),
                ServiceBusEntityKind.Subscription => ServiceBusEntityReference.Subscription(ReadRequiredString(root, "topicName"), name),
                _ => throw new InvalidDataException("A backup source must be a queue or subscription.")
            };
        }
        // Legacy backups did not store components separately.
        var path = ReadRequiredString(root, "sourcePath");
        if (kind == ServiceBusEntityKind.Queue)
        {
            return ServiceBusEntityReference.Queue(path);
        }
        if (kind != ServiceBusEntityKind.Subscription)
        {
            throw new InvalidDataException("A backup source must be a queue or subscription.");
        }

        var separator = "/Subscriptions/";
        var index = path.IndexOf(separator, StringComparison.OrdinalIgnoreCase);
        if (index <= 0 || index + separator.Length >= path.Length)
        {
            throw new InvalidDataException("The backup subscription path is invalid.");
        }
        return ServiceBusEntityReference.Subscription(
            path[..index],
            path[(index + separator.Length)..]);
    }

    private static string ReadRequiredString(JsonElement root, string name) =>
        ReadOptionalString(root, name) ?? throw new InvalidDataException($"Backup property '{name}' is missing.");

    private static string? ReadOptionalString(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    private static long ReadInt64(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.TryGetInt64(out var result)
            ? result
            : throw new InvalidDataException($"Backup property '{name}' is missing or invalid.");

    private static long ReadOptionalInt64(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.TryGetInt64(out var result) ? result : 0;

    private static Guid ReadGuid(JsonElement root, string name) =>
        Guid.TryParse(ReadOptionalString(root, name), out var value)
            ? value
            : throw new InvalidDataException($"Backup property '{name}' is missing or invalid.");

    private static T ReadEnum<T>(JsonElement root, string name) where T : struct, Enum =>
        Enum.TryParse<T>(ReadOptionalString(root, name), ignoreCase: true, out var value)
            ? value
            : throw new InvalidDataException($"Backup property '{name}' is missing or invalid.");

    private static T ReadOptionalEnum<T>(JsonElement root, string name, T fallback) where T : struct, Enum =>
        Enum.TryParse<T>(ReadOptionalString(root, name), ignoreCase: true, out var value) ? value : fallback;

    private static DateTimeOffset ReadRequiredDateTimeOffset(JsonElement root, string name) =>
        ReadOptionalDateTimeOffset(root, name) ??
        throw new InvalidDataException($"Backup property '{name}' is missing or invalid.");

    private static DateTimeOffset? ReadOptionalDateTimeOffset(JsonElement root, string name) =>
        DateTimeOffset.TryParse(
            ReadOptionalString(root, name),
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind,
            out var value)
            ? value
            : null;

    private static TimeSpan? ReadOptionalTimeSpan(JsonElement root, string name) =>
        TimeSpan.TryParse(ReadOptionalString(root, name), CultureInfo.InvariantCulture, out var value)
            ? value
            : null;
}
