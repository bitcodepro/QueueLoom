using System.Globalization;
using System.Text.Json;
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

    public async Task<IReadOnlyList<DeadLetterBackupSummary>> ListAsync(
        CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(RootDirectory))
        {
            return [];
        }

        var summaries = new List<DeadLetterBackupSummary>();
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
                await using var stream = new FileStream(
                    file,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    64 * 1024,
                    FileOptions.Asynchronous | FileOptions.SequentialScan);
                using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                summaries.Add(ParseSummary(file, document.RootElement));
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
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
                    File.GetLastWriteTimeUtc(file),
                    new FileInfo(file).Length,
                    $"Unreadable backup: {exception.GetBaseException().Message}"));
            }
        }

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
        RemoveEmptyParentDirectories(Path.GetDirectoryName(path));
        return Task.CompletedTask;
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
            ReadOptionalTimeSpan(root, "timeToLive"),
            ReadOptionalDateTimeOffset(root, "scheduledEnqueueTimeUtc"));
        var applicationProperties = root.TryGetProperty("applicationProperties", out var values) &&
                                    values.ValueKind == JsonValueKind.Array
            ? values.EnumerateArray().Select(value => new MessageApplicationProperty(
                ReadRequiredString(value, "name"),
                ReadEnum<ApplicationPropertyType>(value, "type"),
                ReadRequiredString(value, "value"))).ToArray()
            : [];
        var body = root.GetProperty("bodyBase64").GetBytesFromBase64();

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
            originalBodySize: ReadInt64(root, "bodySize"));
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
        foreach (var segment in segments.SkipLast(1))
        {
            current = Path.Combine(current, segment);
            if (Directory.Exists(current) &&
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
               !string.Equals(directory, RootDirectory, _pathComparison) &&
               !Directory.EnumerateFileSystemEntries(directory).Any())
        {
            Directory.Delete(directory);
            directory = Path.GetDirectoryName(directory);
        }
    }

    private static void EnsureSchema(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty("schemaVersion", out var schema) ||
            schema.GetInt32() != 1)
        {
            throw new InvalidDataException("Unsupported backup JSON schema.");
        }
    }

    private static ServiceBusEntityReference ParseSource(JsonElement root)
    {
        var kind = ReadEnum<ServiceBusEntityKind>(root, "sourceKind");
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
