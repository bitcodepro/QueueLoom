using System.Globalization;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Infrastructure.Persistence;

public sealed class DeadLetterJsonBackupStore
{
    private readonly QueueLoomPaths _paths;

    public DeadLetterJsonBackupStore(QueueLoomPaths paths)
    {
        ArgumentNullException.ThrowIfNull(paths);
        _paths = paths;
    }

    public async Task<DeadLetterJsonBackupSession> CreateSessionAsync(
        ServiceBusProfile profile,
        DateTimeOffset startedAt,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(profile);
        var sessionName =
            $"{startedAt.UtcDateTime:yyyyMMddTHHmmss.fffffffZ}_{SafeSegment(profile.Name)}_{Guid.NewGuid():N}";
        var sessionDirectory = Path.Combine(
            _paths.BackupsDirectory,
            startedAt.UtcDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            sessionName);
        Directory.CreateDirectory(sessionDirectory);
        RestrictDirectory(sessionDirectory);

        var metadata = JsonSerializer.Serialize(
            new
            {
                schemaVersion = 1,
                createdAtUtc = startedAt,
                profileId = profile.Id,
                profileName = profile.Name,
                environment = profile.Environment.ToString(),
                fullyQualifiedNamespace = profile.FullyQualifiedNamespace,
                format = "One full-fidelity JSON file per message. A message is settled only after its file is written."
            },
            new JsonSerializerOptions { WriteIndented = true });
        await AtomicFile.WriteTextAsync(
                Path.Combine(sessionDirectory, "session.json"),
                metadata,
                cancellationToken)
            .ConfigureAwait(false);

        return new DeadLetterJsonBackupSession(sessionDirectory, profile, startedAt);
    }

    internal static string SafeSegment(string value)
    {
        var sanitized = new string(value
            .Select(character => char.IsLetterOrDigit(character) || character is '-' or '_' or '.'
                ? character
                : '_')
            .ToArray())
            .Trim('.', '_');
        if (sanitized.Length == 0)
        {
            sanitized = "unnamed";
        }
        return sanitized.Length <= 80 ? sanitized : sanitized[..80];
    }

    internal static void RestrictDirectory(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        try
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
        catch (PlatformNotSupportedException)
        {
        }
    }
}

public sealed class DeadLetterJsonBackupSession(
    string rootDirectory,
    ServiceBusProfile profile,
    DateTimeOffset startedAt)
{
    public string RootDirectory { get; } = Path.GetFullPath(rootDirectory);

    public async Task<string> BackupAsync(
        ServiceBusReceivedMessage message,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(source);

        var relativeDirectory = source.Kind switch
        {
            ServiceBusEntityKind.Queue => Path.Combine(
                "queues",
                DeadLetterJsonBackupStore.SafeSegment(source.Name)),
            ServiceBusEntityKind.Subscription => Path.Combine(
                "topics",
                DeadLetterJsonBackupStore.SafeSegment(source.TopicName!),
                "subscriptions",
                DeadLetterJsonBackupStore.SafeSegment(source.Name)),
            _ => throw new ArgumentException("Only queues and subscriptions can be backed up.", nameof(source))
        };
        relativeDirectory = Path.Combine(relativeDirectory, subQueue switch
        {
            ServiceBusSubQueue.DeadLetter => "dead-letter",
            ServiceBusSubQueue.TransferDeadLetter => "transfer-dead-letter",
            _ => throw new ArgumentOutOfRangeException(nameof(subQueue), subQueue, "Unsupported backup subqueue.")
        });

        var directory = Path.Combine(RootDirectory, relativeDirectory);
        Directory.CreateDirectory(directory);
        DeadLetterJsonBackupStore.RestrictDirectory(directory);
        var messageId = DeadLetterJsonBackupStore.SafeSegment(message.MessageId ?? "no-message-id");
        var destination = Path.Combine(directory, $"{message.SequenceNumber:D20}_{messageId}.json");
        var temporary = Path.Combine(directory, $".{Path.GetFileName(destination)}.{Guid.NewGuid():N}.tmp");

        try
        {
            await using (var stream = new FileStream(
                             temporary,
                             FileMode.CreateNew,
                             FileAccess.Write,
                             FileShare.None,
                             bufferSize: 64 * 1024,
                             FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true });
                writer.WriteStartObject();
                writer.WriteNumber("schemaVersion", 1);
                writer.WriteString("backedUpAtUtc", DateTimeOffset.UtcNow);
                writer.WriteString("purgeStartedAtUtc", startedAt);
                writer.WriteString("profileId", profile.Id);
                writer.WriteString("profileName", profile.Name);
                writer.WriteString("environment", profile.Environment.ToString());
                writer.WriteString("fullyQualifiedNamespace", profile.FullyQualifiedNamespace);
                writer.WriteString("sourceKind", source.Kind.ToString());
                writer.WriteString("sourcePath", source.Path);
                writer.WriteString("subQueue", subQueue.ToString());
                writer.WriteNumber("sequenceNumber", message.SequenceNumber);
                writer.WriteNumber("enqueuedSequenceNumber", message.EnqueuedSequenceNumber);
                writer.WriteString("messageId", message.MessageId);
                WriteString(writer, "correlationId", message.CorrelationId);
                WriteString(writer, "subject", message.Subject);
                WriteString(writer, "contentType", message.ContentType);
                WriteString(writer, "to", message.To);
                WriteString(writer, "replyTo", message.ReplyTo);
                WriteString(writer, "sessionId", message.SessionId);
                WriteString(writer, "replyToSessionId", message.ReplyToSessionId);
                WriteString(writer, "partitionKey", message.PartitionKey);
                WriteString(writer, "transactionPartitionKey", message.TransactionPartitionKey);
                writer.WriteString("scheduledEnqueueTimeUtc", message.ScheduledEnqueueTime);
                writer.WriteString("enqueuedTimeUtc", message.EnqueuedTime);
                writer.WriteString("expiresAtUtc", message.ExpiresAt);
                writer.WriteString("timeToLive", message.TimeToLive.ToString("c", CultureInfo.InvariantCulture));
                writer.WriteNumber("deliveryCount", message.DeliveryCount);
                writer.WriteString("state", message.State.ToString());
                WriteString(writer, "deadLetterReason", message.DeadLetterReason);
                WriteString(writer, "deadLetterErrorDescription", message.DeadLetterErrorDescription);

                writer.WriteStartArray("applicationProperties");
                foreach (var property in message.ApplicationProperties)
                {
                    var value = AzureMessageMapper.ToDomainProperty(property);
                    writer.WriteStartObject();
                    writer.WriteString("name", value.Name);
                    writer.WriteString("type", value.Type.ToString());
                    writer.WriteString("value", value.Value);
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();

                var body = message.Body.ToMemory();
                writer.WriteNumber("bodySize", body.Length);
                writer.WriteBase64String("bodyBase64", body.Span);
                writer.WriteEndObject();
                await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                stream.Flush(flushToDisk: true);
            }
            AtomicFile.RestrictToCurrentUser(temporary);
            File.Move(temporary, destination, overwrite: true);
            return destination;
        }
        finally
        {
            if (File.Exists(temporary))
            {
                File.Delete(temporary);
            }
        }
    }

    private static void WriteString(Utf8JsonWriter writer, string propertyName, string? value)
    {
        if (value is null)
        {
            writer.WriteNull(propertyName);
        }
        else
        {
            writer.WriteString(propertyName, value);
        }
    }
}
