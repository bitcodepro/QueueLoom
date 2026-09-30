using System.Globalization;
using System.Security.Cryptography;
using System.Text;
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
        // Profile/entity/message names can each approach the Windows component limit.
        // Keep all user-controlled values in JSON and use fixed-size identifiers on disk.
        // The date folder remains useful when operators inspect or archive backups.
        var sessionName = $"{startedAt.UtcDateTime:HHmmss}-{CompactGuid(Guid.NewGuid())}";
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
                provider = profile.Provider.ToString(),
                fullyQualifiedNamespace = profile.EndpointDisplay,
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

    internal static string UniqueEntitySegment(ServiceBusEntityReference source)
    {
        ArgumentNullException.ThrowIfNull(source);

        // Include the kind so a queue whose literal name resembles a subscription
        // path cannot share a directory with that subscription. Lower-case base32
        // preserves the full SHA-256 while remaining safe on case-insensitive disks.
        var identity = source.Kind switch
        {
            ServiceBusEntityKind.Queue => $"q:{source.Name.Length}:{source.Name}",
            ServiceBusEntityKind.Subscription =>
                $"s:{source.TopicName!.Length}:{source.TopicName}:{source.Name.Length}:{source.Name}",
            _ => throw new ArgumentException("Only queues and subscriptions can be backed up.", nameof(source))
        };
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(identity));
        var kind = source.Kind switch
        {
            ServiceBusEntityKind.Queue => "q",
            ServiceBusEntityKind.Subscription => "s",
            _ => throw new ArgumentOutOfRangeException(nameof(source), source.Kind, "Unsupported source kind.")
        };
        return $"{kind}-{ToLowerBase32(hash)}";
    }

    internal static string CompactGuid(Guid value)
    {
        Span<byte> bytes = stackalloc byte[16];
        value.TryWriteBytes(bytes);
        return ToLowerBase32(bytes);
    }

    internal static string ToLowerBase32(ReadOnlySpan<byte> bytes)
    {
        const string alphabet = "abcdefghijklmnopqrstuvwxyz234567";
        var outputLength = checked((bytes.Length * 8 + 4) / 5);
        Span<char> output = outputLength <= 128
            ? stackalloc char[outputLength]
            : new char[outputLength];
        var outputIndex = 0;
        var buffer = 0;
        var bufferedBits = 0;

        foreach (var value in bytes)
        {
            buffer = (buffer << 8) | value;
            bufferedBits += 8;
            while (bufferedBits >= 5)
            {
                bufferedBits -= 5;
                output[outputIndex++] = alphabet[(buffer >> bufferedBits) & 0x1f];
            }

            buffer = bufferedBits == 0
                ? 0
                : buffer & ((1 << bufferedBits) - 1);
        }

        if (bufferedBits > 0)
        {
            output[outputIndex++] = alphabet[(buffer << (5 - bufferedBits)) & 0x1f];
        }

        return new string(output[..outputIndex]);
    }

    internal static void RestrictDirectory(string path) =>
        AtomicFile.RestrictDirectoryToCurrentUser(path);
}

public sealed class DeadLetterJsonBackupSession(
    string rootDirectory,
    ServiceBusProfile profile,
    DateTimeOffset startedAt)
{
    public string RootDirectory { get; } = Path.GetFullPath(rootDirectory);

    public Task<string> BackupAsync(
        ServiceBusReceivedMessage message,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(source);

        return WriteBackupAsync(source, subQueue, message.SequenceNumber, writer =>
        {
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
            WriteApplicationProperties(writer, message.ApplicationProperties.Select(AzureMessageMapper.ToDomainProperty));
            WriteBody(writer, message.Body.ToMemory());
        }, cancellationToken);
    }

    /// <summary>Backs up a message received from a service other than Azure Service Bus (SQS, Pub/Sub).</summary>
    public Task<string> BackupAsync(BrowsedMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.IsBodyTruncated)
        {
            throw new InvalidOperationException("A message whose body was truncated cannot be backed up in full.");
        }

        var properties = message.Properties;
        return WriteBackupAsync(message.Source, message.SubQueue, message.SequenceNumber, writer =>
        {
            writer.WriteNumber("sequenceNumber", message.SequenceNumber);
            writer.WriteNumber("enqueuedSequenceNumber", message.EnqueuedSequenceNumber);
            WriteString(writer, "messageId", properties.MessageId);
            WriteString(writer, "correlationId", properties.CorrelationId);
            WriteString(writer, "subject", properties.Subject);
            WriteString(writer, "contentType", properties.ContentType);
            WriteString(writer, "to", properties.To);
            WriteString(writer, "replyTo", properties.ReplyTo);
            WriteString(writer, "sessionId", properties.SessionId);
            WriteString(writer, "replyToSessionId", properties.ReplyToSessionId);
            WriteString(writer, "partitionKey", properties.PartitionKey);
            WriteString(writer, "transactionPartitionKey", properties.TransactionPartitionKey);
            WriteDate(writer, "scheduledEnqueueTimeUtc", properties.ScheduledEnqueueTime);
            WriteDate(writer, "enqueuedTimeUtc", message.EnqueuedAt);
            WriteDate(writer, "expiresAtUtc", message.ExpiresAt);
            WriteString(writer, "timeToLive", properties.TimeToLive?.ToString("c", CultureInfo.InvariantCulture));
            writer.WriteNumber("deliveryCount", message.DeliveryCount);
            writer.WriteString("state", message.State.ToString());
            WriteString(writer, "deadLetterReason", message.DeadLetterReason);
            WriteString(writer, "deadLetterErrorDescription", message.DeadLetterErrorDescription);
            WriteApplicationProperties(writer, message.ApplicationProperties);
            WriteBody(writer, message.Body);
        }, cancellationToken);
    }

    private async Task<string> WriteBackupAsync(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        long sequenceNumber,
        Action<Utf8JsonWriter> writeMessage,
        CancellationToken cancellationToken)
    {
        var relativeDirectory = Path.Combine(
            DeadLetterJsonBackupStore.UniqueEntitySegment(source),
            subQueue switch
        {
            ServiceBusSubQueue.DeadLetter => "dlq",
            ServiceBusSubQueue.TransferDeadLetter => "tdlq",
            ServiceBusSubQueue.Active => "active",
            _ => throw new ArgumentOutOfRangeException(nameof(subQueue), subQueue, "Unsupported backup subqueue.")
        });

        var directory = Path.Combine(RootDirectory, relativeDirectory);
        Directory.CreateDirectory(directory);
        DeadLetterJsonBackupStore.RestrictDirectory(directory);
        var backupId = Guid.NewGuid();
        var destination = Path.Combine(
            directory,
            $"{sequenceNumber:D20}-{DeadLetterJsonBackupStore.CompactGuid(backupId)}.json");
        var temporary = Path.Combine(
            directory,
            $".{DeadLetterJsonBackupStore.CompactGuid(Guid.NewGuid())}.tmp");

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
                writer.WriteString("backupId", backupId);
                writer.WriteString("backedUpAtUtc", DateTimeOffset.UtcNow);
                writer.WriteString("purgeStartedAtUtc", startedAt);
                writer.WriteString("profileId", profile.Id);
                writer.WriteString("profileName", profile.Name);
                writer.WriteString("environment", profile.Environment.ToString());
                writer.WriteString("provider", profile.Provider.ToString());
                WriteString(writer, "fullyQualifiedNamespace", profile.EndpointDisplay);
                writer.WriteString("sourceKind", source.Kind.ToString());
                writer.WriteString("sourcePath", source.Path);
                writer.WriteString("sourceName", source.Name);
                WriteString(writer, "topicName", source.TopicName);
                writer.WriteString("subQueue", subQueue.ToString());
                writeMessage(writer);
                writer.WriteEndObject();
                await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                // A successful return authorizes the caller to settle the
                // message. Flush through OS buffers before publishing the final
                // name, so an in-memory write is never treated as a backup.
                stream.Flush(flushToDisk: true);
            }
            AtomicFile.RestrictToCurrentUser(temporary);
            // Never overwrite: independently delivered messages may legitimately
            // share SequenceNumber/MessageId across entities or purge attempts.
            File.Move(temporary, destination);
            var completedFile = new FileInfo(destination);
            if (!completedFile.Exists || completedFile.Length == 0)
            {
                throw new IOException("The durable dead-letter backup file was not created.");
            }
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

    private static void WriteApplicationProperties(
        Utf8JsonWriter writer,
        IEnumerable<MessageApplicationProperty> properties)
    {
        writer.WriteStartArray("applicationProperties");
        foreach (var value in properties)
        {
            writer.WriteStartObject();
            writer.WriteString("name", value.Name);
            writer.WriteString("type", value.Type.ToString());
            writer.WriteString("value", value.Value);
            writer.WriteEndObject();
        }
        writer.WriteEndArray();
    }

    private static void WriteBody(Utf8JsonWriter writer, ReadOnlyMemory<byte> body)
    {
        writer.WriteNumber("bodySize", body.Length);
        writer.WriteBase64String("bodyBase64", body.Span);
    }

    private static void WriteDate(Utf8JsonWriter writer, string propertyName, DateTimeOffset? value)
    {
        if (value is null)
        {
            writer.WriteNull(propertyName);
        }
        else
        {
            writer.WriteString(propertyName, value.Value);
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
