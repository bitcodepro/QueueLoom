using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests;

public sealed class DeadLetterJsonBackupStoreTests
{
    [Fact]
    public async Task BackupWritesFullMessageJsonIntoCompactDateAndEntityFolders()
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = QueueLoomPaths.ForRoot(root);
            var store = new DeadLetterJsonBackupStore(paths);
            var profile = new ServiceBusProfile(
                Guid.NewGuid(),
                "Development",
                EnvironmentKind.Development,
                null,
                "development.servicebus.windows.net",
                AuthenticationSettings.Entra(),
                ProfileAccessMode.ReadWrite);
            var startedAt = DateTimeOffset.Parse("2026-08-12T10:20:30Z");
            var session = await store.CreateSessionAsync(profile, startedAt, CancellationToken.None);
            var body = Encoding.UTF8.GetBytes("{\"orderId\":42}");
            var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: new BinaryData(body),
                messageId: "message/42",
                correlationId: "correlation-42",
                subject: "order.failed",
                contentType: "application/json",
                properties: new Dictionary<string, object> { ["tenant"] = "northwind" },
                sequenceNumber: 17,
                enqueuedSequenceNumber: 16,
                enqueuedTime: startedAt.AddMinutes(-1),
                deliveryCount: 3,
                serviceBusMessageState: Azure.Messaging.ServiceBus.ServiceBusMessageState.Active);

            var path = await session.BackupAsync(
                message,
                ServiceBusEntityReference.Subscription("orders", "billing"),
                ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);

            Assert.True(File.Exists(path));
            Assert.Contains(Path.Combine("2026-08-12"), path, StringComparison.Ordinal);
            Assert.Contains(
                Path.Combine(
                    DeadLetterJsonBackupStore.UniqueEntitySegment(
                        ServiceBusEntityReference.Subscription("orders", "billing")),
                    "dlq"),
                path,
                StringComparison.Ordinal);
            using var document = JsonDocument.Parse(await File.ReadAllTextAsync(path));
            var json = document.RootElement;
            Assert.Equal("correlation-42", json.GetProperty("correlationId").GetString());
            Assert.Equal(17, json.GetProperty("sequenceNumber").GetInt64());
            Assert.Equal(body, json.GetProperty("bodyBase64").GetBytesFromBase64());
            Assert.Equal("northwind", json.GetProperty("applicationProperties")[0].GetProperty("value").GetString());
            Assert.True(File.Exists(Path.Combine(session.RootDirectory, "session.json")));

            var repository = new JsonDeadLetterBackupRepository(paths);
            var summary = Assert.Single(await repository.ListAsync());
            Assert.Equal(profile.Id, summary.ProfileId);
            Assert.Equal("orders / billing", summary.Source.DisplayName);
            Assert.Equal("correlation-42", summary.CorrelationId);

            var restored = await repository.LoadAsync(summary);
            Assert.Equal(body, restored.Body.ToArray());
            Assert.Equal("message/42", restored.Properties.MessageId);
            Assert.Equal("northwind", Assert.Single(restored.ApplicationProperties).Value);

            await repository.DeleteAsync(summary);
            Assert.False(File.Exists(path));
            Assert.True(File.Exists(Path.Combine(session.RootDirectory, "session.json")));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task BackupUsesDistinctPathsForEntityNamesThatSanitizeIdentically()
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = QueueLoomPaths.ForRoot(root);
            var session = await CreateSessionAsync(paths);
            var slashSource = ServiceBusEntityReference.Queue("a/b");
            var underscoreSource = ServiceBusEntityReference.Queue("a_b");
            var first = CreateMessage(sequenceNumber: 7, messageId: "same-message");
            var second = CreateMessage(sequenceNumber: 7, messageId: "same-message");

            var firstPath = await session.BackupAsync(
                first,
                slashSource,
                ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);
            var secondPath = await session.BackupAsync(
                second,
                underscoreSource,
                ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);

            Assert.False(string.Equals(
                Path.GetDirectoryName(firstPath),
                Path.GetDirectoryName(secondPath),
                StringComparison.OrdinalIgnoreCase));
            Assert.True(File.Exists(firstPath));
            Assert.True(File.Exists(secondPath));

            var repository = new JsonDeadLetterBackupRepository(paths);
            var summaries = await repository.ListAsync();
            Assert.Equal(2, summaries.Count);
            Assert.Contains(summaries, summary => summary.Source == slashSource);
            Assert.Contains(summaries, summary => summary.Source == underscoreSource);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task BackupUsesDistinctPathsForQueueAndSubscriptionWithTheSameDisplayPath()
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = QueueLoomPaths.ForRoot(root);
            var session = await CreateSessionAsync(paths);
            var queue = ServiceBusEntityReference.Queue("orders/Subscriptions/billing");
            var subscription = ServiceBusEntityReference.Subscription("orders", "billing");

            var queuePath = await session.BackupAsync(
                CreateMessage(sequenceNumber: 11, messageId: "same-message"),
                queue,
                ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);
            var subscriptionPath = await session.BackupAsync(
                CreateMessage(sequenceNumber: 11, messageId: "same-message"),
                subscription,
                ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);

            Assert.False(string.Equals(
                Path.GetDirectoryName(queuePath),
                Path.GetDirectoryName(subscriptionPath),
                StringComparison.OrdinalIgnoreCase));
            var summaries = await new JsonDeadLetterBackupRepository(paths).ListAsync();
            Assert.Contains(summaries, summary => summary.Source == queue);
            Assert.Contains(summaries, summary => summary.Source == subscription);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task BackupUsesDistinctPathsWhenSubscriptionPathsHaveTheSameRenderedText()
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = QueueLoomPaths.ForRoot(root);
            var session = await CreateSessionAsync(paths);
            var firstSource = ServiceBusEntityReference.Subscription("a/Subscriptions/b", "c");
            var secondSource = ServiceBusEntityReference.Subscription("a", "b/Subscriptions/c");
            Assert.Equal(firstSource.Path, secondSource.Path);

            var firstPath = await session.BackupAsync(
                CreateMessage(sequenceNumber: 12, messageId: "same-message"),
                firstSource,
                ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);
            var secondPath = await session.BackupAsync(
                CreateMessage(sequenceNumber: 12, messageId: "same-message"),
                secondSource,
                ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);

            Assert.False(string.Equals(
                Path.GetDirectoryName(firstPath),
                Path.GetDirectoryName(secondPath),
                StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task BackupNeverOverwritesDuplicateSequenceAndMessageIds()
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = QueueLoomPaths.ForRoot(root);
            var session = await CreateSessionAsync(paths);
            var source = ServiceBusEntityReference.Queue("orders");

            var firstPath = await session.BackupAsync(
                CreateMessage(sequenceNumber: 42, messageId: "duplicate"),
                source,
                ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);
            var secondPath = await session.BackupAsync(
                CreateMessage(sequenceNumber: 42, messageId: "duplicate"),
                source,
                ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);

            Assert.NotEqual(firstPath, secondPath);
            Assert.True(new FileInfo(firstPath).Length > 0);
            Assert.True(new FileInfo(secondPath).Length > 0);
            Assert.Empty(Directory.EnumerateFiles(
                Path.GetDirectoryName(firstPath)!,
                "*.tmp",
                SearchOption.TopDirectoryOnly));

            var repository = new JsonDeadLetterBackupRepository(paths);
            Assert.Equal(2, (await repository.ListAsync()).Count);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task WorstCaseNamesProduceABoundedWindowsSafeRelativePath()
    {
        var root = Path.Combine(Path.GetTempPath(), "ql", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = QueueLoomPaths.ForRoot(root);
            var profile = new ServiceBusProfile(
                Guid.NewGuid(),
                new string('p', 1_000),
                EnvironmentKind.Development,
                null,
                "development.servicebus.windows.net",
                AuthenticationSettings.Entra(),
                ProfileAccessMode.ReadWrite);
            var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(
                profile,
                DateTimeOffset.Parse("2026-08-12T10:20:30Z"),
                CancellationToken.None);
            var source = ServiceBusEntityReference.Subscription(
                new string('t', 1_000),
                new string('s', 1_000));
            var path = await session.BackupAsync(
                CreateMessage(sequenceNumber: long.MaxValue, messageId: new string('m', 4_000)),
                source,
                ServiceBusSubQueue.TransferDeadLetter,
                CancellationToken.None);

            var relativePath = Path.GetRelativePath(paths.BackupsDirectory, path);
            Assert.True(relativePath.Length <= 160, $"Relative backup path was {relativePath.Length} characters: {relativePath}");
            Assert.All(
                relativePath.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar]),
                segment => Assert.True(segment.Length <= 64, $"Path component was {segment.Length} characters: {segment}"));
            Assert.DoesNotContain(new string('m', 80), path, StringComparison.Ordinal);
            Assert.Matches("^[0-9]{20}-[a-z2-7]{26}\\.json$", Path.GetFileName(path));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task BackupViewerStillLoadsLegacyUnhashedPathWithoutBackupId()
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = QueueLoomPaths.ForRoot(root);
            var session = await CreateSessionAsync(paths);
            var source = ServiceBusEntityReference.Queue("legacy/orders");
            var currentPath = await session.BackupAsync(
                CreateMessage(sequenceNumber: 9, messageId: "legacy-message"),
                source,
                ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);

            var legacyDirectory = Path.Combine(
                session.RootDirectory,
                "queues",
                DeadLetterJsonBackupStore.SafeSegment(source.Name),
                "dead-letter");
            Directory.CreateDirectory(legacyDirectory);
            var legacyPath = Path.Combine(legacyDirectory, "00000000000000000009_legacy-message.json");
            var legacyJson = JsonNode.Parse(await File.ReadAllTextAsync(currentPath))!.AsObject();
            Assert.True(legacyJson.Remove("backupId"));
            await File.WriteAllTextAsync(legacyPath, legacyJson.ToJsonString(new JsonSerializerOptions
            {
                WriteIndented = true
            }));
            File.Delete(currentPath);

            var repository = new JsonDeadLetterBackupRepository(paths);
            var summary = Assert.Single(await repository.ListAsync());
            Assert.Equal(source, summary.Source);
            Assert.Equal("legacy-message", summary.MessageId);
            var restored = await repository.LoadAsync(summary);
            Assert.Equal("legacy-message", restored.Properties.MessageId);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    private static async Task<DeadLetterJsonBackupSession> CreateSessionAsync(QueueLoomPaths paths)
    {
        var profile = new ServiceBusProfile(
            Guid.NewGuid(),
            "Development",
            EnvironmentKind.Development,
            null,
            "development.servicebus.windows.net",
            AuthenticationSettings.Entra(),
            ProfileAccessMode.ReadWrite);
        return await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(
            profile,
            DateTimeOffset.Parse("2026-08-12T10:20:30Z"),
            CancellationToken.None);
    }

    private static ServiceBusReceivedMessage CreateMessage(long sequenceNumber, string messageId) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: new BinaryData(Encoding.UTF8.GetBytes("{\"value\":1}")),
            messageId: messageId,
            sequenceNumber: sequenceNumber,
            enqueuedSequenceNumber: sequenceNumber,
            enqueuedTime: DateTimeOffset.Parse("2026-08-12T10:19:30Z"),
            serviceBusMessageState: Azure.Messaging.ServiceBus.ServiceBusMessageState.Active);
}
