using System.Text;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests;

public sealed class DeadLetterJsonBackupStoreTests
{
    [Fact]
    public async Task BackupWritesFullMessageJsonIntoDateTopicAndSubscriptionFolders()
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
                Path.Combine("topics", "orders", "subscriptions", "billing", "dead-letter"),
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
}
