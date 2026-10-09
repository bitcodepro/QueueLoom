using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

// A RabbitMQ message was resent or moved without its AMQP content-encoding and priority. A gzip body then reached
// consumers marked as plain bytes, and a message on a priority queue (x-max-priority) lost its place. Both are read,
// kept through a backup, and published again; a message without them still has none.
public sealed class RabbitEncodingAndPriorityTests
{
    private static BrowsedMessage Browse(bool present) => RabbitMqMessageMapper.FromAmqp("body"u8.ToArray(),
        present
            ? new BasicProperties { MessageId = "m-1", ContentEncoding = "gzip", Priority = 7 }
            : new BasicProperties { MessageId = "m-1" },
        "orders", ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ContentEncodingAndPrioritySurviveAResendAndABackup(bool present)
    {
        var message = Browse(present);
        AssertWire(message.CreateDraft(), present);

        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword))
            with { Provider = MessagingProvider.RabbitMq };
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
        await session.BackupAsync(message, default);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));
        AssertWire(restored.CreateDraft(), present);
    }

    // A resend or move is saved as a durable plan before anything is sent, and each item is published from that file.
    [Fact]
    public void ContentEncodingAndPrioritySurviveTheSavedOperationPlan()
    {
        var draft = Browse(present: true).CreateDraft();
        var payload = new ReplayPayload(draft.Body, draft.Properties, draft.ApplicationProperties.ToArray(), "orders / DeadLetter / 1 / m-1");

        var saved = System.Text.Json.JsonSerializer.Deserialize<ReplayPayload>(System.Text.Json.JsonSerializer.Serialize(payload))!;

        AssertWire(new MessageDraft(saved.Body, saved.Properties, saved.ApplicationProperties), present: true);
    }

    private static void AssertWire(MessageDraft draft, bool present)
    {
        var wire = RabbitMqMessageMapper.ToAmqp(draft);
        Assert.Equal(present ? "gzip" : null, wire.ContentEncoding);
        Assert.Equal(present, wire.IsPriorityPresent());
        if (present) Assert.Equal(7, wire.Priority);
    }
}
