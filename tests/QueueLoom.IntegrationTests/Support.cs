using QueueLoom.Core.Abstractions;

namespace QueueLoom.IntegrationTests;

/// <summary>
/// Runs only when the emulator named by <paramref name="variable"/> is configured, for example
/// QUEUELOOM_LOCALSTACK_URL=http://localhost:4566 or QUEUELOOM_PUBSUB_EMULATOR=localhost:8085.
/// </summary>
public sealed class EmulatorFactAttribute : FactAttribute
{
    public EmulatorFactAttribute(string variable)
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(variable)))
        {
            Skip = $"Set {variable} to run this test against a local emulator.";
        }
    }
}

internal static class Emulators
{
    public const string LocalStack = "QUEUELOOM_LOCALSTACK_URL";
    public const string PubSub = "QUEUELOOM_PUBSUB_EMULATOR";
    public const string ServiceBus = "QUEUELOOM_SERVICEBUS_EMULATOR";

    /// <summary>host:port of a RabbitMQ broker with the management plugin on port + 10000, user guest/guest.</summary>
    public const string RabbitMq = "QUEUELOOM_RABBITMQ";

    /// <summary>Bootstrap servers of a Kafka cluster without authentication, for example localhost:9092.</summary>
    public const string Kafka = "QUEUELOOM_KAFKA";

    public static string LocalStackUrl => Environment.GetEnvironmentVariable(LocalStack)!;

    public static string PubSubHost => Environment.GetEnvironmentVariable(PubSub)!;

    /// <summary>The emulator's connection string, with UseDevelopmentEmulator=true.</summary>
    public static string ServiceBusConnectionString => Environment.GetEnvironmentVariable(ServiceBus)!;

    public static string KafkaServers => Environment.GetEnvironmentVariable(Kafka)!;

    public static string RabbitMqHost => Environment.GetEnvironmentVariable(RabbitMq)!.Split(':')[0];

    public static int RabbitMqPort => int.Parse(Environment.GetEnvironmentVariable(RabbitMq)!.Split(':')[1], System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>A short random suffix so tests never see each other's queues and topics.</summary>
    public static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid().ToString("N")[..8]}";
}

internal sealed class TemporaryDirectory : IDisposable
{
    public string Path { get; } = System.IO.Path.Combine(
        System.IO.Path.GetTempPath(), "QueueLoom.IntegrationTests", Guid.NewGuid().ToString("N"));

    public TemporaryDirectory() => Directory.CreateDirectory(Path);

    public void Dispose()
    {
        if (Directory.Exists(Path))
        {
            Directory.Delete(Path, recursive: true);
        }
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
