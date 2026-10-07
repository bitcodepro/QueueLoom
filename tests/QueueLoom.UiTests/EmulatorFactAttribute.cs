using System.Runtime.CompilerServices;

namespace QueueLoom.UiTests;

/// <summary>
/// A UI test that drives a real broker or emulator. When one of its environment variables is not set, the test is
/// reported as skipped, never as passed: a run without brokers must not look like it exercised them.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class EmulatorFactAttribute : FactAttribute
{
    public EmulatorFactAttribute(string[] variables, [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1) : base(sourceFilePath, sourceLineNumber)
    {
        Skip = SkipReason(variables, Environment.GetEnvironmentVariable);
    }

    public EmulatorFactAttribute(string variable, [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1) : this([variable], sourceFilePath, sourceLineNumber)
    {
    }

    /// <summary>Why the test is skipped, naming every missing variable; null when all are set.</summary>
    internal static string? SkipReason(IReadOnlyList<string> variables, Func<string, string?> read)
    {
        if (variables.Count == 0)
        {
            throw new ArgumentException("Name at least one environment variable.", nameof(variables));
        }
        var missing = variables.Where(variable => string.IsNullOrWhiteSpace(read(variable))).ToArray();
        return missing.Length == 0 ? null : $"Set {string.Join(" and ", missing)} to run this test against a local emulator.";
    }
}

internal static class Emulators
{
    public const string ServiceBus = "QUEUELOOM_SERVICEBUS_EMULATOR";
    public const string LocalStack = "QUEUELOOM_LOCALSTACK_URL";
    public const string PubSub = "QUEUELOOM_PUBSUB_EMULATOR";
    public const string RabbitMq = "QUEUELOOM_RABBITMQ";
    public const string Kafka = "QUEUELOOM_KAFKA";
}
