using System.Diagnostics;
using Confluent.Kafka;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// librdkafka 2.15 has freed a ListConsumerGroups result twice, aborting the whole process. The app now lists groups in
// a child of itself, so such a crash costs only the lag: it becomes a note, and the topics are still shown.
public sealed class KafkaGroupListingChildTests
{
    /// <summary>Starts the app assembly, as built next to this test, in its listing mode.</summary>
    internal static ProcessStartInfo AppChild(params string[] arguments)
    {
        var directory = AppContext.BaseDirectory;
        var start = new ProcessStartInfo(Environment.ProcessPath is { } path && Path.GetFileNameWithoutExtension(path) == "dotnet" ? path : "dotnet");
        foreach (var argument in new[] { "exec", "--runtimeconfig", Path.Combine(directory, "QueueLoom.Tests.runtimeconfig.json"),
                     "--depsfile", Path.Combine(directory, "QueueLoom.Tests.deps.json"), Path.Combine(directory, "QueueLoom.dll") }.Concat(arguments))
        {
            start.ArgumentList.Add(argument);
        }
        return start;
    }

    private static readonly ClientConfig Unreachable = new() { BootstrapServers = "127.0.0.1:1", SocketTimeoutMs = 1000 };

    [Fact]
    public async Task AChildThatDiesIsAReasonNotACrash()
    {
        // Any non-zero exit without an answer stands for an aborted child (the double free ends in SIGABRT).
        var error = await Assert.ThrowsAsync<KafkaGroupListing.GroupListingException>(() => KafkaGroupListing.ListAsync(null, Unreachable,
            TimeSpan.FromSeconds(2), CancellationToken.None, () => new ProcessStartInfo("dotnet", ["exec", Path.Combine(AppContext.BaseDirectory, "no-such-listing-child.dll")])));

        Assert.Contains("listing process stopped with exit code", error.Message, StringComparison.Ordinal);
        Assert.Contains("Consumer groups could not be read (the listing process", KafkaWorkspace.ConsumerGroupsUnreadable(error.Message), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheAppsListingModeAnswersWithTheBrokersReason()
    {
        // The real executable in its listing mode: the request goes in through standard input, the broker's refusal
        // comes back as the reason. With Confluent.Kafka 2.15/2.16 that failed listing corrupts the child's heap
        // (free(): invalid pointer on Linux, 0xC0000374 on Windows) once it answered; listed in this process, the
        // same request aborted the test host.
        var error = await Assert.ThrowsAsync<KafkaGroupListing.GroupListingException>(() => KafkaGroupListing.ListAsync(null, Unreachable,
            TimeSpan.FromSeconds(2), CancellationToken.None, () => AppChild(KafkaGroupListing.HelperArgument)));

        // On macOS the corruption can abort the child before it answers; the app still gets a reason, not a crash.
        if (!(OperatingSystem.IsMacOS() && error.Message.Contains("stopped with exit code", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("listing process", error.Message, StringComparison.Ordinal);
        }
        Assert.False(string.IsNullOrWhiteSpace(error.Message));
    }
}
