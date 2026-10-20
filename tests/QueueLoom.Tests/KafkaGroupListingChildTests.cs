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
    public async Task AChildThatCannotStartCostsOnlyTheLag()
    {
        // Process.Start throws Win32Exception for a missing executable; that must become the lag note too, not a failed
        // refresh that hides the topics.
        var missing = Path.Combine(AppContext.BaseDirectory, "no-such-folder", OperatingSystem.IsWindows() ? "QueueLoom.exe" : "QueueLoom");

        var error = await Assert.ThrowsAsync<KafkaGroupListing.GroupListingException>(() => KafkaGroupListing.ListAsync(null, Unreachable,
            TimeSpan.FromSeconds(2), CancellationToken.None, () => new ProcessStartInfo(missing, [KafkaGroupListing.HelperArgument])));

        Assert.StartsWith("the listing process could not start", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheAppStartsItsListingChildFromAWindowsPathBeyondMaxPath()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows extended-path process launch."); return; }
        // The launcher supports installations whose app executable is at 260 characters or more; Process.Start cannot
        // start an executable there. A junction at the end of a long folder chain points at the built app.
        var root = Path.Combine(Path.GetTempPath(), "ql-long-" + Guid.NewGuid().ToString("N")[..8]);
        var folder = root;
        while (folder.Length < 240) folder = Path.Combine(folder, new string('d', 40));
        Directory.CreateDirectory(folder);
        var app = Path.Combine(folder, "app");
        try
        {
            using (var link = Process.Start(new ProcessStartInfo("cmd.exe", ["/c", "mklink", "/J", Extended + app, AppContext.BaseDirectory.TrimEnd('\\')])
                   { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true })!)
            {
                await link.WaitForExitAsync();
                Assert.True(link.ExitCode == 0, "mklink failed");
            }
            var executable = Path.Combine(app, "QueueLoom.exe");
            Assert.True(executable.Length >= 260);

            var error = await Assert.ThrowsAsync<KafkaGroupListing.GroupListingException>(() => KafkaGroupListing.ListAsync(null, Unreachable,
                TimeSpan.FromSeconds(2), CancellationToken.None, () => new ProcessStartInfo(executable, [KafkaGroupListing.HelperArgument])));

            // It started and answered with the broker's reason.
            Assert.False(error.Message.Contains("listing process", StringComparison.Ordinal), error.Message);
        }
        finally
        {
            // The junction first, so deleting the folders cannot reach the built app it points at.
            if (Directory.Exists(Extended + app)) Directory.Delete(Extended + app);
            Directory.Delete(Extended + root, recursive: true);
        }
    }

    private const string Extended = @"\\?\";

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
