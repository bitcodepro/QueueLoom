using System.Diagnostics;
using Confluent.Kafka;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// librdkafka has corrupted the heap after a failed ListConsumerGroups, aborting the whole process. The app lists groups
// in a child of itself, so such a crash costs only the lag: it becomes a note, and the topics are still shown.
// These tests are deterministic: the parent's handling of a child that answers, dies or hangs is driven by a fixture
// child with a chosen outcome, and the real app's listing mode is exercised with a request that fails before any
// network call, so the librdkafka fault (which may abort, crash or hang the child) is never needed.
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

    /// <summary>The fixture child with a chosen outcome: answer-then-abort, abort, hang or garbage.</summary>
    private static ProcessStartInfo FixtureChild(string outcome) => new(
        Path.Combine(AppContext.BaseDirectory, "UpdateFixture", OperatingSystem.IsWindows() ? "QueueLoom.UpdateFixture.exe" : "QueueLoom.UpdateFixture"),
        ["--listing-fixture", outcome]);

    private static readonly ClientConfig Unreachable = new() { BootstrapServers = "127.0.0.1:1", SocketTimeoutMs = 1000 };

    /// <summary>A request librdkafka refuses while creating the client, before it sends anything.</summary>
    private static ClientConfig InvalidSetting()
    {
        var config = new ClientConfig { BootstrapServers = "127.0.0.1:1" };
        config.Set("no.such.property", "1");
        return config;
    }

    private static Task<KafkaGroupListing.GroupListingException> Fails(ClientConfig config, Func<ProcessStartInfo> child, TimeSpan? timeout = null) =>
        Assert.ThrowsAsync<KafkaGroupListing.GroupListingException>(() =>
            KafkaGroupListing.ListAsync(null, config, timeout ?? TimeSpan.FromSeconds(5), CancellationToken.None, child));

    [Fact]
    public async Task TheAppsListingModeAnswersThroughItsPipes()
    {
        // The real executable: the request goes in through standard input, and the answer (here librdkafka refusing the
        // setting) comes back through standard output as the reason.
        var error = await Fails(InvalidSetting(), () => AppChild(KafkaGroupListing.HelperArgument));

        Assert.Contains("no.such.property", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("listing process", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnAnswerCountsEvenWhenTheChildAbortsAfterwards()
    {
        var error = await Fails(Unreachable, () => FixtureChild("answer-then-abort"));

        Assert.Equal("Local: Timed out", error.Message);
    }

    [Fact]
    public async Task AChildThatAbortsBeforeAnsweringIsReportedAsStopped()
    {
        var error = await Fails(Unreachable, () => FixtureChild("abort"));

        Assert.StartsWith("the listing process stopped with exit code", error.Message, StringComparison.Ordinal);
        Assert.Contains("Consumer groups could not be read (the listing process", KafkaWorkspace.ConsumerGroupsUnreadable(error.Message), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AChildThatHangsIsStoppedAndReportedInTime()
    {
        // The deadline is the request timeout plus a grace period; the hung child is killed rather than left behind.
        var elapsed = Stopwatch.StartNew();

        var error = await Fails(Unreachable, () => FixtureChild("hang"), TimeSpan.FromSeconds(1));

        Assert.Equal("the listing process did not answer in time", error.Message);
        Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(60), $"took {elapsed.Elapsed}");
    }

    [Fact]
    public async Task AnAnswerThatCannotBeReadIsAReason()
    {
        var error = await Fails(Unreachable, () => FixtureChild("garbage"));

        Assert.Equal("the listing process gave an answer that could not be read", error.Message);
    }

    [Fact]
    public async Task AChildThatDiesIsAReasonNotACrash()
    {
        var error = await Fails(Unreachable, () => new ProcessStartInfo("dotnet", ["exec", Path.Combine(AppContext.BaseDirectory, "no-such-listing-child.dll")]));

        Assert.Contains("listing process stopped with exit code", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AChildThatCannotStartCostsOnlyTheLag()
    {
        // Process.Start throws Win32Exception for a missing executable; that must become the lag note too, not a failed
        // refresh that hides the topics.
        var missing = Path.Combine(AppContext.BaseDirectory, "no-such-folder", OperatingSystem.IsWindows() ? "QueueLoom.exe" : "QueueLoom");

        var error = await Fails(Unreachable, () => new ProcessStartInfo(missing, [KafkaGroupListing.HelperArgument]));

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

            var error = await Fails(InvalidSetting(), () => new ProcessStartInfo(executable, [KafkaGroupListing.HelperArgument]));

            // It started, read the request and answered (no COM failure on the STA thread, no start failure).
            Assert.Contains("no.such.property", error.Message, StringComparison.Ordinal);
        }
        finally
        {
            // The junction first, so deleting the folders cannot reach the built app it points at.
            if (Directory.Exists(Extended + app)) Directory.Delete(Extended + app);
            Directory.Delete(Extended + root, recursive: true);
        }
    }

    private const string Extended = @"\\?\";
}
