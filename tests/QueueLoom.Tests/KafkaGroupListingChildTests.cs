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

            AssertStartedAndListed(error);
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
    public async Task TheAppsListingModeEndsInAReasonNotACrash()
    {
        // The real executable in its listing mode: the request goes in through standard input, the broker's refusal
        // comes back as the reason. With Confluent.Kafka 2.15/2.16 that failed listing corrupts the child's heap
        // (free(): invalid pointer on Linux, 0xC0000374 on Windows); listed in this process, the same request aborted
        // the test host. The corruption races the answer, so the child either answers or is reported as stopped.
        var error = await Assert.ThrowsAsync<KafkaGroupListing.GroupListingException>(() => KafkaGroupListing.ListAsync(null, Unreachable,
            TimeSpan.FromSeconds(2), CancellationToken.None, () => AppChild(KafkaGroupListing.HelperArgument)));

        AssertStartedAndListed(error);
    }

    /// <summary>The child started and ran its listing: the broker's reason, or the corruption ended it afterwards.</summary>
    private static void AssertStartedAndListed(KafkaGroupListing.GroupListingException error) =>
        Assert.True(ShowsTheListingRan(error.Message), error.Message);

    /// <summary>
    /// The broker's own reason, or the child ended by the native heap corruption with no managed error line: SIGABRT
    /// (134) from malloc refusing the free in rd_kafka_ListConsumerGroupsResult_free, or, once the heap is corrupted,
    /// SIGSEGV (139) or SIGILL (132) wherever the runtime next touches it (macOS crash reports of a failed listing,
    /// https://github.com/bitcodepro/QueueLoom/actions/runs/39461410282); STATUS_HEAP_CORRUPTION on Windows. Anything
    /// else (no start, a clean or ordinary exit without an answer, a managed exception) does not show that the listing ran.
    /// </summary>
    internal static bool ShowsTheListingRan(string reason)
    {
        if (!reason.StartsWith("the listing process", StringComparison.Ordinal)) return true;
        int[] aborts = OperatingSystem.IsWindows() ? [unchecked((int)0xC0000374)] : [134, 139, 132];
        return aborts.Any(abort => reason == $"the listing process stopped with exit code {abort}"
               || reason.StartsWith($"the listing process stopped with exit code {abort}: free(", StringComparison.Ordinal)
               || reason.StartsWith($"the listing process stopped with exit code {abort}: double free", StringComparison.Ordinal)
               || reason.StartsWith($"the listing process stopped with exit code {abort}: malloc", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("Local: Timed out", true)]
    [InlineData("the listing process stopped with exit code 0", false)]
    [InlineData("the listing process stopped with exit code 1", false)]
    [InlineData("the listing process could not start (The system cannot find the file specified.)", false)]
    [InlineData("the listing process gave no answer", false)]
    [InlineData("the listing process gave an answer that could not be read", false)]
    [InlineData("the listing process did not answer in time", false)]
    public void OnlyTheBrokersReasonOrANativeAbortShowsTheListingRan(string reason, bool ran) =>
        Assert.Equal(ran, ShowsTheListingRan(reason));

    [Fact]
    public void ANativeAbortShowsTheListingRanButAManagedCrashDoesNot()
    {
        var abort = OperatingSystem.IsWindows() ? unchecked((int)0xC0000374) : 134;

        Assert.True(ShowsTheListingRan($"the listing process stopped with exit code {abort}"));
        Assert.True(ShowsTheListingRan($"the listing process stopped with exit code {abort}: free(): invalid pointer"));
        if (!OperatingSystem.IsWindows())
        {
            Assert.True(ShowsTheListingRan("the listing process stopped with exit code 139"));
            Assert.True(ShowsTheListingRan("the listing process stopped with exit code 132"));
            Assert.False(ShowsTheListingRan("the listing process stopped with exit code 137"));
        }
        Assert.False(ShowsTheListingRan($"the listing process stopped with exit code {abort}: Unhandled exception. System.Runtime.InteropServices.COMException (0x8007007A)"));
        Assert.False(ShowsTheListingRan("the listing process stopped with exit code -532462766: Unhandled exception. System.IO.IOException"));
    }
}
