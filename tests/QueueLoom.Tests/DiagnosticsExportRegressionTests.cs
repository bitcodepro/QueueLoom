using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using QueueLoom.App.Services;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class DiagnosticsExportRegressionTests
{
    [Fact]
    public async Task DistinctPreviews_RacingOnePath_HaveOneWinnerWithExactFrozenBytes()
    {
        using var directory = new TemporaryDirectory();
        var previews = new[] { Capture("Connecting"), Capture("Disconnecting") };
        Assert.NotEqual(previews[0].Report, previews[1].Report);
        for (var round = 0; round < 24; round++)
        {
            var destination = Path.Combine(directory.Path, $"race-{round}.zip");
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var saves = previews.Select(async preview =>
            {
                await start.Task;
                return await Record.ExceptionAsync(() => preview.SaveAsync(destination));
            }).ToArray();
            start.SetResult();
            var errors = await Task.WhenAll(saves);
            var winner = Assert.Single(Enumerable.Range(0, errors.Length), i => errors[i] is null);
            Assert.IsAssignableFrom<IOException>(errors[1 - winner]);
            await AssertArchive(destination, previews[winner].Report, previews[winner].Json);
        }
        Assert.Empty(Directory.GetFiles(directory.Path, "*.tmp"));
    }

    [Fact]
    public async Task SeparatePaths_ConcurrentExportsMatchTheirOwnFrozenPreviews()
    {
        using var directory = new TemporaryDirectory();
        var previews = new[] { Capture("Connecting"), Capture("Disconnecting") };
        var paths = new[] { Path.Combine(directory.Path, "first.zip"), Path.Combine(directory.Path, "second.zip") };
        await Task.WhenAll(previews.Select((preview, i) => preview.SaveAsync(paths[i])));
        for (var i = 0; i < paths.Length; i++) await AssertArchive(paths[i], previews[i].Report, previews[i].Json);
        Assert.Equal(2, Directory.GetFiles(directory.Path).Length);
    }

    [Fact]
    public async Task ExternalCreator_AtPublicationWinsWithoutBeingChangedOrDeleted()
    {
        using var directory = new TemporaryDirectory();
        var destination = Path.Combine(directory.Path, "created-by-another-actor.zip");
        var preview = Capture("Connecting");
        await Assert.ThrowsAnyAsync<IOException>(() => preview.SaveAsync(destination, default, (staging, full) =>
        {
            File.WriteAllText(full, "external creator's bytes");
            DiagnosticsPreview.PublishCreateOnly(staging, full);
        }));
        Assert.Equal("external creator's bytes", await File.ReadAllTextAsync(destination));
        Assert.Equal(new[] { destination }, Directory.GetFiles(directory.Path));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task PublicationFailureOrCancellation_RemovesOnlyItsStaging(bool cancel, bool foreignDestination)
    {
        using var directory = new TemporaryDirectory();
        var destination = Path.Combine(directory.Path, "failed.zip");
        var unrelated = Path.Combine(directory.Path, ".queueloom-diagnostics-foreign.tmp");
        await File.WriteAllTextAsync(unrelated, "keep this file");
        using var cancellation = new CancellationTokenSource();
        string? owned = null;
        var error = await Record.ExceptionAsync(() => Capture("Connecting").SaveAsync(destination,
            cancellation.Token, (staging, full) =>
            {
                owned = staging;
                Assert.True(File.Exists(staging));
                if (foreignDestination) File.WriteAllText(full, "foreign destination created during publication");
                if (cancel)
                {
                    cancellation.Cancel();
                    cancellation.Token.ThrowIfCancellationRequested();
                }
                throw new IOException("Injected publication failure.");
            }));
        if (cancel) Assert.IsAssignableFrom<OperationCanceledException>(error);
        else Assert.IsAssignableFrom<IOException>(error);
        Assert.NotNull(owned);
        Assert.False(File.Exists(owned));
        if (foreignDestination)
            Assert.Equal("foreign destination created during publication", await File.ReadAllTextAsync(destination));
        else Assert.False(File.Exists(destination));
        Assert.Equal("keep this file", await File.ReadAllTextAsync(unrelated));
        Assert.Equal((foreignDestination ? new[] { destination, unrelated } : new[] { unrelated }).Order(),
            Directory.GetFiles(directory.Path).Order());
    }

    [Fact]
    public async Task DestinationReplacedAfterPublication_IsNeverDeletedByFailureCleanup()
    {
        using var directory = new TemporaryDirectory();
        var destination = Path.Combine(directory.Path, "replaced.zip");
        var replacement = Path.Combine(directory.Path, "external.zip");
        var other = Capture("Disconnecting");
        await other.SaveAsync(replacement);
        await Assert.ThrowsAnyAsync<IOException>(() => Capture("Connecting").SaveAsync(destination, default, (staging, full) =>
        {
            DiagnosticsPreview.PublishCreateOnly(staging, full);
            File.Move(replacement, full, overwrite: true); // Another actor takes ownership of the name.
            throw new IOException("Injected failure after external replacement.");
        }));
        await AssertArchive(destination, other.Report, other.Json);
        Assert.Equal(new[] { destination }, Directory.GetFiles(directory.Path));
    }

    [Fact]
    public async Task CancellationAfterPublication_DoesNotRetractACompletedZip()
    {
        using var directory = new TemporaryDirectory();
        var destination = Path.Combine(directory.Path, "committed.zip");
        var preview = Capture("Connecting");
        using var cancellation = new CancellationTokenSource();
        await preview.SaveAsync(destination, cancellation.Token, (staging, full) =>
        {
            DiagnosticsPreview.PublishCreateOnly(staging, full);
            cancellation.Cancel();
        });
        Assert.True(cancellation.IsCancellationRequested);
        await AssertArchive(destination, preview.Report, preview.Json);
        Assert.Empty(Directory.GetFiles(directory.Path, "*.tmp"));
    }

    [Fact]
    public async Task UnixExistingDanglingSymlink_IsNotFollowedReplacedOrDeleted()
    {
        if (OperatingSystem.IsWindows()) return;
        using var directory = new TemporaryDirectory();
        var destination = Path.Combine(directory.Path, "symlink.zip");
        var missing = Path.Combine(directory.Path, "missing.zip");
        File.CreateSymbolicLink(destination, missing);
        await Assert.ThrowsAnyAsync<IOException>(() => Capture("Connecting").SaveAsync(destination));
        Assert.Equal(missing, new FileInfo(destination).LinkTarget);
        Assert.False(File.Exists(missing));
        Assert.Empty(Directory.GetFiles(directory.Path, "*.tmp"));
    }

    [Fact]
    public async Task UnicodeDestination_PreservesFrozenPreviewBytes()
    {
        using var directory = new TemporaryDirectory();
        var destination = Path.Combine(directory.Path, "diagnostics-\u00e9-\u961f\u5217.zip");
        var preview = Capture("Connecting");
        await preview.SaveAsync(destination);
        await AssertArchive(destination, preview.Report, preview.Json);
        Assert.Single(Directory.GetFiles(directory.Path));
    }

    [Fact]
    public async Task UnixStagingCleanupFailure_ReportsFailureButLeavesTheCompletePublishedZip()
    {
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;
        using var directory = new TemporaryDirectory();
        var originalMode = File.GetUnixFileMode(directory.Path);
        var destination = Path.Combine(directory.Path, "committed-before-cleanup-error.zip");
        var preview = Capture("Connecting");
        string? stagingPath = null;
        try
        {
            await Assert.ThrowsAsync<UnauthorizedAccessException>(() => preview.SaveAsync(destination, default, (staging, full) =>
            {
                stagingPath = staging;
                DiagnosticsPreview.PublishCreateOnly(staging, full);
                if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
                    File.SetUnixFileMode(directory.Path, UnixFileMode.UserRead | UnixFileMode.UserExecute);
            }));
            await AssertArchive(destination, preview.Report, preview.Json);
            Assert.NotNull(stagingPath);
            Assert.True(File.Exists(stagingPath));
        }
        finally { File.SetUnixFileMode(directory.Path, originalMode); }
    }

    [Fact]
    public Task IndependentProcesses_RacingOnePath_PreserveTheSuccessfulPreview() => RaceProcesses(interposeRename: false);

    [Fact]
    public async Task UnixCheckThenRename_ConcurrentDistinctExports_CannotBothSucceed()
    {
        // Linux CI supplies cc and LD_PRELOAD. Other platforms run the real process race above.
        if (!OperatingSystem.IsLinux()) return;
        await RaceProcesses(interposeRename: true);
    }

    private static async Task RaceProcesses(bool interposeRename)
    {
        using var directory = new TemporaryDirectory();
        var destination = Path.Combine(directory.Path, "collision.zip");
        var release = Path.Combine(directory.Path, "start");
        var renameRelease = Path.Combine(directory.Path, "rename-release");
        var prefixes = new[] { Path.Combine(directory.Path, "first"), Path.Combine(directory.Path, "second") };
        var names = new[] { "first", "second" };
        var interposer = Path.Combine(directory.Path, "rename-barrier.so");
        if (interposeRename)
        {
            var compile = new ProcessStartInfo("cc") { UseShellExecute = false, RedirectStandardError = true };
            foreach (var argument in new[] { "-shared", "-fPIC", "-o", interposer,
                         Path.Combine(AppContext.BaseDirectory, "Fixtures", "diagnostics-rename-barrier.c"), "-ldl" })
                compile.ArgumentList.Add(argument);
            using var compiler = Process.Start(compile)!;
            var errors = compiler.StandardError.ReadToEndAsync();
            await compiler.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
            Assert.True(compiler.ExitCode == 0, await errors);
        }
        var processes = new List<Process>();
        try
        {
            for (var i = 0; i < names.Length; i++)
            {
                var fixture = Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture" +
                    (OperatingSystem.IsWindows() ? ".exe" : string.Empty));
                var start = new ProcessStartInfo(fixture) { UseShellExecute = false, WindowStyle = ProcessWindowStyle.Hidden };
                foreach (var argument in new[] { "--export-diagnostics", destination, prefixes[i], names[i], release })
                    start.ArgumentList.Add(argument);
                if (interposeRename)
                {
                    start.Environment["LD_PRELOAD"] = interposer;
                    start.Environment["QUEUELOOM_EXPORT_RACE_DESTINATION"] = destination;
                    start.Environment["QUEUELOOM_EXPORT_RENAME_READY"] = prefixes[i] + ".rename";
                    start.Environment["QUEUELOOM_EXPORT_RENAME_RELEASE"] = renameRelease;
                }
                processes.Add(Process.Start(start)!);
            }
            await WaitFor(() => prefixes.All(p => File.Exists(p + ".ready")));
            await File.WriteAllTextAsync(release, "start");
            if (interposeRename)
            {
                await WaitFor(() => prefixes.All(p => File.Exists(p + ".rename")) || processes.All(p => p.HasExited));
                await File.WriteAllTextAsync(renameRelease, "publish");
            }
            await Task.WhenAll(processes.Select(p => p.WaitForExitAsync())).WaitAsync(TimeSpan.FromSeconds(20));
            Assert.All(processes, p => Assert.Equal(0, p.ExitCode));
            var outcomes = await Task.WhenAll(prefixes.Select(p => File.ReadAllTextAsync(p + ".result")));
            using var archive = ZipFile.OpenRead(destination);
            using var reader = new StreamReader(archive.GetEntry("report.txt")!.Open());
            var actualReport = await reader.ReadToEndAsync();
            Assert.True(outcomes.Count(o => o == "saved") == 1,
                $"Distinct exporters returned [{string.Join(", ", outcomes)}]; destination retained '{actualReport.Trim()}', " +
                "so another successful frozen preview was overwritten.");
            var winner = Array.IndexOf(outcomes, "saved");
            Assert.Equal("collision", outcomes[1 - winner]);
            await AssertArchive(destination, $"Report from {names[winner]}\n", $"{{\"export\":\"{names[winner]}\"}}");
            Assert.Empty(Directory.GetFiles(directory.Path, "*.tmp"));
        }
        finally
        {
            foreach (var process in processes)
            {
                if (!process.HasExited) { process.Kill(); await process.WaitForExitAsync(); }
                process.Dispose();
            }
        }
    }

    private static async Task WaitFor(Func<bool> ready)
    {
        var clock = Stopwatch.StartNew();
        while (!ready())
        {
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(15), "Export process barrier timed out.");
            await Task.Delay(5);
        }
    }

    private static DiagnosticsPreview Capture(string kind)
    {
        var journal = new DiagnosticsJournal();
        journal.Begin(kind);
        var preview = journal.Capture();
        journal.Begin("Monitor"); // This later change must never appear in the saved ZIP.
        return preview;
    }

    private static async Task AssertArchive(string path, string report, string json)
    {
        using var archive = ZipFile.OpenRead(path);
        Assert.Equal(new[] { "report.txt", "diagnostics.json" }, archive.Entries.Select(e => e.FullName));
        foreach (var entry in archive.Entries)
        {
            await using var stream = entry.Open();
            using var bytes = new MemoryStream();
            await stream.CopyToAsync(bytes);
            Assert.Equal(Encoding.UTF8.GetBytes(entry.Name == "report.txt" ? report : json), bytes.ToArray());
        }
    }
}
