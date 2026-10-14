using System.Diagnostics;
using System.Text.Json;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class WindowsUpdateSmokeTests
{
    [Theory]
    [InlineData("diagnostic", 7)]
    [InlineData("cleanup", 7)]
    [InlineData("cleanup", 0)]
    public async Task SecondaryObserverErrorsPreserveHelperFailureAndCleanupOnlyStillFails(string obstruction, int helperExit)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("The packaged GUI smoke script requires Windows."); return; }
        using var fixture = new StableLauncherTests.InstallationFixture();
        using var directory = new TemporaryDirectory();
        var gate = Path.Combine(directory.Path, "helper exit gate");
        var start = SmokeStart(fixture, directory, false);
        start.Environment["QUEUELOOM_SMOKE_FIXTURE_GATE"] = gate;
        using var script = StartPowerShell(start);
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var output = ReadOutput(script, waiting);
        var error = script.StandardError.ReadToEndAsync();
        var exited = script.WaitForExitAsync();
        FileStream? heldFile = null;
        Process? retainedHelper = null;
        string? ownedRoot = null;
        try
        {
            // The real observer announces this barrier only after retaining its verified helper.
            var observed = await Task.WhenAny(waiting.Task, exited).WaitAsync(TimeSpan.FromSeconds(40));
            Assert.True(observed == waiting.Task, await CompletedOutput(script, output, error));
            await WaitForPublishedSmokeMarker(gate + ".helper.json", exited);
            using var marker = JsonDocument.Parse(ReadPublishedSmokeFile(gate + ".helper.json"));
            var helper = retainedHelper = Process.GetProcessById(marker.RootElement.GetProperty("Pid").GetInt32());
            _ = helper.Handle;
            Assert.Equal(marker.RootElement.GetProperty("StartTicks").GetInt64(), helper.StartTime.ToUniversalTime().Ticks);
            Assert.Equal(marker.RootElement.GetProperty("Executable").GetString(), helper.MainModule?.FileName);
            Assert.False(helper.HasExited);
            var receipt = marker.RootElement.GetProperty("Receipt").GetString()!;
            ownedRoot = ValidateSmokeRoot(Path.GetDirectoryName(receipt)!);
            var lockedPath = obstruction == "diagnostic" ? receipt + ".error" : Path.Combine(ownedRoot, "test-owned-cleanup.lock");
            // Deny reads or deletion before releasing the helper; hold until the observer exits.
            heldFile = new FileStream(lockedPath, FileMode.CreateNew, FileAccess.ReadWrite,
                obstruction == "diagnostic" ? FileShare.Delete : FileShare.ReadWrite);
            PublishRelease(gate + ".release", helperExit);
            await helper.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            LogSmokeOutput($"Requested helper exit: {helperExit}; observed helper exit: {helper.ExitCode}.");
            Assert.Equal(helperExit, helper.ExitCode);
            await exited.WaitAsync(TimeSpan.FromSeconds(30));
            LogSmokeOutput($"Observer exit: {script.ExitCode}; obstruction: {obstruction}.");
            var detail = await output + await error;
            Assert.NotEqual(0, script.ExitCode);
            if (helperExit != 0)
            {
                Assert.Contains("Packaged restart helper failed (7)", detail, StringComparison.Ordinal);
                Assert.Contains(obstruction == "diagnostic" ? "Diagnostic detail unavailable" : "Smoke cleanup also failed", detail, StringComparison.Ordinal);
            }
            else Assert.DoesNotContain("Packaged restart helper failed", detail, StringComparison.Ordinal);
            Assert.DoesNotContain("PASS packaged Windows update", detail, StringComparison.Ordinal);
        }
        finally
        {
            // Release only the fixture gate, retaining the obstruction through normal script teardown.
            if (!File.Exists(gate + ".release")) PublishRelease(gate + ".release", 0);
            try { await exited.WaitAsync(TimeSpan.FromSeconds(30)); }
            catch (TimeoutException) { if (!script.HasExited) script.Kill(entireProcessTree: true); await exited; }
            heldFile?.Dispose();
            await Task.WhenAll(output, error);
            LogSmokeOutput($"Observer final exit: {script.ExitCode}.");
            if (retainedHelper is not null)
            {
                LogSmokeOutput(retainedHelper.HasExited ? $"Retained helper PID {retainedHelper.Id}, final exit: {retainedHelper.ExitCode}." :
                    $"Retained helper PID {retainedHelper.Id} is still running after observer teardown.");
                retainedHelper.Dispose();
            }
            LogSmokeOutput("Observer stdout:\n" + await output + "\nObserver stderr:\n" + await error);
            if (ownedRoot is not null && Directory.Exists(ownedRoot)) Directory.Delete(ValidateSmokeRoot(ownedRoot), recursive: true);
        }
    }

    private static async Task WaitForPublishedSmokeMarker(string path, Task exited)
    {
        var published = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var watcher = new FileSystemWatcher(Path.GetDirectoryName(path)!, Path.GetFileName(path));
        watcher.Created += (_, _) => published.TrySetResult();
        watcher.Renamed += (_, _) => published.TrySetResult();
        watcher.Error += (_, args) => published.TrySetException(args.GetException());
        watcher.EnableRaisingEvents = true;
        if (File.Exists(path)) published.TrySetResult();
        // Follow the actual observer's lifetime. Its existing startup/sender/helper/cleanup
        // phase limits are 15/45/45/10 seconds; a separate 30-second marker limit can preempt it.
        var observed = await Task.WhenAny(published.Task, exited).WaitAsync(TimeSpan.FromSeconds(15 + 45 + 45 + 10));
        Assert.True(observed == published.Task, "The observer exited before its helper reached the fixture gate.");
        await published.Task;
    }

    private static string ValidateSmokeRoot(string root)
    {
        var resolved = Path.GetFullPath(root);
        var temporary = Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.GetTempPath())) + Path.DirectorySeparatorChar;
        const string prefix = "QueueLoom update smoke with spaces ";
        var name = Path.GetFileName(resolved);
        if (!resolved.StartsWith(temporary, StringComparison.OrdinalIgnoreCase) ||
            !name.StartsWith(prefix, StringComparison.Ordinal) || !Guid.TryParseExact(name[prefix.Length..], "N", out _))
            throw new IOException("Unexpected smoke fixture cleanup path.");
        return resolved;
    }
}
