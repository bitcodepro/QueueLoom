using System.Diagnostics;
using System.Text.Json;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class WindowsUpdateSmokeTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(7)]
    public async Task FirstProcessCleanupFailureStillSweepsTheOwnedGuiAndDeletesTheInstallation(int helperExit)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("The packaged GUI smoke script requires Windows."); return; }
        LogSmokeOutput("Test base directory: " + AppContext.BaseDirectory);
        using var fixture = new StableLauncherTests.InstallationFixture();
        using var directory = new TemporaryDirectory();
        var gate = Path.Combine(directory.Path, "helper exit gate");
        var start = SmokeStart(fixture, directory, true);
        start.Environment["QUEUELOOM_SMOKE_FIXTURE_GATE"] = gate;
        start.Environment["QUEUELOOM_SMOKE_DISPOSE_PARENT"] = "1";
        using var script = StartPowerShell(start);
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var output = ReadOutput(script, waiting);
        var error = script.StandardError.ReadToEndAsync();
        var exited = script.WaitForExitAsync();
        Process? ownedGui = null;
        string? ownedRoot = null;
        string? ownedExecutable = null;
        var ownedProcesses = new List<Process>();
        var primaryFailure = false;
        try
        {
            await WaitForPublishedSmokeMarker(gate + ".helper.json", exited);
            var observed = await Task.WhenAny(waiting.Task, exited).WaitAsync(TimeSpan.FromSeconds(60));
            Assert.True(observed == waiting.Task, await CompletedOutput(script, output, error));
            using var marker = JsonDocument.Parse(ReadPublishedSmokeFile(gate + ".helper.json"));
            using var helper = Process.GetProcessById(marker.RootElement.GetProperty("Pid").GetInt32());
            _ = helper.Handle;
            Assert.Equal(marker.RootElement.GetProperty("StartTicks").GetInt64(), helper.StartTime.ToUniversalTime().Ticks);
            var executable = marker.RootElement.GetProperty("Executable").GetString()!;
            Assert.Equal(executable, helper.MainModule?.FileName);
            ownedRoot = ValidateSmokeRoot(Path.GetDirectoryName(marker.RootElement.GetProperty("Receipt").GetString())!);
            Assert.StartsWith(ownedRoot + Path.DirectorySeparatorChar, executable, StringComparison.OrdinalIgnoreCase);
            ownedExecutable = executable;
            using var disposed = JsonDocument.Parse(ReadPublishedSmokeFile(gate + ".disposed-parent.json"));
            Assert.Equal(ownedRoot, disposed.RootElement.GetProperty("Root").GetString());
            LogSmokeOutput($"Disposed parent observer PID {disposed.RootElement.GetProperty("Pid").GetInt32()}, start ticks {disposed.RootElement.GetProperty("StartTicks").GetInt64()}.");
            var candidate = Process.GetProcessById(int.Parse(ReadPublishedSmokeFile(gate + ".gui.pid"), System.Globalization.CultureInfo.InvariantCulture));
            try
            {
                _ = candidate.Handle;
                Assert.Equal(executable, candidate.MainModule?.FileName);
                Assert.True(candidate.StartTime.ToUniversalTime() >= script.StartTime.ToUniversalTime());
                ownedGui = candidate;
                candidate = null;
            }
            finally { candidate?.Dispose(); }
            ownedProcesses = CaptureOwnedSmokeProcesses(ownedRoot, ownedExecutable, script.StartTime.ToUniversalTime());
            Assert.Contains(ownedProcesses, process => process.Id == ownedGui.Id);
            PublishRelease(gate + ".release", helperExit);
            await exited.WaitAsync(TimeSpan.FromSeconds(45 + 10));
            await helper.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            LogSmokeOutput($"Helper PID {helper.Id}, observed exit {helper.ExitCode}; observer exit {script.ExitCode}; GUI exited {ownedGui.HasExited}; installation exists {Directory.Exists(ownedRoot)}.");
            Assert.Equal(helperExit, helper.ExitCode);
            Assert.NotEqual(0, script.ExitCode);
            Assert.True(ownedGui.HasExited, "The GUI sweep was skipped after the earlier process cleanup failed.");
            Assert.False(Directory.Exists(ownedRoot), "Installation deletion was skipped after the earlier process cleanup failed.");
            var detail = await output + await error;
            Assert.DoesNotContain("PASS packaged Windows update", detail, StringComparison.Ordinal);
            if (helperExit == 7) Assert.Contains("Packaged restart helper failed (7)", detail, StringComparison.Ordinal);
            else Assert.DoesNotContain("Packaged restart helper failed", detail, StringComparison.Ordinal);
        }
        catch { primaryFailure = true; throw; }
        finally
        {
            try
            {
                if (!File.Exists(gate + ".release")) PublishRelease(gate + ".release", 0);
                try { await exited.WaitAsync(TimeSpan.FromSeconds(90)); }
                catch (TimeoutException) { if (!script.HasExited) script.Kill(entireProcessTree: true); await exited; }
                LogSmokeOutput($"Observer final exit: {script.ExitCode}.");
                // These baseline/control resources are registered only after proving exact fixture ownership.
                // Stop both entry and payload before awaiting pipe EOF: a leaked child inherits the observer's pipe.
                await StopOwnedSmokeProcesses(ownedProcesses);
                ownedGui?.Dispose();
                LogSmokeOutput("Observer stdout:\n" + await output + "\nObserver stderr:\n" + await error);
                if (ownedRoot is not null && Directory.Exists(ownedRoot)) Directory.Delete(ValidateSmokeRoot(ownedRoot), recursive: true);
            }
            catch (Exception cleanup) when (primaryFailure) { LogSmokeOutput("Test-owned cleanup also failed: " + cleanup); }
        }
    }

    private static List<Process> CaptureOwnedSmokeProcesses(string root, string payload, DateTime observerStart)
    {
        var entry = Path.Combine(ValidateSmokeRoot(root), "QueueLoom.exe");
        var owned = new List<Process>();
        foreach (var process in Process.GetProcessesByName("QueueLoom"))
        {
            var retained = false;
            try
            {
                _ = process.Handle;
                if (process.HasExited) continue;
                string? path;
                try { path = process.MainModule?.FileName; }
                catch (System.ComponentModel.Win32Exception) { continue; } // Unproven ownership: never kill.
                if ((!string.Equals(path, entry, StringComparison.OrdinalIgnoreCase) &&
                     !string.Equals(path, payload, StringComparison.OrdinalIgnoreCase)) ||
                    process.StartTime.ToUniversalTime() < observerStart) continue;
                owned.Add(process);
                retained = true;
            }
            catch (System.ComponentModel.Win32Exception) { } // Unproven ownership: never kill.
            catch (InvalidOperationException) { }
            finally { if (!retained) process.Dispose(); }
        }
        return owned;
    }

    private static async Task StopOwnedSmokeProcesses(List<Process> processes)
    {
        Exception? firstFailure = null;
        foreach (var process in processes)
        {
            try
            {
                if (!process.HasExited) process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (InvalidOperationException) when (process.HasExited) { }
            catch (Exception failure) { firstFailure ??= failure; }
            finally { process.Dispose(); }
        }
        if (firstFailure is not null) throw firstFailure;
    }
}
