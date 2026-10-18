using System.Diagnostics;
using System.Text.Json;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class UpdateRestartHandoffTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData("released", 0)]
    [InlineData("read", 0)]
    [InlineData("delete", 0)]
    [InlineData("write", -532462766)]
    [InlineData("wrong-pid", -532462766)]
    [InlineData("malformed-pid", -532462766)]
    [InlineData("helper-exit", -532462766)]
    public async Task ActualHandoffRequiresReadableCompleteOwnedIdentity(string mode, int expectedSenderExit)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows handle sharing requires Windows."); return; }
        var directory = new TemporaryDirectory();
        var root = directory.Path;
        foreach (var file in Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "UpdateFixture")))
            File.Copy(file, Path.Combine(root, Path.GetFileName(file)));
        // Keep these concurrently running children out of the existing packaged-smoke
        // tests' machine-wide QueueLoom process-name enumeration.
        var exe = Path.Combine(root, "QueueLoom.HandoffFixture.exe");
        File.Copy(Path.Combine(root, "QueueLoom.UpdateFixture.exe"), exe);
        var receipt = Path.Combine(root, ".queueloom-update.json");
        File.WriteAllText(receipt, JsonSerializer.Serialize(new { Id = Guid.NewGuid().ToString("N"),
            Target = new { Rid = "win-x64", InstallDirectory = root, Executable = exe, Bundle = (string?)null },
            DownloadDirectory = (string?)null, Entries = new[] { new { Current = exe, Backup = (string?)null } } }));
        var suffix = Guid.NewGuid().ToString("N");
        var readyName = "Local\\QueueLoomHandoffReady-" + suffix;
        var releaseName = "Local\\QueueLoomHandoffRelease-" + suffix;
        using var ready = new EventWaitHandle(false, EventResetMode.ManualReset, readyName);
        using var release = new EventWaitHandle(false, EventResetMode.ManualReset, releaseName);
        var claimed = Path.Combine(root, "claimed");
        var handoff = Path.Combine(root, "sender.lock");
        var start = new ProcessStartInfo(exe) { UseShellExecute = false, CreateNoWindow = true,
            WorkingDirectory = root, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var arg in new[] { "--legacy-handoff", exe, handoff, "0", claimed }) start.ArgumentList.Add(arg);
        start.Environment["QL_PROBE_ROOT"] = root; start.Environment["QL_PROBE_MODE"] = mode;
        start.Environment["QL_PROBE_READY"] = readyName; start.Environment["QL_PROBE_RELEASE"] = releaseName;
        start.Environment.Remove("QUEUELOOM_SMOKE_FIXTURE_GATE");
        var elapsed = Stopwatch.StartNew();
        using var sender = Process.Start(start)!; _ = sender.Handle;
        var stdout = sender.StandardOutput.ReadToEndAsync();
        var stderr = sender.StandardError.ReadToEndAsync();
        Process? helper = null;
        Process? gui = null;
        var primaryFailure = false;
        try
        {
            Assert.True(ready.WaitOne(TimeSpan.FromSeconds(30)), "The actual writer did not reach BeforePublish.");
            var holder = ReadJson(Path.Combine(root, "holder.json"));
            helper = Adopt(holder, exe);
            Log($"Sender PID {sender.Id}, helper/holder PID {helper.Id}, start ticks {helper.StartTime.ToUniversalTime().Ticks}, mode {mode}, access {holder.GetProperty("RequestedAccess")}, share {holder.GetProperty("Share")}, handle {holder.GetProperty("Handle")}.");
            if (mode == "helper-exit")
            {
                Assert.False(File.Exists(receipt + ".helper"));
                helper.Kill(entireProcessTree: true);
                await helper.WaitForExitAsync();
            }
            if (expectedSenderExit == 0)
            {
                await Published(handoff + ".helper.json", sender.WaitForExitAsync());
                var accepted = ReadJson(handoff + ".helper.json");
                Assert.Equal(helper.Id, accepted.GetProperty("Pid").GetInt32());
                Assert.Equal(helper.StartTime.ToUniversalTime().Ticks, accepted.GetProperty("StartTicks").GetInt64());
                File.WriteAllText(claimed, "caller accepted exact owned identity");
            }
            await SenderExit(sender, helper, root, receipt, handoff, elapsed);
            Assert.Equal(expectedSenderExit, sender.ExitCode);
            if (expectedSenderExit == 0)
            {
                await Published(Path.Combine(root, "helper-finished.json"), helper.WaitForExitAsync());
                Assert.Equal(0, ReadJson(Path.Combine(root, "helper-finished.json")).GetProperty("Code").GetInt32());
                await Published(Path.Combine(root, "gui.json"), helper.WaitForExitAsync());
                // The started application is a fixture that exits after about a second: on a slow runner it can be gone
                // before it is adopted, which is a finished start, not a failed one.
                gui = AdoptIfRunning(ReadJson(Path.Combine(root, "gui.json")), exe);
                Assert.False(File.Exists(receipt));
            }
            else
            {
                Assert.False(File.Exists(handoff + ".helper.json"));
                var error = ReadJson(Path.Combine(root, "reader-error.json"));
                Log("Actual caller error: " + error);
                Assert.Equal("System.IO.IOException", error.GetProperty("Type").GetString());
                Assert.Contains(mode == "helper-exit" ? "exited before accepting the handoff" : "did not accept the handoff",
                    error.GetProperty("Message").GetString(), StringComparison.Ordinal);
                Assert.Equal("0x80131620", error.GetProperty("HResult").GetString());
                if (mode != "helper-exit") Assert.True(elapsed.Elapsed >= TimeSpan.FromSeconds(10), "The original handoff deadline must remain in force.");
                Assert.True(File.Exists(receipt));
            }
            Log($"Actual sender exit {sender.ExitCode}; receipt {File.Exists(receipt)}; acknowledgement {File.Exists(receipt + ".helper")}; accepted identity {File.Exists(handoff + ".helper.json")}.");
        }
        catch { primaryFailure = true; throw; }
        finally
        {
            var cleanupFailures = new List<Exception>();
            async Task Cleanup(Func<Task> action)
            {
                try { await action(); }
                catch (Exception failure) { cleanupFailures.Add(failure); Log("Test-owned cleanup failed: " + failure); }
            }
            await Cleanup(() => { release.Set(); return Task.CompletedTask; });
            await Cleanup(async () =>
            {
                if (helper is not null)
                {
                    try { await helper.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5)); }
                    catch (TimeoutException) { helper.Kill(entireProcessTree: true); await helper.WaitForExitAsync(); }
                }
            });
            await Cleanup(async () =>
            {
                if (gui is null && File.Exists(Path.Combine(root, "gui.json"))) gui = AdoptIfRunning(ReadJson(Path.Combine(root, "gui.json")), exe);
                if (gui is not null)
                {
                    // The harmless startup fixture exits normally after one second. Observe its
                    // exit before removing its mapped binaries instead of terminating mid-startup.
                    try { await gui.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5)); }
                    catch (TimeoutException) { gui.Kill(); await gui.WaitForExitAsync(); }
                    finally { gui.Dispose(); }
                }
            });
            await Cleanup(async () =>
            {
                if (!sender.HasExited) { sender.Kill(entireProcessTree: true); await sender.WaitForExitAsync(); }
            });
            await Cleanup(() => { helper?.Dispose(); sender.Dispose(); return Task.CompletedTask; });
            await Cleanup(async () =>
            {
                Log("Sender stdout:\n" + await stdout + "\nSender stderr:\n" + await stderr);
            });
            await Cleanup(() => { directory.Dispose(); return Task.CompletedTask; });
            if (!primaryFailure && cleanupFailures.Count > 0)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(cleanupFailures[0]).Throw();
        }
    }

    private void Log(string value) => output.WriteLine(SensitiveDataRedactor.Redact(value));

    /// <summary>
    /// Waits (bounded) for the sender to exit. When it has not, the failure says, before any teardown, how long the case
    /// has run, whether the sender and helper are alive, which handoff files exist, and what error the sender recorded and
    /// when, so a slow exit is told apart from a sender that never reached its error.
    /// </summary>
    private static async Task SenderExit(Process sender, Process? helper, string root, string receipt, string handoff, Stopwatch elapsed)
    {
        try
        {
            await sender.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(30));
        }
        catch (TimeoutException)
        {
            var errorFile = Path.Combine(root, "reader-error.json");
            string recorded = "none";
            if (File.Exists(errorFile))
            {
                var error = ReadJson(errorFile);
                recorded = error.ToString();
                if (error.TryGetProperty("FailedAtUtcTicks", out var at))
                    recorded += $" (recorded {(DateTime.UtcNow - new DateTime(at.GetInt64(), DateTimeKind.Utc)).TotalSeconds:F1} s ago)";
            }
            bool Alive(Process? process) { try { return process is not null && !process.HasExited; } catch (InvalidOperationException) { return false; } }
            throw new Xunit.Sdk.XunitException(
                $"The sender (PID {sender.Id}) had not exited {elapsed.Elapsed.TotalSeconds:F1} s into the case. Sender alive: {Alive(sender)}; " +
                $"helper alive: {Alive(helper)}; acknowledgement: {File.Exists(receipt + ".helper")}; accepted identity: " +
                $"{File.Exists(handoff + ".helper.json")}; receipt: {File.Exists(receipt)}; sender error: {recorded}.");
        }
    }
    private static JsonElement ReadJson(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
        using var reader = new StreamReader(stream);
        using var document = JsonDocument.Parse(reader.ReadToEnd());
        return document.RootElement.Clone();
    }
    /// <summary>The identified process, or null when it has already exited (its PID is free or now names another start).</summary>
    private static Process? AdoptIfRunning(JsonElement identity, string executable)
    {
        Process process;
        try { process = Process.GetProcessById(identity.GetProperty("Pid").GetInt32()); }
        catch (ArgumentException) { return null; }
        try
        {
            _ = process.Handle;
            if (process.HasExited || identity.GetProperty("StartTicks").GetInt64() != process.StartTime.ToUniversalTime().Ticks)
            {
                process.Dispose();
                return null;
            }
        }
        catch (InvalidOperationException)
        {
            // It exited between the lookup and the checks.
            process.Dispose();
            return null;
        }
        // A running process is kept even when its module cannot be read (it may be starting or ending): the test waits
        // for it to exit before its files are removed. When the module can be read, it must be the fixture.
        string? module = null;
        try { module = process.MainModule?.FileName; }
        catch (Exception exception) when (exception is System.ComponentModel.Win32Exception or InvalidOperationException) { }
        if (module is not null)
        {
            try { Assert.Equal(executable, module); }
            catch { process.Dispose(); throw; }
        }
        return process;
    }

    private static Process Adopt(JsonElement identity, string executable)
    {
        var process = Process.GetProcessById(identity.GetProperty("Pid").GetInt32());
        try
        {
            _ = process.Handle;
            Assert.Equal(identity.GetProperty("StartTicks").GetInt64(), process.StartTime.ToUniversalTime().Ticks);
            Assert.Equal(executable, process.MainModule?.FileName);
            return process;
        }
        catch { process.Dispose(); throw; }
    }
    private static async Task Published(string path, Task exited)
    {
        var published = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var watcher = new FileSystemWatcher(Path.GetDirectoryName(path)!, Path.GetFileName(path));
        watcher.Created += (_, _) => published.TrySetResult(); watcher.Renamed += (_, _) => published.TrySetResult();
        watcher.Error += (_, error) => published.TrySetException(error.GetException());
        watcher.EnableRaisingEvents = true;
        if (File.Exists(path)) published.TrySetResult();
        var first = await Task.WhenAny(published.Task, exited).WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(first == published.Task, "The process exited before publishing " + Path.GetFileName(path));
        await published.Task;
    }
}
