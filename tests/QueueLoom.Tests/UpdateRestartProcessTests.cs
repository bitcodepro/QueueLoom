using System.Diagnostics;
using System.Text.Json;
using QueueLoom.App.Services;

namespace QueueLoom.Tests;

/// <summary>Real processes, isolated installations and real Windows file locks; never application storage.</summary>
public sealed class UpdateRestartProcessTests : IAsyncDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "update process with spaces " + Guid.NewGuid().ToString("N"));
    private readonly List<Process> _processes = [];
    // Helper runs a test started without awaiting them to the end; cleanup awaits them before anything else.
    private readonly List<Task> _helpers = [];
    private string Fixture => Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture" + (OperatingSystem.IsWindows() ? ".exe" : ""));

    public UpdateRestartProcessTests() => Directory.CreateDirectory(_root);

    [Fact]
    public async Task Restart_WaitsForParentExit_ThenLaunchesAndCleansOwnedFiles()
    {
        var target = Installation();
        var parent = Start(target.Executable, "--hold-lock", Path.Combine(_root, "storage.lock"), "1500");
        await WaitFor(Path.Combine(_root, "storage.lock.held"));
        var staging = Path.Combine(_root, "staging");
        Directory.CreateDirectory(staging);
        File.Copy(Fixture, Path.Combine(staging, Path.GetFileName(target.Executable)));
        AppUpdater.Install(target, staging);
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(UpdateRestart.ReceiptPath(target)))!;
        File.WriteAllText(Path.Combine(_root, "user-notes.old"), "keep");

        // Production StartInstalled uses this process as its parent, so test the same protocol with our disposable parent.
        var parentTicks = parent.StartTime.ToUniversalTime().Ticks;
        // Explicitly exercise reconstructed birth-time differences on Unix instead of relying on clock jitter.
        if (!OperatingSystem.IsWindows()) parentTicks += TimeSpan.TicksPerSecond;
        var helper = Start(target.Executable, "--update-helper", UpdateRestart.ReceiptPath(target), receipt.Id,
            parent.Id.ToString(System.Globalization.CultureInfo.InvariantCulture), parentTicks.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Task.Delay(250);
        Assert.False(File.Exists(Path.Combine(_root, "updated-started.txt")));
        Assert.True(File.Exists(receipt.Entries.Single().Backup));
        await helper.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));

        Assert.Equal(0, helper.ExitCode);
        Assert.True(File.Exists(Path.Combine(_root, "updated-started.txt")));
        Assert.False(File.Exists(receipt.Entries.Single().Backup));
        Assert.False(File.Exists(UpdateRestart.ReceiptPath(target)));
        Assert.Equal("keep", File.ReadAllText(Path.Combine(_root, "user-notes.old")));
    }

    [Fact]
    public async Task ParentExitTimeout_PreservesBothVersions_AndDoesNotKillTheParent()
    {
        var target = Installation();
        var parent = Start(target.Executable, "--hold-lock", Path.Combine(_root, "storage.lock"), "1500");
        await WaitFor(Path.Combine(_root, "storage.lock.held"));
        var staging = Path.Combine(_root, "staging");
        Directory.CreateDirectory(staging);
        File.Copy(Fixture, Path.Combine(staging, Path.GetFileName(target.Executable)));
        AppUpdater.Install(target, staging);
        var path = UpdateRestart.ReceiptPath(target);
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
        var result = await UpdateRestart.RunAsync(path, receipt.Id, parent.Id, parent.StartTime.ToUniversalTime().Ticks,
            exitTimeout: TimeSpan.FromMilliseconds(100));
        Assert.Equal(1, result);
        Assert.False(parent.HasExited);
        Assert.True(File.Exists(target.Executable));
        Assert.True(File.Exists(receipt.Entries.Single().Backup));
        Assert.False(File.Exists(Path.Combine(_root, "updated-started.txt")));
        Assert.Contains("Waiting", File.ReadAllText(path + ".error"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ProductionRestart_DoesNotLaunchWhileTheCallingProcessOwnsStorage()
    {
        var target = Installation();
        var staging = Path.Combine(_root, "staging");
        Directory.CreateDirectory(staging);
        File.Copy(Fixture, Path.Combine(staging, Path.GetFileName(target.Executable)));
        AppUpdater.Install(target, staging);
        using var storage = new FileStream(Path.Combine(_root, "storage.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        AppUpdater.StartInstalled(target);
        var helperPath = UpdateRestart.ReceiptPath(target) + ".helper";
        if (File.Exists(helperPath)) _processes.Add(Process.GetProcessById(int.Parse(File.ReadAllText(helperPath), System.Globalization.CultureInfo.InvariantCulture)));
        await Task.Delay(500);
        Assert.False(File.Exists(Path.Combine(_root, "startup-blocked.txt")));
        Assert.False(File.Exists(Path.Combine(_root, "recovered-started.txt")));
        // Terminate only the helper created in this test; it deliberately waits for our test runner to exit.
        if (_processes.Count > 0) { _processes[0].Kill(); await _processes[0].WaitForExitAsync(); }
    }

    [Fact]
    public async Task StartupFailure_RestoresThePreviousExecutable_AndRestartsIt()
    {
        var target = Installation();
        var oldBytes = File.ReadAllBytes(target.Executable);
        var staging = Path.Combine(_root, "staging");
        Directory.CreateDirectory(staging);
        File.Copy(Fixture, Path.Combine(staging, Path.GetFileName(target.Executable)));
        using (var changed = new FileStream(Path.Combine(staging, Path.GetFileName(target.Executable)), FileMode.Append))
            changed.Write([1, 2, 3, 4]);
        AppUpdater.Install(target, staging);
        File.WriteAllText(Path.Combine(_root, "fail-startup"), "fail only the updated startup");
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(UpdateRestart.ReceiptPath(target)))!;
        var helper = Start(target.Executable, "--update-helper", UpdateRestart.ReceiptPath(target), receipt.Id, "2147483647", "0");
        await helper.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20));
        await WaitFor(Path.Combine(_root, "recovered-started.txt"));

        Assert.Equal(1, helper.ExitCode);
        Assert.Equal(oldBytes, File.ReadAllBytes(target.Executable));
        Assert.True(File.Exists(target.Executable + "." + receipt.Id + ".failed"));
        Assert.Contains("restoring", File.ReadAllText(UpdateRestart.ReceiptPath(target) + ".error"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartupTimeout_StopsOnlyTheFailedChild_AndRecovers()
    {
        var target = Installation();
        var staging = Path.Combine(_root, "staging");
        Directory.CreateDirectory(staging);
        File.Copy(Fixture, Path.Combine(staging, Path.GetFileName(target.Executable)));
        AppUpdater.Install(target, staging);
        File.WriteAllText(Path.Combine(_root, "hang-startup"), "hang only the updated startup");
        var path = UpdateRestart.ReceiptPath(target);
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
        var result = await UpdateRestart.RunAsync(path, receipt.Id, int.MaxValue, 0, startupTimeout: TimeSpan.FromMilliseconds(300));
        await WaitFor(Path.Combine(_root, "recovered-started.txt"));
        Assert.Equal(1, result);
        Assert.Contains("did not confirm startup", File.ReadAllText(path + ".error"), StringComparison.Ordinal);
        Assert.True(File.Exists(target.Executable));
        Assert.True(File.Exists(receipt.Entries.Single().Backup)); // Retained until explicit successful cleanup.
    }

    // Another program (an antivirus scan, an indexer) holds the startup acknowledgement open while the new
    // application keeps running. The helper reads it again, like an acknowledgement still to come, instead of taking
    // the sharing violation for a failed startup: that rolled back a healthy update and killed the new application.
    // The lock is released only after the helper has met it, and a delayed launch changes nothing.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ALockedAcknowledgementOfARunningApplicationIsReadAgain_NotRolledBack(bool delayedLaunch)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows sharing violations."); return; }
        var (target, receipt) = ChangedInstallation();
        var path = UpdateRestart.ReceiptPath(target);
        var updatedBytes = File.ReadAllBytes(target.Executable);
        // The updated child stays up without acknowledging; this test publishes the acknowledgement and holds it open.
        File.WriteAllText(Path.Combine(_root, "hang-startup"), "hang only the updated startup");
        using var scanner = HoldAcknowledgement(path, receipt.Id);
        await DelayLaunchAsync(target, delayedLaunch);
        var steps = new Checkpoints();

        var running = Own(UpdateRestart.RunAsync(path, receipt.Id, int.MaxValue, 0, startupTimeout: TimeSpan.FromSeconds(60),
            checkpoint: steps.Record));
        try
        {
            await steps.Reached("ack-unreadable");
            Assert.True(steps.Saw("child-started"));
            Assert.False(steps.Saw("child-exited-awaiting-ack"));
            Assert.False(running.IsCompleted);
            scanner.Dispose();

            Assert.Equal(0, await running.WaitAsync(TimeSpan.FromSeconds(60)));
            Assert.Equal(updatedBytes, File.ReadAllBytes(target.Executable));
            Assert.False(File.Exists(target.Executable + "." + receipt.Id + ".failed"));
            Assert.False(File.Exists(Path.Combine(_root, "recovered-started.txt")));
        }
        finally
        {
            scanner.Dispose();
        }
    }

    // The new application acknowledged and exited while its acknowledgement was held open. Released during the
    // wait that follows the exit, the update stays; held past that wait, the exit is a failed startup and the
    // previous version returns. Every step is synchronized with the helper, and a delayed launch changes nothing.
    [Theory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task AnExitAfterALockedAcknowledgementKeepsTheUpdateOnlyWhenItCanBeRead(bool readable, bool delayedLaunch)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows sharing violations."); return; }
        var (target, receipt) = ChangedInstallation();
        var path = UpdateRestart.ReceiptPath(target);
        var updatedBytes = File.ReadAllBytes(target.Executable);
        // The updated child exits at once; the acknowledgement it would have written is this test's, held open.
        File.WriteAllText(Path.Combine(_root, "fail-startup"), "exit the updated startup at once");
        using var scanner = HoldAcknowledgement(path, receipt.Id);
        await DelayLaunchAsync(target, delayedLaunch);
        var steps = new Checkpoints();

        // Released: a wait long enough that only the release can end it. Held: a short wait, and the lock outlives it.
        var running = Own(UpdateRestart.RunAsync(path, receipt.Id, int.MaxValue, 0, startupTimeout: TimeSpan.FromSeconds(60),
            exitedAcknowledgementWait: readable ? TimeSpan.FromSeconds(60) : TimeSpan.FromMilliseconds(300), checkpoint: steps.Record));
        int result;
        try
        {
            if (readable)
            {
                await steps.Reached("ack-unreadable-after-exit");
                Assert.False(running.IsCompleted);
                scanner.Dispose();
            }
            result = await running.WaitAsync(TimeSpan.FromSeconds(60));
        }
        finally
        {
            scanner.Dispose();
        }

        Assert.True(steps.Saw("child-exited-awaiting-ack"));
        Assert.True(steps.Saw("ack-unreadable-after-exit"));
        if (readable)
        {
            Assert.Equal(0, result);
            Assert.Equal(updatedBytes, File.ReadAllBytes(target.Executable));
            Assert.False(File.Exists(Path.Combine(_root, "recovered-started.txt")));
        }
        else
        {
            Assert.Equal(1, result);
            await WaitFor(Path.Combine(_root, "recovered-started.txt"));
            Assert.NotEqual(updatedBytes, File.ReadAllBytes(target.Executable));
            Assert.True(File.Exists(target.Executable + "." + receipt.Id + ".failed"));
        }
    }

    // Negative control: the new application exited and the acknowledgement is readable but names another update.
    // That is a failed startup at once; it is not read again during the post-exit wait, which only covers a held file.
    [Fact]
    public async Task AnExitWithAnAcknowledgementForAnotherUpdateRollsBackWithoutWaiting()
    {
        var (target, receipt) = ChangedInstallation();
        var path = UpdateRestart.ReceiptPath(target);
        var updatedBytes = File.ReadAllBytes(target.Executable);
        File.WriteAllText(Path.Combine(_root, "fail-startup"), "exit the updated startup at once");
        File.WriteAllText(path + "." + receipt.Id + ".ready", Guid.NewGuid().ToString("N"));
        var steps = new Checkpoints();

        // A post-exit wait far longer than the bound below: only an immediate answer can pass.
        var result = await Own(UpdateRestart.RunAsync(path, receipt.Id, int.MaxValue, 0, startupTimeout: TimeSpan.FromSeconds(60),
            exitedAcknowledgementWait: TimeSpan.FromMinutes(10), checkpoint: steps.Record)).WaitAsync(TimeSpan.FromMinutes(2));

        Assert.Equal(1, result);
        Assert.True(steps.Saw("child-exited-awaiting-ack"));
        Assert.True(steps.Saw("ack-mismatch"));
        Assert.False(steps.Saw("ack-unreadable"));
        await WaitFor(Path.Combine(_root, "recovered-started.txt"));
        Assert.NotEqual(updatedBytes, File.ReadAllBytes(target.Executable));
        Assert.True(File.Exists(target.Executable + "." + receipt.Id + ".failed"));
    }

    // A test fails while the helper's restarted previous version is still starting. Cleanup stops that child, which
    // the helper launched and never awaited, before any file of the installation is removed; the test's own failure
    // stays the reported one. Before, the directory was deleted beneath it and its loader error hid the failure.
    [Fact]
    public async Task AFailureWhileRecoveryStartsStopsTheRecoveryBeforeRemovingItsFiles()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows sharing violations."); return; }
        var (target, receipt) = ChangedInstallation();
        var path = UpdateRestart.ReceiptPath(target);
        File.WriteAllText(Path.Combine(_root, "fail-startup"), "exit the updated startup at once");
        File.WriteAllText(Path.Combine(_root, "hold-recovery"), "hold the restarted previous version");
        var held = Path.Combine(_root, "recovery-held.txt");

        var failure = await Record.ExceptionAsync(async () =>
        {
            Assert.Equal(1, await Own(UpdateRestart.RunAsync(path, receipt.Id, int.MaxValue, 0, startupTimeout: TimeSpan.FromSeconds(60))));
            await WaitFor(held);
            Assert.Fail("A deliberate failure while the restarted previous version is still starting.");
        });

        Assert.Contains("deliberate failure", failure?.Message, StringComparison.Ordinal);
        using var recovery = Process.GetProcessById(int.Parse(File.ReadAllText(held), System.Globalization.CultureInfo.InvariantCulture));
        Assert.False(recovery.HasExited);
        Assert.Contains(recovery.Id, OwnedProcesses().Select(process => { using (process) return process.Id; }));

        await StopOwnedAsync();

        Assert.True(recovery.HasExited);
        Assert.Empty(OwnedProcesses());
        // Nothing was removed while it ran: its executable and dependencies are still in place.
        Assert.True(File.Exists(target.Executable));
        Assert.True(File.Exists(Path.Combine(_root, "QueueLoom.Core.dll")));
        Assert.False(File.Exists(Path.Combine(_root, "recovered-started.txt")));
    }

    /// <summary>Publishes the acknowledgement and holds it open the way a scanner would, until disposed.</summary>
    private static FileStream HoldAcknowledgement(string receiptPath, string id)
    {
        var ready = receiptPath + "." + id + ".ready";
        File.WriteAllText(ready, id);
        return new FileStream(ready, FileMode.Open, FileAccess.Read, FileShare.None);
    }

    /// <summary>Holds the updated executable so the helper's first launch attempts fail and it starts late.</summary>
    private async Task DelayLaunchAsync(UpdateTarget target, bool delayed)
    {
        if (!delayed) return;
        Start(Fixture, "--hold-lock", target.Executable, "1500");
        await WaitFor(target.Executable + ".held");
    }

    /// <summary>The helper's checkpoints, with "ack-unreadable-after-exit" for an unreadable read after the child exited.</summary>
    private sealed class Checkpoints
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, TaskCompletionSource> _reached = new();
        private volatile bool _exited;

        public void Record(string step)
        {
            if (step == "child-exited-awaiting-ack") _exited = true;
            Signal(step);
            if (step == "ack-unreadable" && _exited) Signal("ack-unreadable-after-exit");
        }

        public bool Saw(string step) => Source(step).Task.IsCompleted;

        public Task Reached(string step) => Source(step).Task.WaitAsync(TimeSpan.FromSeconds(60));

        private void Signal(string step) => Source(step).TrySetResult();

        private TaskCompletionSource Source(string step) =>
            _reached.GetOrAdd(step, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }

    [Fact]
    public async Task WindowsLaunch_RetriesUntilTheExecutableLockIsReleased()
    {
        if (!OperatingSystem.IsWindows()) return;
        var target = Installation();
        var staging = Path.Combine(_root, "staging");
        Directory.CreateDirectory(staging);
        File.Copy(Fixture, Path.Combine(staging, Path.GetFileName(target.Executable)));
        AppUpdater.Install(target, staging);
        var holder = Start(Fixture, "--hold-lock", target.Executable, "1200");
        await WaitFor(target.Executable + ".held");
        var path = UpdateRestart.ReceiptPath(target);
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
        var result = await UpdateRestart.RunAsync(path, receipt.Id, int.MaxValue, 0);
        Assert.Equal(0, result);
        Assert.True(holder.HasExited);
        Assert.True(File.Exists(Path.Combine(_root, "updated-started.txt")));
        Assert.False(File.Exists(receipt.Entries.Single().Backup));
    }

    [Fact]
    public async Task Cleanup_RetriesLockedBackup_WithoutDeletingUserFiles()
    {
        var target = Installation();
        var staging = Path.Combine(_root, "staging");
        Directory.CreateDirectory(staging);
        File.Copy(Fixture, Path.Combine(staging, Path.GetFileName(target.Executable)));
        AppUpdater.Install(target, staging);
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(UpdateRestart.ReceiptPath(target)))!;
        var holder = Start(Fixture, "--hold-lock", receipt.Entries.Single().Backup!, "1200");
        await WaitFor(receipt.Entries.Single().Backup! + ".held");
        var cleaning = UpdateRestart.CleanAsync(receipt, UpdateRestart.ReceiptPath(target), TimeSpan.FromSeconds(10));
        if (OperatingSystem.IsWindows())
        {
            await Task.Delay(250);
            Assert.False(cleaning.IsCompleted);
            Assert.True(File.Exists(receipt.Entries.Single().Backup));
        }
        await cleaning;
        await holder.WaitForExitAsync();
        Assert.False(File.Exists(receipt.Entries.Single().Backup));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HealthyStartup_WhenDiagnosticCannotBeWritten_NeverRollsBack(bool lockedDiagnostic)
    {
        if (lockedDiagnostic && !OperatingSystem.IsWindows()) return;
        var (target, receipt) = ChangedInstallation();
        var path = UpdateRestart.ReceiptPath(target);
        // An existing unowned download causes cleanup to fail after the GUI confirms readiness.
        var unowned = Path.Combine(_root, "unowned-" + receipt.Id);
        Directory.CreateDirectory(unowned);
        File.WriteAllText(Path.Combine(unowned, "keep"), "keep");
        receipt = receipt with { DownloadDirectory = unowned };
        File.WriteAllText(path, JsonSerializer.Serialize(receipt));
        var updatedBytes = File.ReadAllBytes(target.Executable);
        using var diagnosticLock = BlockDiagnostic(path, lockedDiagnostic);

        var result = await UpdateRestart.RunAsync(path, receipt.Id, int.MaxValue, 0);

        Assert.Equal(0, result);
        Assert.Equal(updatedBytes, File.ReadAllBytes(target.Executable));
        Assert.False(File.Exists(target.Executable + "." + receipt.Id + ".failed"));
        Assert.False(File.Exists(Path.Combine(_root, "recovered-started.txt")));
        Assert.Equal("keep", File.ReadAllText(Path.Combine(unowned, "keep")));
        using var child = Process.GetProcessById(int.Parse(File.ReadAllText(Path.Combine(_root, "updated-started.txt")),
            System.Globalization.CultureInfo.InvariantCulture));
        Assert.False(child.HasExited);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedStartup_WhenDiagnosticCannotBeWritten_StillRestoresAndRestarts(bool lockedDiagnostic)
    {
        if (lockedDiagnostic && !OperatingSystem.IsWindows()) return;
        var (target, receipt) = ChangedInstallation();
        var path = UpdateRestart.ReceiptPath(target);
        var previousBytes = File.ReadAllBytes(receipt.Entries.Single().Backup!);
        File.WriteAllText(Path.Combine(_root, "fail-startup"), "fail only updated startup");
        using var diagnosticLock = BlockDiagnostic(path, lockedDiagnostic);

        var result = await UpdateRestart.RunAsync(path, receipt.Id, int.MaxValue, 0);
        await WaitFor(Path.Combine(_root, "recovered-started.txt"));

        Assert.Equal(1, result);
        Assert.Equal(previousBytes, File.ReadAllBytes(target.Executable));
        Assert.True(File.Exists(target.Executable + "." + receipt.Id + ".failed"));
        Assert.True(JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!.Recovered);
    }

    private (UpdateTarget Target, UpdateRestart.Receipt Receipt) ChangedInstallation()
    {
        var target = Installation();
        var staging = Path.Combine(_root, "staging");
        Directory.CreateDirectory(staging);
        var updated = Path.Combine(staging, Path.GetFileName(target.Executable));
        File.Copy(Fixture, updated);
        using (var changed = new FileStream(updated, FileMode.Append)) changed.Write([1, 2, 3, 4]);
        AppUpdater.Install(target, staging);
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(UpdateRestart.ReceiptPath(target)))!;
        return (target, receipt);
    }

    private static FileStream? BlockDiagnostic(string path, bool locked)
    {
        if (locked) return new FileStream(path + ".error", FileMode.Create, FileAccess.ReadWrite, FileShare.None);
        // Portable equivalent of an unwritable diagnostic destination.
        Directory.CreateDirectory(path + ".error");
        return null;
    }
    private UpdateTarget Installation()
    {
        Assert.True(File.Exists(Fixture), "Update fixture must be built with the tests.");
        foreach (var file in Directory.EnumerateFiles(Path.GetDirectoryName(Fixture)!))
            File.Copy(file, Path.Combine(_root, Path.GetFileName(file)));
        return AppUpdater.TargetFor(OperatingSystem.IsWindows() ? "win-x64" : "linux-x64", Path.Combine(_root, Path.GetFileName(Fixture)));
    }

    private Process Start(string executable, params string[] arguments)
    {
        var start = new ProcessStartInfo(executable) { UseShellExecute = false, WorkingDirectory = _root };
        foreach (var argument in arguments) start.ArgumentList.Add(argument);
        var process = Process.Start(start)!;
        _processes.Add(process);
        return process;
    }

    private static async Task WaitFor(string path)
    {
        var watch = Stopwatch.StartNew();
        while (!File.Exists(path))
        {
            if (watch.Elapsed > TimeSpan.FromSeconds(15)) throw new TimeoutException(path);
            await Task.Delay(50);
        }
    }

    private Task<int> Own(Task<int> helper)
    {
        lock (_helpers) _helpers.Add(helper);
        return helper;
    }

    /// <summary>
    /// Every process running an executable from this test's own installation: the updated child, the restarted
    /// previous version and fixture helpers. Nothing outside the installation directory is ever touched.
    /// </summary>
    private List<Process> OwnedProcesses()
    {
        var root = Path.GetFullPath(_root) + Path.DirectorySeparatorChar;
        var owned = new List<Process>();
        foreach (var process in Process.GetProcessesByName(Path.GetFileNameWithoutExtension(Fixture)))
        {
            try
            {
                if (process.MainModule?.FileName is { } file && file.StartsWith(root, StringComparison.OrdinalIgnoreCase))
                {
                    owned.Add(process);
                    continue;
                }
            }
            catch (Exception exception) when (exception is InvalidOperationException or System.ComponentModel.Win32Exception)
            {
                // Exited meanwhile, or not ours to inspect.
            }
            process.Dispose();
        }
        return owned;
    }

    /// <summary>
    /// Awaits the helper runs this test started, then stops and joins every process of this installation, so no
    /// file is removed beneath a running process. Runs before the directory is deleted, after a failure too.
    /// </summary>
    private async Task StopOwnedAsync()
    {
        Task[] helpers;
        lock (_helpers) helpers = [.. _helpers];
        foreach (var helper in helpers)
        {
            try { await helper.WaitAsync(TimeSpan.FromSeconds(90)); }
            catch (Exception) { /* Its outcome belongs to the test; cleanup only needs it finished. */ }
        }
        foreach (var process in _processes)
        {
            if (!process.HasExited) { process.Kill(); await process.WaitForExitAsync(); }
        }
        // A restarted previous version is started by the helper and not awaited by it; stop until none is left.
        for (var round = 0; round < 10; round++)
        {
            var owned = OwnedProcesses();
            if (owned.Count == 0) return;
            foreach (var process in owned)
            {
                using (process)
                {
                    try
                    {
                        if (!process.HasExited) process.Kill(entireProcessTree: true);
                        await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                    }
                    catch (InvalidOperationException) { /* Exited meanwhile. */ }
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopOwnedAsync();
        foreach (var process in _processes) process.Dispose();
        var cleanup = Stopwatch.StartNew();
        while (true)
        {
            try { Directory.Delete(_root, true); break; }
            catch (Exception exception) when ((exception is IOException or UnauthorizedAccessException) && cleanup.Elapsed < TimeSpan.FromSeconds(5))
            {
                // Windows may hold image sections briefly after the process exit signal.
                await Task.Delay(50);
            }
        }
    }
}
