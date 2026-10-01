using System.Diagnostics;
using System.Text.Json;
using QueueLoom.App.Services;

namespace QueueLoom.Tests;

/// <summary>Real processes, isolated installations and real Windows file locks; never application storage.</summary>
public sealed class UpdateRestartProcessTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "update process with spaces " + Guid.NewGuid().ToString("N"));
    private readonly List<Process> _processes = [];
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
        var helper = Start(target.Executable, "--update-helper", UpdateRestart.ReceiptPath(target), receipt.Id,
            parent.Id.ToString(System.Globalization.CultureInfo.InvariantCulture), parent.StartTime.ToUniversalTime().Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture));
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
        Assert.False(File.Exists(receipt.Entries.Single().Backup));
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

    public void Dispose()
    {
        foreach (var process in _processes)
        {
            if (!process.HasExited) { process.Kill(); process.WaitForExit(); }
            process.Dispose();
        }
        // Fixture startup children exit after one second. Wait for only those belonging to this isolated installation.
        var marker = Path.Combine(_root, "updated-started.txt");
        if (File.Exists(marker))
        {
            try
            {
                using var child = Process.GetProcessById(int.Parse(File.ReadAllText(marker), System.Globalization.CultureInfo.InvariantCulture));
                if (child.MainModule?.FileName == Path.Combine(_root, Path.GetFileName(Fixture))) { child.Kill(); child.WaitForExit(); }
            }
            catch (ArgumentException) { }
        }
        var cleanup = Stopwatch.StartNew();
        while (true)
        {
            try { Directory.Delete(_root, true); break; }
            catch (Exception exception) when ((exception is IOException or UnauthorizedAccessException) && cleanup.Elapsed < TimeSpan.FromSeconds(5))
            {
                // Windows may hold image sections briefly after the process exit signal.
                Thread.Sleep(50);
            }
        }
    }
}
