using System.Diagnostics;
using System.Text.Json;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class WindowsUpdateSmokeTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(7)]
    public async Task StableSmokeWaitsForTheActualHelperAfterReceiptDeletionAndChecksItsExit(int helperExit)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("The packaged GUI smoke script requires Windows."); return; }
        using var fixture = new StableLauncherTests.InstallationFixture();
        using var gateDirectory = new TemporaryDirectory();
        var gate = Path.Combine(gateDirectory.Path, "helper exit gate");
        // The framework-dependent launcher shares the already installed fixture's Core assembly.
        // Copy only launcher-specific files so this test never overwrites a DLL mapped by the old process.
        var package = Path.Combine(gateDirectory.Path, "stable package");
        Directory.CreateDirectory(package);
        foreach (var file in Directory.GetFiles(fixture.Root).Where(file =>
                     Path.GetFileName(file) == "QueueLoom.exe" || Path.GetFileName(file).StartsWith("QueueLoom.Launcher.", StringComparison.Ordinal) ||
                     Path.GetFileName(file).StartsWith("QueueLoom.bootstrap.", StringComparison.Ordinal)))
            File.Copy(file, Path.Combine(package, Path.GetFileName(file)));
        var repository = FindRepository();
        var start = new ProcessStartInfo("pwsh")
        {
            UseShellExecute = false, CreateNoWindow = true, WorkingDirectory = repository,
            RedirectStandardOutput = true, RedirectStandardError = true
        };
        foreach (var argument in new[] { "-NoProfile", "-File", Path.Combine(repository, ".github", "scripts", "smoke-update.ps1"),
                     "-Executable", Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture.exe"),
                     "-FixtureDirectory", Path.Combine(AppContext.BaseDirectory, "UpdateFixture"), "-StablePackageDirectory", package })
            start.ArgumentList.Add(argument);
        start.Environment["QUEUELOOM_SMOKE_FIXTURE_GATE"] = gate;
        using var script = Process.Start(start)!;
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var output = ReadOutput(script, waiting);
        var error = script.StandardError.ReadToEndAsync();
        var exited = script.WaitForExitAsync();
        try
        {
            var deadline = Stopwatch.StartNew();
            while (!File.Exists(gate + ".helper.json") && !script.HasExited && deadline.Elapsed < TimeSpan.FromSeconds(30))
                await Task.Delay(10);
            Assert.True(File.Exists(gate + ".helper.json"), script.HasExited ? await output + await error : "Helper did not reach the exit gate.");
            // On the old script, receipt deletion triggers the GUI count and failure while the helper is gated.
            var observed = await Task.WhenAny(waiting.Task, exited).WaitAsync(TimeSpan.FromSeconds(15));
            Assert.True(observed == waiting.Task, await CompletedOutput(script, output, error));
            Assert.False(script.HasExited);
            using var marker = JsonDocument.Parse(File.ReadAllText(gate + ".helper.json"));
            var helperPid = marker.RootElement.GetProperty("Pid").GetInt32();
            var helperTicks = marker.RootElement.GetProperty("StartTicks").GetInt64();
            var executable = marker.RootElement.GetProperty("Executable").GetString()!;
            Assert.False(File.Exists(marker.RootElement.GetProperty("Receipt").GetString()));
            using var helper = Process.GetProcessById(helperPid);
            Assert.Equal(helperTicks, helper.StartTime.ToUniversalTime().Ticks);
            Assert.False(helper.HasExited);
            var guiPid = int.Parse(File.ReadAllText(gate + ".gui.pid"), System.Globalization.CultureInfo.InvariantCulture);
            var matching = Process.GetProcessesByName("QueueLoom");
            try
            {
                var ids = matching.Where(process => !process.HasExited && process.MainModule?.FileName == executable).Select(process => process.Id).ToArray();
                Assert.Equal(2, ids.Length);
                Assert.Contains(helperPid, ids);
                Assert.Contains(guiPid, ids);
                Console.WriteLine($"Receipt removed with helper PID {helperPid} (start ticks {helperTicks}) and GUI PID {guiPid} sharing {executable}.");
            }
            finally { foreach (var process in matching) process.Dispose(); }
            File.WriteAllText(gate + ".release", helperExit.ToString(System.Globalization.CultureInfo.InvariantCulture));
            await exited.WaitAsync(TimeSpan.FromSeconds(30));
            var detail = await output + await error;
            Console.WriteLine(detail);
            if (helperExit == 0)
            {
                Assert.True(script.ExitCode == 0, detail);
                Assert.Contains("PASS packaged Windows update", detail, StringComparison.Ordinal);
            }
            else
            {
                Assert.NotEqual(0, script.ExitCode);
                Assert.Contains("Packaged restart helper failed (7)", detail, StringComparison.Ordinal);
                Assert.DoesNotContain("PASS packaged Windows update", detail, StringComparison.Ordinal);
            }
        }
        finally
        {
            // Release only this test's gate; give the script's own scoped cleanup a chance to finish.
            if (!File.Exists(gate + ".release")) File.WriteAllText(gate + ".release", "0");
            try { await exited.WaitAsync(TimeSpan.FromSeconds(10)); }
            catch (TimeoutException) { if (!script.HasExited) script.Kill(entireProcessTree: true); await exited; }
            await Task.WhenAll(output, error);
        }
    }

    private static async Task<string> ReadOutput(Process script, TaskCompletionSource waiting)
    {
        var output = new System.Text.StringBuilder();
        while (await script.StandardOutput.ReadLineAsync() is { } line)
        {
            output.AppendLine(line);
            if (line.StartsWith("Waiting for restart helper PID ", StringComparison.Ordinal)) waiting.TrySetResult();
        }
        return output.ToString();
    }

    private static async Task<string> CompletedOutput(Process script, Task<string> output, Task<string> error) =>
        script.HasExited ? await output + await error : "The smoke script did not wait for its identified restart helper.";

    private static string FindRepository()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
            if (File.Exists(Path.Combine(directory.FullName, ".github", "scripts", "smoke-update.ps1"))) return directory.FullName;
        throw new DirectoryNotFoundException("The Windows smoke script was not found.");
    }
}
