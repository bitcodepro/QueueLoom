using System.Diagnostics;
using System.Text.Json;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class WindowsUpdateSmokeTests(ITestOutputHelper testOutput)
{
    [Theory]
    [InlineData(0, false, false)]
    [InlineData(7, false, false)]
    [InlineData(0, true, false)]
    [InlineData(7, false, true)]
    public async Task StableSmokeWaitsForTheActualHelperAfterReceiptDeletionAndChecksItsExit(int helperExit, bool gateGuiMarker, bool diagnosticsUnavailable)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("The packaged GUI smoke script requires Windows."); return; }
        using var fixture = new StableLauncherTests.InstallationFixture();
        using var gateDirectory = new TemporaryDirectory();
        var gate = Path.Combine(gateDirectory.Path, "helper exit gate");
        var start = SmokeStart(fixture, gateDirectory, diagnosticsUnavailable);
        start.Environment["QUEUELOOM_SMOKE_FIXTURE_GATE"] = gate;
        if (diagnosticsUnavailable) start.Environment["QUEUELOOM_SMOKE_DIAGNOSTICS_FAILURE"] = "1";
        if (gateGuiMarker) File.WriteAllText(gate + ".gui-marker", "hold");
        using var script = StartPowerShell(start);
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var output = ReadOutput(script, waiting);
        var error = script.StandardError.ReadToEndAsync();
        var exited = script.WaitForExitAsync();
        try
        {
            if (gateGuiMarker)
            {
                await WaitForPublishedSmokeMarker(gate + ".gui.pid.tmp", exited);
                using var startup = JsonDocument.Parse(ReadPublishedSmokeFile(gate + ".gui-startup.json"));
                var ready = startup.RootElement.GetProperty("Receipt").GetString() + "." + startup.RootElement.GetProperty("Id").GetString() + ".ready";
                Assert.False(File.Exists(ready)); // No helper completion may precede the closed GUI marker.
                Assert.False(File.Exists(gate + ".helper.json"));
                var published = WaitForPublishedSmokeMarker(gate + ".gui.pid", exited);
                Assert.False(published.IsCompleted); // The actual GUI is still held before final publication.
                Assert.False(script.HasExited);
                File.WriteAllText(gate + ".gui-marker.open", "open");
                await published;
            }
            var deadline = Stopwatch.StartNew();
            while (!File.Exists(gate + ".helper.json") && !script.HasExited && deadline.Elapsed < TimeSpan.FromSeconds(30))
                await Task.Delay(10);
            Assert.True(File.Exists(gate + ".helper.json"), script.HasExited ? await output + await error : "Helper did not reach the exit gate.");
            // On the old script, receipt deletion triggers the GUI count and failure while the helper is gated.
            var observed = await Task.WhenAny(waiting.Task, exited).WaitAsync(TimeSpan.FromSeconds(15));
            Assert.True(observed == waiting.Task, await CompletedOutput(script, output, error));
            Assert.False(script.HasExited);
            using var marker = JsonDocument.Parse(ReadPublishedSmokeFile(gate + ".helper.json"));
            var helperPid = marker.RootElement.GetProperty("Pid").GetInt32();
            var helperTicks = marker.RootElement.GetProperty("StartTicks").GetInt64();
            var executable = marker.RootElement.GetProperty("Executable").GetString()!;
            Assert.False(File.Exists(marker.RootElement.GetProperty("Receipt").GetString()));
            using var helper = Process.GetProcessById(helperPid);
            _ = helper.Handle;
            Assert.Equal(helperTicks, helper.StartTime.ToUniversalTime().Ticks);
            Assert.False(helper.HasExited);
            var guiPid = int.Parse(ReadPublishedSmokeFile(gate + ".gui.pid"), System.Globalization.CultureInfo.InvariantCulture);
            var matching = Process.GetProcessesByName("QueueLoom");
            try
            {
                var ids = matching.Where(process => !process.HasExited && process.MainModule?.FileName == executable).Select(process => process.Id).ToArray();
                Assert.Equal(2, ids.Length);
                Assert.Contains(helperPid, ids);
                Assert.Contains(guiPid, ids);
                LogSmokeOutput($"Receipt removed with helper PID {helperPid} (start ticks {helperTicks}) and GUI PID {guiPid} sharing {executable}.");
            }
            finally { foreach (var process in matching) process.Dispose(); }
            PublishRelease(gate + ".release", helperExit);
            await exited.WaitAsync(TimeSpan.FromSeconds(30));
            await helper.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            LogSmokeOutput($"Requested helper exit: {helperExit}; observed helper exit: {helper.ExitCode}; observer exit: {script.ExitCode}.");
            Assert.Equal(helperExit, helper.ExitCode);
            var detail = await output + await error;
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
            if (gateGuiMarker && !File.Exists(gate + ".gui-marker.open")) File.WriteAllText(gate + ".gui-marker.open", "open");
            if (!File.Exists(gate + ".release")) PublishRelease(gate + ".release", 0);
            try { await exited.WaitAsync(TimeSpan.FromSeconds(10)); }
            catch (TimeoutException) { if (!script.HasExited) script.Kill(entireProcessTree: true); await exited; }
            await Task.WhenAll(output, error);
            LogSmokeOutput($"Observer final exit: {script.ExitCode}.");
            LogSmokeOutput("Observer stdout:\n" + await output + "\nObserver stderr:\n" + await error);
        }
    }

    private void LogSmokeOutput(string detail) => testOutput.WriteLine(SensitiveDataRedactor.Redact(detail));

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
