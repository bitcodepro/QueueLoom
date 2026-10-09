using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class WindowsUpdateSmokeTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RejectedLauncherOrPayloadCandidateCannotKillAnOutsideSentinel(bool payloadCandidate)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("The packaged GUI smoke script requires Windows."); return; }
        using var fixture = new StableLauncherTests.InstallationFixture();
        using var directory = new TemporaryDirectory();
        var sentinelLock = Path.Combine(directory.Path, "outside sentinel.lock");
        var sentinelStart = new ProcessStartInfo(Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture.exe"))
            { UseShellExecute = false, CreateNoWindow = true };
        foreach (var arg in new[] { "--hold-lock", sentinelLock, "until-released" }) sentinelStart.ArgumentList.Add(arg);
        using var sentinel = Process.Start(sentinelStart)!;
        Process? script = null;
        try
        {
            await WaitForSmokeMarker(sentinelLock + ".held", sentinel);
            var identity = Path.Combine(directory.Path, "outside identity.json");
            File.WriteAllText(identity, JsonSerializer.Serialize(new { Pid = sentinel.Id, StartTicks = sentinel.StartTime.ToUniversalTime().Ticks }));
            var start = SmokeStart(fixture, directory, payloadCandidate);
            start.Environment[payloadCandidate ? "QUEUELOOM_SMOKE_PAYLOAD_IDENTITY" : "QUEUELOOM_SMOKE_FIXTURE_IDENTITY"] = identity;
            script = StartPowerShell(start);
            var output = script.StandardOutput.ReadToEndAsync();
            var error = script.StandardError.ReadToEndAsync();
            await script.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(30));
            var detail = await output + await error;
            Assert.NotEqual(0, script.ExitCode);
            Assert.Contains(payloadCandidate ? "payload identity changed" : "unexpected restart helper identity", detail, StringComparison.Ordinal);
            Assert.False(sentinel.HasExited, "A rejected outside process identity was killed by smoke cleanup. " + detail);
        }
        finally
        {
            if (script is not null)
            {
                if (!script.HasExited) { script.Kill(entireProcessTree: true); await script.WaitForExitAsync(); }
                script.Dispose();
            }
            File.WriteAllText(sentinelLock + ".release", "release");
            if (!sentinel.HasExited) await sentinel.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(7)]
    public async Task ReleasePublicationNeverExposesAnOpenWriterToTheRealReader(int exitCode)
    {
        using var directory = new TemporaryDirectory();
        var release = Path.Combine(directory.Path, "release");
        using var entered = new ManualResetEventSlim();
        using var closeWriter = new ManualResetEventSlim();
        var start = new ProcessStartInfo(Path.Combine(AppContext.BaseDirectory, "UpdateFixture",
            OperatingSystem.IsWindows() ? "QueueLoom.UpdateFixture.exe" : "QueueLoom.UpdateFixture"))
            { UseShellExecute = false, CreateNoWindow = true, RedirectStandardError = true };
        foreach (var arg in new[] { "--smoke-release-reader", release, release + ".visible" }) start.ArgumentList.Add(arg);
        using var reader = Process.Start(start)!;
        var error = reader.StandardError.ReadToEndAsync();
        var publication = Task.Run(() => PublishRelease(release, exitCode, () =>
        {
            entered.Set();
            if (!closeWriter.Wait(TimeSpan.FromSeconds(20))) throw new TimeoutException("Release publication gate timed out.");
        }));
        try
        {
            Assert.True(entered.Wait(TimeSpan.FromSeconds(10)));
            Assert.False(File.Exists(release));
            Assert.False(File.Exists(release + ".visible"));
            Assert.False(reader.HasExited);
            closeWriter.Set();
            await publication;
            await reader.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(exitCode, reader.ExitCode);
            Assert.Equal(string.Empty, await error);
            Assert.Equal(exitCode.ToString(System.Globalization.CultureInfo.InvariantCulture), File.ReadAllText(release));
        }
        finally
        {
            closeWriter.Set();
            try { await publication; }
            finally
            {
                if (!reader.HasExited) { reader.Kill(); await reader.WaitForExitAsync(); }
                await error;
            }
        }
    }

    private static void PublishRelease(string path, int exitCode, Action? beforeClose = null)
    {
        var temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            using (var writer = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                writer.Write(System.Text.Encoding.UTF8.GetBytes(exitCode.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                beforeClose?.Invoke();
            }
            File.Move(temporary, path); // Readers observe only closed, complete content in both release paths.
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }

    private static Process StartPowerShell(ProcessStartInfo start)
    {
        try { return Process.Start(start) ?? throw new IOException("PowerShell did not start."); }
        catch (Win32Exception error) when (error.NativeErrorCode == 2)
        { Assert.Skip("PowerShell 7 (pwsh) is not installed or available on PATH."); throw; }
    }

    private static async Task WaitForSmokeMarker(string path, Process process)
    {
        var watch = Stopwatch.StartNew();
        while (!File.Exists(path) && !process.HasExited && watch.Elapsed < TimeSpan.FromSeconds(30)) await Task.Delay(10);
        Assert.True(File.Exists(path), "The fixture did not publish " + Path.GetFileName(path));
    }

    private static ProcessStartInfo SmokeStart(StableLauncherTests.InstallationFixture fixture, TemporaryDirectory directory, bool wrapper)
    {
        // Framework-dependent fixture dependencies already exist beside the old process; do not overwrite mapped DLLs.
        var package = Path.Combine(directory.Path, "stable package");
        Directory.CreateDirectory(package);
        foreach (var file in Directory.GetFiles(fixture.Root).Where(file => Path.GetFileName(file) == "QueueLoom.exe" ||
                     Path.GetFileName(file).StartsWith("QueueLoom.Launcher.", StringComparison.Ordinal) ||
                     Path.GetFileName(file).StartsWith("QueueLoom.bootstrap.", StringComparison.Ordinal)))
            File.Copy(file, Path.Combine(package, Path.GetFileName(file)));
        var repository = FindRepository();
        var scriptPath = Path.Combine(repository, ".github", "scripts", "smoke-update.ps1");
        var entry = scriptPath;
        if (wrapper)
        {
            entry = Path.Combine(directory.Path, "controlled-cim.ps1");
            File.WriteAllText(entry, """
                param([string] $Executable, [string] $FixtureDirectory, [string] $StablePackageDirectory)
                $ErrorActionPreference = 'Stop'
                function global:Get-CimInstance {
                    param([string] $ClassName, [string] $Filter)
                    if ($env:QUEUELOOM_SMOKE_DIAGNOSTICS_FAILURE -eq '1' -and $Filter.StartsWith('Name =')) {
                        throw 'Injected diagnostics CIM failure'
                    }
                    $rows = @(CimCmdlets\Get-CimInstance -ClassName $ClassName -Filter $Filter)
                    if ($env:QUEUELOOM_SMOKE_PAYLOAD_IDENTITY -and $Filter.StartsWith('ParentProcessId =')) {
                        $identity = [IO.File]::ReadAllText($env:QUEUELOOM_SMOKE_PAYLOAD_IDENTITY) | ConvertFrom-Json
                        foreach ($row in $rows) {
                            [pscustomobject]@{ ProcessId = $identity.Pid; ParentProcessId = $row.ParentProcessId;
                                ExecutablePath = $row.ExecutablePath; CommandLine = $row.CommandLine;
                                CreationDate = [DateTime]::new([long]$identity.StartTicks, [DateTimeKind]::Utc) }
                        }
                    } else { $rows }
                }
                & $env:QUEUELOOM_SMOKE_SCRIPT -Executable $Executable -FixtureDirectory $FixtureDirectory -StablePackageDirectory $StablePackageDirectory
                """);
        }
        var start = new ProcessStartInfo("pwsh") { UseShellExecute = false, CreateNoWindow = true,
            WorkingDirectory = repository, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var arg in new[] { "-NoProfile", "-File", entry, "-Executable",
                     Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture.exe"),
                     "-FixtureDirectory", Path.Combine(AppContext.BaseDirectory, "UpdateFixture"), "-StablePackageDirectory", package })
            start.ArgumentList.Add(arg);
        start.Environment["QUEUELOOM_SMOKE_SCRIPT"] = scriptPath;
        return start;
    }
}
