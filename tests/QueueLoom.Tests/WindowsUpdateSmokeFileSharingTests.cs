using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class WindowsUpdateSmokeTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(7)]
    public async Task ActualFixtureReleaseReaderCanReadWhileARenameHandleRemainsOpen(int exitCode)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows file sharing semantics require Windows."); return; }
        using var directory = new TemporaryDirectory();
        var release = Path.Combine(directory.Path, "release");
        PublishRelease(release, exitCode);
        // A rename needs DELETE access. Holding that access reproduces the sharing requirement
        // without depending on how long File.Move keeps its own handle open.
        using var rename = OpenSmokeRenameHandle(release, 0x00010000,
            FileShare.Read | FileShare.Write | FileShare.Delete, IntPtr.Zero, 3, 0x80, IntPtr.Zero);
        if (rename.IsInvalid) throw new Win32Exception(Marshal.GetLastWin32Error());
        var start = new ProcessStartInfo(Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture.exe"))
            { UseShellExecute = false, CreateNoWindow = true, RedirectStandardError = true };
        foreach (var argument in new[] { "--smoke-release-reader", release, release + ".visible" }) start.ArgumentList.Add(argument);
        using var reader = Process.Start(start)!;
        _ = reader.Handle;
        LogSmokeOutput($"Reader PID {reader.Id}, start ticks {reader.StartTime.ToUniversalTime().Ticks}; fixture {start.FileName}.");
        var error = reader.StandardError.ReadToEndAsync();
        try
        {
            await reader.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(45));
            LogSmokeOutput($"Requested reader exit: {exitCode}; observed reader exit: {reader.ExitCode}.");
            Assert.Equal(exitCode, reader.ExitCode);
            Assert.Equal(exitCode.ToString(System.Globalization.CultureInfo.InvariantCulture), ReadPublishedSmokeFile(release));
        }
        finally
        {
            if (!reader.HasExited) { reader.Kill(); await reader.WaitForExitAsync(); }
            LogSmokeOutput("Actual fixture reader stderr:\n" + await error);
        }
    }

    [Fact]
    public async Task ActualFixtureReleaseReaderStillRejectsAnOpenWriter()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows file sharing semantics require Windows."); return; }
        using var directory = new TemporaryDirectory();
        var release = Path.Combine(directory.Path, "release");
        PublishRelease(release, 7);
        using var writer = new FileStream(release, FileMode.Open, FileAccess.Write,
            FileShare.Read | FileShare.Write | FileShare.Delete);
        var start = new ProcessStartInfo(Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture.exe"))
            { UseShellExecute = false, CreateNoWindow = true, RedirectStandardError = true };
        foreach (var argument in new[] { "--smoke-release-reader", release, release + ".visible" }) start.ArgumentList.Add(argument);
        using var reader = Process.Start(start)!;
        _ = reader.Handle;
        LogSmokeOutput($"Open-writer control PID {reader.Id}, start ticks {reader.StartTime.ToUniversalTime().Ticks}; fixture {start.FileName}.");
        var error = reader.StandardError.ReadToEndAsync();
        try
        {
            await reader.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(45));
            LogSmokeOutput($"Reader exit while writer is held: {reader.ExitCode}.");
            Assert.NotEqual(7, reader.ExitCode);
            Assert.Contains("System.IO.IOException", await error, StringComparison.Ordinal);
            Assert.Throws<IOException>(() => ReadPublishedSmokeFile(release));
        }
        finally
        {
            if (!reader.HasExited) { reader.Kill(); await reader.WaitForExitAsync(); }
            LogSmokeOutput("Open-writer control stderr:\n" + await error);
        }
    }

    [Fact]
    public async Task ActualObserverExitCannotStandInForAPublishedFixtureMarker()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("The packaged GUI smoke script requires Windows."); return; }
        using var directory = new TemporaryDirectory();
        var repository = FindRepository();
        var start = new ProcessStartInfo("pwsh") { UseShellExecute = false, CreateNoWindow = true,
            WorkingDirectory = repository, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var argument in new[] { "-NoProfile", "-File", Path.Combine(repository, ".github", "scripts", "smoke-update.ps1"),
                     "-Executable", Path.Combine(directory.Path, "missing fixture.exe") }) start.ArgumentList.Add(argument);
        using var script = StartPowerShell(start);
        var output = script.StandardOutput.ReadToEndAsync();
        var error = script.StandardError.ReadToEndAsync();
        var exited = script.WaitForExitAsync();
        try
        {
            var failure = await Record.ExceptionAsync(() => WaitForPublishedSmokeMarker(Path.Combine(directory.Path, "missing marker"), exited));
            Assert.NotNull(failure);
            Assert.Contains("The observer exited before", failure.Message, StringComparison.Ordinal);
            await exited;
            Assert.NotEqual(0, script.ExitCode);
        }
        finally
        {
            if (!script.HasExited) { script.Kill(entireProcessTree: true); await exited; }
            LogSmokeOutput($"Missing-marker observer final exit: {script.ExitCode}.");
            LogSmokeOutput("Missing-marker observer stdout:\n" + await output + "\nObserver stderr:\n" + await error);
        }
    }

    private static string ReadPublishedSmokeFile(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    [DllImport("kernel32.dll", EntryPoint = "CreateFileW", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle OpenSmokeRenameHandle(string path, uint access, FileShare share,
        IntPtr securityAttributes, uint creationDisposition, uint flagsAndAttributes, IntPtr templateFile);
}
