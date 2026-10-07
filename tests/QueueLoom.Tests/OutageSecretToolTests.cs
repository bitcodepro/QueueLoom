using System.Diagnostics;
using System.Reflection;
using QueueLoom.Infrastructure.Security;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

[Collection("Fake secret-tool")]
public sealed class OutageSecretToolTests
{
    [Fact]
    public async Task Outage_CancelledSecretToolCannotFinishAfterTheNextStore()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Portable Windows fixture; Linux backend has its own gated test."); return; }
        using var directory = new TemporaryDirectory();
        var fixture = Path.Combine(AppContext.BaseDirectory, "UpdateFixture");
        foreach (var file in Directory.EnumerateFiles(fixture)) File.Copy(file, Path.Combine(directory.Path, Path.GetFileName(file)));
        File.Copy(Path.Combine(fixture, "QueueLoom.UpdateFixture.exe"), Path.Combine(directory.Path, "secret-tool.exe"));
        var oldPath = Environment.GetEnvironmentVariable("PATH");
        Environment.SetEnvironmentVariable("PATH", directory.Path + Path.PathSeparator + oldPath);
        using var cancellation = new CancellationTokenSource();
        Process? first = null;
        try
        {
            var method = typeof(LinuxSecretServiceMasterKeyStore).GetMethod("RunSecretToolAsync", BindingFlags.Static | BindingFlags.NonPublic)!;
            Task Store(string value, CancellationToken token) => (Task)method.Invoke(null, [new[] { "store" }, value, token])!;
            var pending = Store("fake-key-A", cancellation.Token);
            var marker = Path.Combine(directory.Path, "first-store.pid");
            for (var i = 0; i < 200 && !File.Exists(marker); i++) await Task.Delay(10);
            Assert.True(File.Exists(marker), "The isolated child did not reach the barrier.");
            first = Process.GetProcessById(int.Parse(File.ReadAllText(marker), System.Globalization.CultureInfo.InvariantCulture));
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
            // Exercise the race all the way through, rather than only checking a disposal flag.
            await Store("fake-key-B", default);
            File.WriteAllText(Path.Combine(directory.Path, "release-first-store"), "release");
            if (!first.HasExited) await first.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("fake-key-B", File.ReadAllText(Path.Combine(directory.Path, "stored")));
        }
        finally
        {
            if (first is not null) { if (!first.HasExited) { first.Kill(entireProcessTree: true); await first.WaitForExitAsync(); } first.Dispose(); }
            Environment.SetEnvironmentVariable("PATH", oldPath);
        }
    }
}
