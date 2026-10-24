using System.Diagnostics;
using QueueLoom.App.Services;
using QueueLoom.Core.Updates;
using QueueLoom.Tests.Infrastructure;
using InstallationFixture = QueueLoom.Tests.StableLauncherTests.InstallationFixture;

namespace QueueLoom.Tests;

public sealed class UpdateCleanupOffCallerThreadTests
{
    [Fact]
    public async Task CleanupWaitingForAnotherProcessesInstallationLockDoesNotBlockTheCaller()
    {
        using var fixture = new InstallationFixture();
        using var downloads = new TemporaryDirectory();
        var installation = new VersionInstallation(fixture.Launcher);
        installation.EnsureBootstrap();
        var target = AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher);
        var updater = new AppUpdater(new HttpClient(), downloads.Path);
        Task cleanup;
        // Another QueueLoom process staging an update holds the lock; the window opening now must stay responsive.
        using (new FileStream(Path.Combine(installation.Store, "installation.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
        {
            var elapsed = Stopwatch.StartNew();
            cleanup = updater.CleanUpPreviousUpdateAsync(target);
            Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(1), $"The caller was blocked for {elapsed.Elapsed}.");
            await Task.Delay(200);
            Assert.False(cleanup.IsCompleted);
        }
        await cleanup.WaitAsync(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task InstallProbeRunsOffTheCaller()
    {
        using var fixture = new InstallationFixture();
        var target = AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher);
        Assert.Equal(AppUpdater.CanInstall(target), await AppUpdater.CanInstallAsync(target));
    }
}
