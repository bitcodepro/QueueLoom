using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Updates;

namespace QueueLoom.Tests;

public sealed partial class AppUpdaterTests
{
    private static FailedStartup RolledBack(string failed, bool noticed = false) => new(
        new VersionReference(Guid.NewGuid().ToString("N"), new string('a', 64)), failed,
        new VersionReference(Guid.NewGuid().ToString("N"), new string('b', 64)), "1.4.0", noticed);

    private static UpdateCheckResult Release(string version) =>
        new(Version.Parse(version), "v" + version, new Uri("https://github.com/bitcodepro/QueueLoom/releases/tag/v" + version));

    [Fact]
    public void ReleaseThatFailedToStartIsExplainedOnceThenNotOfferedAutomaticallyButANewerOneIs()
    {
        Assert.True(FailedUpdateNotice.ShouldShow(Release("1.5.0"), null));
        Assert.True(FailedUpdateNotice.ShouldShow(Release("1.5.0"), RolledBack("1.5.0")));
        Assert.False(FailedUpdateNotice.ShouldShow(Release("1.5.0"), RolledBack("1.5.0", noticed: true)));
        Assert.True(FailedUpdateNotice.ShouldShow(Release("1.5.1"), RolledBack("1.5.0", noticed: true)));
        Assert.True(FailedUpdateNotice.ShouldShow(Release("1.5.0"), RolledBack(null!, noticed: true)));
        Assert.Null(FailedUpdateNotice.Read(Target(OperatingSystem.IsWindows() ? "win-x64" : "linux-x64")));
    }

    [Fact]
    public async Task UpdateDialogExplainsTheRollbackAndStillInstallsTheFailedReleaseOnRequest()
    {
        var installs = 0;
        var dialog = new UpdateDialogViewModel("1.4.0", Release("1.5.0"), (_, _) => { installs++; return Task.CompletedTask; },
            failedStartup: RolledBack("1.5.0"));
        Assert.True(dialog.IsRolledBack);
        Assert.Equal("QueueLoom 1.5.0 failed to start", dialog.Title);
        Assert.Contains("QueueLoom 1.5.0 failed to start, and QueueLoom returned to version 1.4.0.", dialog.Message, StringComparison.Ordinal);
        Assert.Contains("not offered again automatically", dialog.Message, StringComparison.Ordinal);
        Assert.False(dialog.ShowInstallButton);
        Assert.True(dialog.ShowInstallAnyway);

        await dialog.InstallAsync();
        Assert.Equal(1, installs);
        Assert.True(dialog.IsReady);
        Assert.False(dialog.ShowInstallAnyway);

        var newer = new UpdateDialogViewModel("1.4.0", Release("1.5.1"), (_, _) => Task.CompletedTask, failedStartup: RolledBack("1.5.0"));
        Assert.False(newer.IsRolledBack);
        Assert.True(newer.ShowInstallButton);
        Assert.False(newer.ShowInstallAnyway);
        Assert.Equal("QueueLoom 1.5.1 is available", newer.Title);
        Assert.StartsWith("QueueLoom 1.5.0 failed to start, and QueueLoom returned to version 1.4.0. You have 1.4.0.", newer.Message, StringComparison.Ordinal);
    }
}
