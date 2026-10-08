using System.Collections.Concurrent;
using QueueLoom.Core.Updates;
using InstallationFixture = QueueLoom.Tests.StableLauncherTests.InstallationFixture;

namespace QueueLoom.Tests;

// Another program (an antivirus scan, an indexer) briefly holds an installation file open. The launcher read the
// sharing violation as a damaged payload or state record: a locked manifest or payload file rolled a healthy
// installation back to its previous version, and a locked newest state record was skipped for an older one. A locked
// staging marker stopped the launch. Each is now read again, and one held past the wait stops the launch unchanged.
public sealed class InstallationFileBusyTests
{
    public static TheoryData<string> LockedFiles => new() { "manifest", "payload", "state" };

    [Theory]
    [MemberData(nameof(LockedFiles))]
    public void ABrieflyLockedInstallationFileIsReadAgainInsteadOfRollingBack(string locked)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows sharing violations."); return; }
        using var fixture = new InstallationFixture();
        FileStream? scanner = null;
        var steps = new ConcurrentQueue<string>();
        var installation = new VersionInstallation(fixture.Launcher, step =>
        {
            steps.Enqueue(step);
            if (step == "installation-file-busy") scanner?.Dispose();
        });
        var next = Confirmed(installation, fixture);

        scanner = Hold(PathOf(installation, next, locked));
        LaunchSelection selected;
        try { selected = installation.SelectForLaunch(); }
        finally { scanner.Dispose(); }

        Assert.Equal(next, selected.Version);
        Assert.Contains("installation-file-busy", steps);
        Assert.DoesNotContain("recover-state-published", steps);
        Assert.Equal(next, installation.SelectForLaunch().Version);
    }

    [Theory]
    [MemberData(nameof(LockedFiles))]
    public void AnInstallationFileLockedPastTheWaitStopsTheLaunchWithoutRollingBack(string locked)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows sharing violations."); return; }
        using var fixture = new InstallationFixture();
        var steps = new ConcurrentQueue<string>();
        var installation = new VersionInstallation(fixture.Launcher, steps.Enqueue);
        var next = Confirmed(installation, fixture);

        using (Hold(PathOf(installation, next, locked)))
        {
            var error = Assert.Throws<InstallationFileBusyException>(() => installation.SelectForLaunch());
            Assert.IsAssignableFrom<IOException>(error.InnerException);
        }

        Assert.Contains("installation-file-busy", steps);
        Assert.DoesNotContain("recover-state-published", steps);
        // Nothing was committed while the file was held: once released, the same version launches.
        Assert.Equal(next, installation.SelectForLaunch().Version);
        Assert.DoesNotContain("recover-state-published", steps);
    }

    [Fact]
    public void AMissingInstallationFileIsStillDamageAndRecoversAtOnce()
    {
        using var fixture = new InstallationFixture();
        var steps = new ConcurrentQueue<string>();
        var installation = new VersionInstallation(fixture.Launcher, steps.Enqueue);
        var next = Confirmed(installation, fixture);
        File.Delete(PathOf(installation, next, "payload"));

        Assert.Equal(installation.Bootstrap, installation.SelectForLaunch().Version);
        Assert.Contains("recover-state-published", steps);
        Assert.DoesNotContain("installation-file-busy", steps);
    }

    [Fact]
    public void ALockedStagingMarkerLeavesItsDirectoryAndTheLaunchGoesOn()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows sharing violations."); return; }
        using var fixture = new InstallationFixture();
        var installation = new VersionInstallation(fixture.Launcher);
        var next = Confirmed(installation, fixture);
        var id = Guid.NewGuid().ToString("N");
        var leftover = Path.Combine(installation.Store, "staging", id);
        Directory.CreateDirectory(leftover);
        File.WriteAllText(Path.Combine(leftover, ".staging-owner"), id);
        File.WriteAllText(Path.Combine(leftover, "partial"), "x");

        using (Hold(Path.Combine(leftover, ".staging-owner")))
            installation.CleanupStaging();
        Assert.True(Directory.Exists(leftover));

        installation.CleanupStaging();
        Assert.False(Directory.Exists(leftover));
        Assert.Equal(next, installation.SelectForLaunch().Version);
    }

    private static VersionReference Confirmed(VersionInstallation installation, InstallationFixture fixture)
    {
        installation.StagePackage(fixture.Package("next").Path);
        var selected = installation.SelectForLaunch();
        installation.Acknowledge(selected.Version, selected.Attempt!);
        installation.Confirm(selected);
        Assert.False(installation.HasPendingActivation());
        return selected.Version;
    }

    private static string PathOf(VersionInstallation installation, VersionReference version, string locked)
    {
        var directory = Path.Combine(installation.Store, "versions", version.Id);
        return locked switch
        {
            "manifest" => Path.Combine(directory, "manifest.json"),
            "payload" => installation.Verify(version),
            "state" => Directory.GetFiles(Path.Combine(installation.Store, "state"), "*.json").Max()!,
            _ => throw new ArgumentOutOfRangeException(nameof(locked)),
        };
    }

    private static FileStream Hold(string path) => new(path, FileMode.Open, FileAccess.Read, FileShare.None);
}
