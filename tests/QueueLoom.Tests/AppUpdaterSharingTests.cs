using System.Diagnostics;
using QueueLoom.App.Services;

namespace QueueLoom.Tests;

public sealed partial class AppUpdaterTests
{
    // An antivirus scanner briefly holds the freshly staged program open (without sharing deletion), so the rename
    // that installs it hits a sharing violation on Windows. The update waits for the file instead of failing.
    [Fact]
    public async Task InstallWaitsForAStagedFileThatIsBrieflyOpenElsewhere()
    {
        var target = Target(OperatingSystem.IsWindows() ? "win-x64" : "linux-x64");
        File.WriteAllText(target.Executable, "previous executable");
        var staging = Staging((Path.GetFileName(target.Executable), "updated executable"));
        var scanner = new FileStream(Path.Combine(staging, Path.GetFileName(target.Executable)), FileMode.Open, FileAccess.Read, FileShare.Read);
        var released = Task.Run(async () =>
        {
            await Task.Delay(500);
            scanner.Dispose();
        });

        AppUpdater.Install(target, staging);
        await released;

        Assert.Equal("updated executable", File.ReadAllText(target.Executable));
    }

    // Only "in use" is waited for: any other failure (here a missing file) is reported at once.
    [Fact]
    public void OtherRenameFailuresAreNotRetried()
    {
        var watch = Stopwatch.StartNew();
        Assert.Throws<FileNotFoundException>(() =>
            UpdateRestart.MoveRetrying(() => File.Move(Path.Combine(_root, "missing"), Path.Combine(_root, "elsewhere"))));
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(2), $"Took {watch.Elapsed}.");
    }
}
