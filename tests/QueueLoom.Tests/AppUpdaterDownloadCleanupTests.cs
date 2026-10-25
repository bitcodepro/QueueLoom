using QueueLoom.App.Services;

namespace QueueLoom.Tests;

public sealed partial class AppUpdaterTests
{
    [Fact]
    public async Task Install_CancelledAfterUnpacking_RemovesTheDownloadFolder()
    {
        var target = Target("win-x64");
        File.WriteAllText(target.Executable, "old program");
        var package = Zip(("QueueLoom.exe", "new program"));
        var downloads = Path.Combine(_root, "download");
        var staging = await new AppUpdater(Serve(package, Sha(package)), downloads)
            .DownloadAsync(Update, target, null, CancellationToken.None);
        using var cancel = new CancellationTokenSource();
        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            AppUpdater.InstallWithProgressAsync(target, staging, new Progress<UpdateProgress>(), cancel.Token));

        Assert.Empty(Directory.GetDirectories(downloads));
        Assert.Equal("old program", File.ReadAllText(target.Executable));
    }

    [Fact]
    public async Task Install_FailingWithoutTheProgram_RemovesTheDownloadFolder()
    {
        var target = Target("win-x64");
        File.WriteAllText(target.Executable, "old program");
        var package = Zip(("README.md", "new readme"));
        var downloads = Path.Combine(_root, "download");
        var staging = await new AppUpdater(Serve(package, Sha(package)), downloads)
            .DownloadAsync(Update, target, null, CancellationToken.None);

        Assert.Throws<InvalidOperationException>(() => AppUpdater.Install(target, staging));

        Assert.Empty(Directory.GetDirectories(downloads));
        Assert.False(File.Exists(UpdateRestart.ReceiptPath(target)));
        Assert.Equal("old program", File.ReadAllText(target.Executable));
    }

    [Fact]
    public void Sweep_RemovesOnlyStaleUnreferencedAttemptFolders()
    {
        var target = Target("win-x64");
        var downloads = Path.Combine(_root, "download");
        var stale = Attempt(downloads, "9.0.0", TimeSpan.FromDays(2));
        var referenced = Attempt(downloads, "9.0.1", TimeSpan.FromDays(2));
        var recent = Attempt(downloads, "9.0.2", TimeSpan.Zero);
        var foreign = Path.Combine(downloads, "keep-me");
        Directory.CreateDirectory(foreign);
        Directory.SetLastWriteTimeUtc(foreign, DateTime.UtcNow.AddDays(-2));
        var id = Path.GetFileName(referenced)[6..];
        File.WriteAllText(UpdateRestart.ReceiptPath(target), System.Text.Json.JsonSerializer.Serialize(
            new UpdateRestart.Receipt(id, target, referenced, [])));

        new AppUpdater(new HttpClient(), downloads).SweepStaleDownloads(target);

        Assert.False(Directory.Exists(stale));
        Assert.True(Directory.Exists(referenced));
        Assert.True(Directory.Exists(recent));
        Assert.True(Directory.Exists(foreign));
    }

    [Fact]
    public async Task Download_SweepsOffTheCallingThreadAndStopsWhenCancelled()
    {
        var target = Target("win-x64");
        var downloads = Path.Combine(_root, "download");
        var first = Attempt(downloads, "9.0.0", TimeSpan.FromDays(2));
        var second = Attempt(downloads, "9.0.1", TimeSpan.FromDays(2));
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        using var cancel = new CancellationTokenSource();
        var updater = new AppUpdater(new HttpClient(), downloads)
        {
            // A slow filesystem: the first folder blocks until the caller has regained control and cancelled.
            SweepingFolder = _ => { entered.Set(); release.Wait(TimeSpan.FromSeconds(10)); },
        };

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var download = updater.DownloadAsync(Update, target, null, cancel.Token);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5), "DownloadAsync blocked its caller on the sweep.");
        Assert.True(entered.Wait(TimeSpan.FromSeconds(10)));
        await cancel.CancelAsync();
        release.Set();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => download);
        // Only one of the two stale folders was reached before cancellation.
        Assert.Single(new[] { first, second }, Directory.Exists);
    }

    private static string Attempt(string downloads, string version, TimeSpan age)
    {
        var id = Guid.NewGuid().ToString("N");
        var folder = Path.Combine(downloads, version + "-" + id);
        Directory.CreateDirectory(Path.Combine(folder, "files"));
        File.WriteAllText(Path.Combine(folder, UpdateRestart.DownloadMarker), id);
        File.WriteAllText(Path.Combine(folder, "files", "QueueLoom.exe"), "program");
        var time = DateTime.UtcNow - age;
        foreach (var entry in Directory.EnumerateFiles(folder, "*", SearchOption.AllDirectories))
            File.SetLastWriteTimeUtc(entry, time);
        Directory.SetLastWriteTimeUtc(Path.Combine(folder, "files"), time);
        Directory.SetLastWriteTimeUtc(folder, time);
        return folder;
    }
}
