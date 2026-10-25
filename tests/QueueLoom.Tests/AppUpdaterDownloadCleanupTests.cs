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
