using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.Core.Updates;
using QueueLoom.Tests.Infrastructure;
using InstallationFixture = QueueLoom.Tests.StableLauncherTests.InstallationFixture;

namespace QueueLoom.Tests;

public sealed partial class StableUpdateCleanupRegressionTests
{
    [Theory]
    [InlineData("{")]
    [InlineData("null")]
    [InlineData("{}")]
    public async Task Review_InvalidReceiptsPreserveEvidenceAndDoNotBlockOtherCleanupOrDiscovery(string invalidJson)
    {
        using var fixture = new InstallationFixture();
        using var downloads = new TemporaryDirectory();
        using var http = new HttpClient(new PackageHandler(Archive(fixture.Package("receipt-review").Path)));
        var updater = new AppUpdater(http, downloads.Path);
        var target = AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher);
        var installation = new VersionInstallation(fixture.Launcher);
        installation.EnsureBootstrap();
        var (invalidDirectory, invalidReceipt) = await ReviewDownload(updater, target);
        var path = WriteReviewReceipt(installation, invalidReceipt, invalidJson);
        var (foreignDirectory, _) = await ReviewDownload(updater, target);
        var invalidSnapshot = ReviewSnapshot(invalidDirectory);
        var foreignSnapshot = ReviewSnapshot(foreignDirectory);
        await AssertReviewCleanupAndDiscovery(updater, target, installation);
        Assert.Equal(invalidJson, File.ReadAllText(path));
        Assert.Equal(invalidSnapshot, ReviewSnapshot(invalidDirectory));
        Assert.Equal(foreignSnapshot, ReviewSnapshot(foreignDirectory));
    }

    [Fact]
    public async Task Review_MovedPortableInstallationPreservesOldReceiptWithoutRebindingItsDownload()
    {
        using var fixture = new InstallationFixture();
        using var moved = new TemporaryDirectory();
        using var downloads = new TemporaryDirectory();
        using var http = new HttpClient(new PackageHandler(Archive(fixture.Package("move-review").Path)));
        var updater = new AppUpdater(http, downloads.Path);
        var originalTarget = AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher);
        var originalInstallation = new VersionInstallation(fixture.Launcher);
        originalInstallation.EnsureBootstrap();
        var (staleDirectory, staleReceipt) = await ReviewDownload(updater, originalTarget);
        WriteReviewReceipt(originalInstallation, staleReceipt);
        var staleSnapshot = ReviewSnapshot(staleDirectory);
        var (foreignDirectory, _) = await ReviewDownload(updater, originalTarget);
        var foreignSnapshot = ReviewSnapshot(foreignDirectory);
        var movedRoot = Path.Combine(moved.Path, "moved portable installation");
        var launcherRelative = Path.GetRelativePath(fixture.Root, fixture.Launcher);
        var storeRelative = Path.GetRelativePath(fixture.Root, originalInstallation.Store);
        var temporaryBoundary = Path.GetFullPath(Path.Combine(Path.GetTempPath(), "QueueLoom.Tests")) + Path.DirectorySeparatorChar;
        Assert.StartsWith(temporaryBoundary, Path.GetFullPath(fixture.Root), StringComparison.Ordinal);
        Assert.StartsWith(temporaryBoundary, Path.GetFullPath(movedRoot), StringComparison.Ordinal);
        Directory.Move(fixture.Root, movedRoot); // The actual portable store and retained receipt move together.
        var launcher = Path.Combine(movedRoot, launcherRelative);
        var target = AppUpdater.TargetFor(VersionInstallation.CurrentRid(), launcher);
        var installation = new VersionInstallation(launcher);
        Assert.Equal(Path.Combine(movedRoot, storeRelative), installation.Store);
        Assert.True(File.Exists(installation.Verify(installation.Bootstrap)));
        await AssertReviewCleanupAndDiscovery(updater, target, installation);
        var retained = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(
            Path.Combine(installation.Store, "download-cleanup", staleReceipt.Id + ".json")))!;
        Assert.Equal(originalTarget, retained.Target);
        Assert.Equal(staleSnapshot, ReviewSnapshot(staleDirectory));
        Assert.Equal(foreignSnapshot, ReviewSnapshot(foreignDirectory));
    }

    [Fact]
    public async Task Review_LinkedVersionStoreMakesInstallationIneligibleWithoutThrowing()
    {
        using var fixture = new InstallationFixture();
        using var external = new TemporaryDirectory();
        File.WriteAllText(Path.Combine(external.Path, "keep"), "external data");
        var target = AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher);
        var installation = new VersionInstallation(fixture.Launcher);
        var link = installation.Store;
        await CreateCleanupReviewLink(link, external.Path);
        try
        {
            Assert.False(AppUpdater.CanInstall(target));
            Assert.Equal("external data", File.ReadAllText(Path.Combine(external.Path, "keep")));
            Assert.Single(Directory.GetFileSystemEntries(external.Path));
        }
        finally { RemoveCleanupReviewLink(link, fixture.Root); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Review_LinkedDownloadOrLeaseIsRejectedWithoutDeletingDataOrBlockingOtherReceipts(bool linkedLease)
    {
        using var fixture = new InstallationFixture();
        using var downloads = new TemporaryDirectory();
        using var external = new TemporaryDirectory();
        File.WriteAllText(Path.Combine(external.Path, "keep"), "external data");
        using var http = new HttpClient(new PackageHandler(Archive(fixture.Package("link-review").Path)));
        var updater = new AppUpdater(http, downloads.Path);
        var target = AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher);
        var installation = new VersionInstallation(fixture.Launcher);
        installation.EnsureBootstrap();
        var (directory, receipt) = await ReviewDownload(updater, target);
        var snapshot = ReviewSnapshot(directory);
        var path = WriteReviewReceipt(installation, receipt);
        var link = linkedLease ? path + ".handoff" : Path.Combine(directory, "linked");
        await CreateCleanupReviewLink(link, external.Path);
        try
        {
            await AssertReviewCleanupAndDiscovery(updater, target, installation);
            Assert.True(File.Exists(path));
            Assert.Equal(receipt.Id, File.ReadAllText(Path.Combine(directory, UpdateRestart.DownloadMarker)));
            Assert.Equal("external data", File.ReadAllText(Path.Combine(external.Path, "keep")));
            Assert.Single(Directory.GetFileSystemEntries(external.Path));
        }
        finally { RemoveCleanupReviewLink(link, linkedLease ? installation.Store : downloads.Path); }
        Assert.Equal(snapshot, ReviewSnapshot(directory));
    }

    private static async Task AssertReviewCleanupAndDiscovery(AppUpdater updater, UpdateTarget target, VersionInstallation installation)
    {
        using var handler = new CleanupReviewReleaseHandler();
        using var http = new HttpClient(handler);
        using var checker = new GitHubUpdateChecker(http, "1.0.0");
        for (var launch = 0; launch < 3; launch++)
        {
            // A fresh valid transaction must be processed on every launch despite the retained bad receipt.
            var (directory, receipt) = await ReviewDownload(updater, target);
            var path = WriteReviewReceipt(installation, receipt);
            updater.CleanUpPreviousUpdate(target);
            var update = await checker.CheckAsync(); // The production cleanup -> discovery sequence.
            Assert.NotNull(update);
            Assert.Equal(new Version(9, 1, 0), update.Version);
            Assert.False(Directory.Exists(directory));
            Assert.False(File.Exists(path));
        }
        Assert.Equal(3, handler.Requests);
    }

    private static async Task<(string Directory, UpdateRestart.Receipt Receipt)> ReviewDownload(AppUpdater updater, UpdateTarget target)
    {
        var update = new UpdateCheckResult(new Version(9, 1, 0), "v9.1.0", new Uri("https://github.com/bitcodepro/QueueLoom/releases/tag/v9.1.0"));
        var staging = await updater.DownloadAsync(update, target, null, default);
        var directory = Path.GetDirectoryName(staging)!;
        var id = File.ReadAllText(Path.Combine(directory, UpdateRestart.DownloadMarker));
        return (directory, new UpdateRestart.Receipt(id, target, directory, []));
    }

    private static string WriteReviewReceipt(VersionInstallation installation, UpdateRestart.Receipt receipt, string? json = null)
    {
        var folder = Path.Combine(installation.Store, "download-cleanup");
        Directory.CreateDirectory(folder);
        var path = Path.Combine(folder, receipt.Id + ".json");
        File.WriteAllText(path, json ?? JsonSerializer.Serialize(receipt));
        return path;
    }

    private static string[] ReviewSnapshot(string directory) => Directory.GetFiles(directory, "*", SearchOption.AllDirectories)
        .Select(file => Path.GetRelativePath(directory, file) + ":" + Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(file))))
        .Order(StringComparer.Ordinal).ToArray();

    private static async Task CreateCleanupReviewLink(string link, string target)
    {
        if (!OperatingSystem.IsWindows()) { Directory.CreateSymbolicLink(link, target); return; }
        var start = new ProcessStartInfo("cmd.exe") { UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var argument in new[] { "/c", "mklink", "/J", link, target }) start.ArgumentList.Add(argument);
        using var process = Process.Start(start)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        try { await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(30)); }
        finally { if (!process.HasExited) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(); } }
        Assert.True(process.ExitCode == 0, await output + await error);
    }

    private static void RemoveCleanupReviewLink(string link, string boundary)
    {
        Assert.StartsWith(Path.GetFullPath(boundary) + Path.DirectorySeparatorChar, Path.GetFullPath(link), StringComparison.Ordinal);
        Assert.True((File.GetAttributes(link) & FileAttributes.ReparsePoint) != 0);
        Directory.Delete(link, recursive: false);
    }

    private sealed class CleanupReviewReleaseHandler : HttpMessageHandler
    {
        public int Requests { get; private set; }
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Assert.Equal("api.github.com", request.RequestUri!.Host);
            Requests++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""[{"draft":false,"prerelease":false,"tag_name":"v9.1.0"}]""")
            });
        }
    }
}
