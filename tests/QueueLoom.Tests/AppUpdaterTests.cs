using System.Formats.Tar;
using System.IO.Compression;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;

namespace QueueLoom.Tests;

public sealed class AppUpdaterTests : IDisposable
{
    private static readonly UpdateCheckResult Update =
        new(new Version(9, 1, 0), "v9.1.0", new Uri("https://github.com/bitcodepro/QueueLoom/releases/tag/v9.1.0"));
    private readonly string _root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "updater", Guid.NewGuid().ToString("N"));

    public AppUpdaterTests() => Directory.CreateDirectory(_root);

    public void Dispose() => Directory.Delete(_root, recursive: true);

    [Fact]
    public void Packages_AreNamedLikeTheReleaseAssets()
    {
        Assert.Equal("QueueLoom-9.1.0-win-x64.zip", AppUpdater.PackageName("9.1.0", "win-x64"));
        Assert.Equal("QueueLoom-9.1.0-linux-x64.tar.gz", AppUpdater.PackageName("9.1.0", "linux-x64"));
        Assert.Equal(
            "https://github.com/bitcodepro/QueueLoom/releases/download/v9.1.0/QueueLoom-9.1.0-osx-arm64.zip",
            AppUpdater.PackageUri("v9.1.0", "9.1.0", "osx-arm64").ToString());
    }

    [Fact]
    public void MacTarget_IsTheAppBundle()
    {
        var target = AppUpdater.TargetFor("osx-arm64", "/Applications/QueueLoom.app/Contents/MacOS/QueueLoom");

        Assert.Equal("/Applications/QueueLoom.app", target.Bundle);
        Assert.Equal("/Applications", target.InstallDirectory);
    }

    [Fact]
    public async Task Download_VerifiesTheChecksumAndUnpacks()
    {
        var package = Zip(("QueueLoom.exe", "new program"), ("README.md", "new readme"));
        var updater = new AppUpdater(Serve(package, Sha(package)), Path.Combine(_root, "download"));

        var staging = await updater.DownloadAsync(Update, Target("win-x64"), null, CancellationToken.None);

        Assert.Equal("new program", File.ReadAllText(Path.Combine(staging, "QueueLoom.exe")));
    }

    [Fact]
    public async Task Download_WithAWrongChecksum_InstallsNothing()
    {
        var package = Zip(("QueueLoom.exe", "tampered"));
        var updater = new AppUpdater(Serve(package, new string('0', 64)), Path.Combine(_root, "download"));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            updater.DownloadAsync(Update, Target("win-x64"), null, CancellationToken.None));

        Assert.Contains("checksum", error.Message, StringComparison.Ordinal);
        Assert.False(Directory.Exists(Path.Combine(_root, "download", "9.1.0", "files")));
    }

    [Fact]
    public async Task LinuxPackages_AreTarGz()
    {
        var package = TarGz(("QueueLoom", "new program"), ("queueloom.png", "icon"));
        var updater = new AppUpdater(Serve(package, Sha(package)), Path.Combine(_root, "download"));

        var staging = await updater.DownloadAsync(Update, Target("linux-x64"), null, CancellationToken.None);

        Assert.Equal("new program", File.ReadAllText(Path.Combine(staging, "QueueLoom")));
    }

    [Fact]
    public void Install_KeepsTheOldFilesUntilTheNextStartAndThenRemovesThem()
    {
        var target = Target("win-x64");
        File.WriteAllText(target.Executable, "old program");
        var staging = Staging(("QueueLoom.exe", "new program"), ("README.md", "new readme"));

        AppUpdater.Install(target, staging);

        Assert.Equal("new program", File.ReadAllText(target.Executable));
        Assert.Equal("old program", File.ReadAllText(target.Executable + ".old"));
        Assert.Equal("new readme", File.ReadAllText(Path.Combine(target.InstallDirectory, "README.md")));

        new AppUpdater(new HttpClient(), Path.Combine(_root, "download")).CleanUpPreviousUpdate(target);
        Assert.False(File.Exists(target.Executable + ".old"));
    }

    [Fact]
    public void Install_WithoutTheProgram_LeavesEverythingAsItWas()
    {
        var target = Target("win-x64");
        File.WriteAllText(target.Executable, "old program");
        File.WriteAllText(Path.Combine(target.InstallDirectory, "README.md"), "old readme");
        var staging = Staging(("README.md", "new readme"));

        Assert.Throws<InvalidOperationException>(() => AppUpdater.Install(target, staging));

        Assert.Equal("old program", File.ReadAllText(target.Executable));
        Assert.Equal("old readme", File.ReadAllText(Path.Combine(target.InstallDirectory, "README.md")));
        Assert.False(File.Exists(target.Executable + ".old"));
    }

    [Fact]
    public void Install_ReplacesTheWholeMacBundle()
    {
        var bundle = Path.Combine(_root, "Applications", "QueueLoom.app");
        Directory.CreateDirectory(Path.Combine(bundle, "Contents", "MacOS"));
        File.WriteAllText(Path.Combine(bundle, "Contents", "MacOS", "QueueLoom"), "old program");
        var target = AppUpdater.TargetFor("osx-arm64", Path.Combine(bundle, "Contents", "MacOS", "QueueLoom"));
        var staging = Staging(("QueueLoom.app/Contents/MacOS/QueueLoom", "new program"), ("QueueLoom.app/Contents/Info.plist", "plist"));

        AppUpdater.Install(target, staging);

        Assert.Equal("new program", File.ReadAllText(Path.Combine(bundle, "Contents", "MacOS", "QueueLoom")));
        Assert.True(File.Exists(Path.Combine(bundle, "Contents", "Info.plist")));
        Assert.True(Directory.Exists(bundle + ".old"));
    }

    [Fact]
    public async Task Dialog_GoesFromAvailableThroughDownloadToReady()
    {
        var dialog = new UpdateDialogViewModel("1.4.0", Update, async (progress, _) =>
        {
            progress.Report(new UpdateProgress(5 * 1024 * 1024, 10 * 1024 * 1024));
            await Task.Yield();
        });
        Assert.True(dialog.ShowInstallButton);
        Assert.Contains("You have 1.4.0", dialog.Message, StringComparison.Ordinal);

        await dialog.InstallAsync();

        Assert.True(dialog.IsReady);
        Assert.Equal("QueueLoom 9.1.0 is installed", dialog.Title);
    }

    [Fact]
    public async Task Dialog_ReportsAFailedInstallAndOffersTheReleasesPage()
    {
        var failing = new UpdateDialogViewModel("1.4.0", Update,
            (_, _) => throw new InvalidOperationException("The downloaded package does not match its published checksum."));
        await failing.InstallAsync();
        Assert.True(failing.IsFailed);
        Assert.Contains("checksum", failing.Error, StringComparison.Ordinal);

        var readOnly = new UpdateDialogViewModel("1.4.0", Update, null, "QueueLoom cannot write to its folder (C:\\Program Files\\QueueLoom).");
        Assert.False(readOnly.CanInstall);
        Assert.False(readOnly.ShowInstallButton);
        Assert.Contains("Program Files", readOnly.Message, StringComparison.Ordinal);
    }

    private UpdateTarget Target(string rid)
    {
        var directory = Path.Combine(_root, "app");
        Directory.CreateDirectory(directory);
        return new UpdateTarget(rid, directory, Path.Combine(directory, rid.StartsWith("win", StringComparison.Ordinal) ? "QueueLoom.exe" : "QueueLoom"), null);
    }

    private string Staging(params (string Path, string Content)[] files)
    {
        var staging = Path.Combine(_root, "staging-" + Guid.NewGuid().ToString("N"));
        foreach (var (path, content) in files)
        {
            var full = Path.Combine(staging, path);
            Directory.CreateDirectory(Path.GetDirectoryName(full)!);
            File.WriteAllText(full, content);
        }
        return staging;
    }

    private static byte[] Zip(params (string Path, string Content)[] files)
    {
        using var output = new MemoryStream();
        using (var zip = new ZipArchive(output, ZipArchiveMode.Create, leaveOpen: true))
        {
            foreach (var (path, content) in files)
            {
                using var entry = zip.CreateEntry(path).Open();
                entry.Write(Encoding.UTF8.GetBytes(content));
            }
        }
        return output.ToArray();
    }

    private static byte[] TarGz(params (string Path, string Content)[] files)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
        using (var tar = new TarWriter(gzip))
        {
            foreach (var (path, content) in files)
            {
                tar.WriteEntry(new PaxTarEntry(TarEntryType.RegularFile, "./" + path)
                {
                    DataStream = new MemoryStream(Encoding.UTF8.GetBytes(content))
                });
            }
        }
        return output.ToArray();
    }

    private static string Sha(byte[] bytes) => Convert.ToHexStringLower(SHA256.HashData(bytes));

    private static HttpClient Serve(byte[] package, string checksum) => new(new PackageHandler(package, checksum));

    private sealed class PackageHandler(byte[] package, string checksum) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;
            Assert.StartsWith("/bitcodepro/QueueLoom/releases/download/v9.1.0/QueueLoom-9.1.0-", path, StringComparison.Ordinal);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = path.EndsWith(".sha256", StringComparison.Ordinal)
                    ? new StringContent($"{checksum}  {Path.GetFileName(path)[..^7]}")
                    : new ByteArrayContent(package)
            });
        }
    }
}
