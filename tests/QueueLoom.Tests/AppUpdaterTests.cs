using System.Formats.Tar;
using System.IO.Compression;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;

namespace QueueLoom.Tests;

public sealed partial class AppUpdaterTests : IDisposable
{
    [Fact]
    public void MissingRollbackBackupNeverClaimsThatPreviousVersionWasRestored()
    {
        var target = Target(OperatingSystem.IsWindows() ? "win-x64" : "linux-x64");
        File.WriteAllText(target.Executable, "previous executable");
        AppUpdater.Install(target, Staging((Path.GetFileName(target.Executable), "updated executable")));
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(UpdateRestart.ReceiptPath(target)))!;
        File.Delete(receipt.Entries.Single().Backup!);
        File.Delete(target.Executable);
        var error = Assert.ThrowsAny<IOException>(() => AppUpdater.StartInstalled(target));
        Assert.DoesNotContain("previous version was restored", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(File.Exists(UpdateRestart.ReceiptPath(target)));
    }

    [Fact]
    public void Restart_WhenTheHelperIsMissing_RestoresThePreviousFiles()
    {
        var target = Target(OperatingSystem.IsWindows() ? "win-x64" : "linux-x64");
        File.WriteAllText(target.Executable, "previous executable");
        AppUpdater.Install(target, Staging((Path.GetFileName(target.Executable), "updated executable")));
        File.Delete(target.Executable);
        var error = Assert.Throws<IOException>(() => AppUpdater.StartInstalled(target));
        Assert.Contains("previous version was restored", error.Message, StringComparison.Ordinal);
        Assert.Equal("previous executable", File.ReadAllText(target.Executable));
    }

    [Fact]
    public void Cleanup_PreservesUnrelatedOldAndBackupFiles()
    {
        var target = Target("win-x64");
        var notes = Path.Combine(target.InstallDirectory, "user-notes.old");
        var backup = Path.Combine(target.InstallDirectory, "user-settings.bak");
        File.WriteAllText(notes, "notes");
        File.WriteAllText(backup, "backup");
        new AppUpdater(new HttpClient(), Path.Combine(_root, "download")).CleanUpPreviousUpdate(target);
        Assert.Equal("notes", File.ReadAllText(notes));
        Assert.Equal("backup", File.ReadAllText(backup));
    }

    [Fact]
    public void Install_WithAnUnfinishedReceipt_LeavesTheInstallationUntouched()
    {
        var target = Target("win-x64");
        File.WriteAllText(target.Executable, "old program");
        File.WriteAllText(UpdateRestart.ReceiptPath(target), "previous transaction");
        Assert.Throws<IOException>(() => AppUpdater.Install(target, Staging(("QueueLoom.exe", "new program"))));
        Assert.Equal("old program", File.ReadAllText(target.Executable));
        Assert.Equal("previous transaction", File.ReadAllText(UpdateRestart.ReceiptPath(target)));
    }

    [Fact]
    public async Task CycleOne_CleanupWithLockedDownloadNeverDeletesAndRecreatesOwnershipProof()
    {
        if (!OperatingSystem.IsWindows()) return;
        var (target, _, receipt) = await DownloadAndInstall();
        var marker = Path.Combine(receipt.DownloadDirectory, UpdateRestart.DownloadMarker);
        var timestamp = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(marker, timestamp);
        var archive = Directory.GetFiles(receipt.DownloadDirectory, "*.zip").Single();
        using (var locked = new FileStream(archive, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            await UpdateRestart.CleanAsync(receipt, UpdateRestart.ReceiptPath(target), TimeSpan.Zero);
            Assert.True(File.Exists(UpdateRestart.ReceiptPath(target)));
            Assert.True(File.Exists(archive));
            Assert.Equal(receipt.Id, File.ReadAllText(marker));
            Assert.Equal(timestamp, File.GetLastWriteTimeUtc(marker));
        }
        await UpdateRestart.CleanAsync(receipt, UpdateRestart.ReceiptPath(target), TimeSpan.Zero);
        Assert.False(Directory.Exists(receipt.DownloadDirectory));
        Assert.False(File.Exists(UpdateRestart.ReceiptPath(target)));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task CycleOne_TerminalCleanupCrashRequiresItsExactProofAndAnEmptyDirectory(bool foreignProof, bool foreignFile)
    {
        var (target, _, receipt) = await DownloadAndInstall();
        var path = UpdateRestart.ReceiptPath(target);
        Directory.Delete(receipt.DownloadDirectory, true);
        Directory.CreateDirectory(receipt.DownloadDirectory);
        var proof = path + "." + receipt.Id + ".download-cleaned";
        File.WriteAllText(proof, foreignProof ? Guid.NewGuid().ToString("N") : receipt.Id);
        var extra = Path.Combine(receipt.DownloadDirectory, "unrelated");
        if (foreignFile) File.WriteAllText(extra, "keep");
        if (foreignProof || foreignFile)
        {
            await Assert.ThrowsAsync<InvalidDataException>(() => UpdateRestart.CleanAsync(receipt, path, TimeSpan.Zero));
            Assert.True(File.Exists(path));
            Assert.True(Directory.Exists(receipt.DownloadDirectory));
            if (foreignFile) Assert.Equal("keep", File.ReadAllText(extra));
        }
        else
        {
            await UpdateRestart.CleanAsync(receipt, path, TimeSpan.Zero);
            Assert.False(Directory.Exists(receipt.DownloadDirectory));
            Assert.False(File.Exists(path));
            Assert.False(File.Exists(proof));
        }
    }

    [Fact]
    public async Task Cleanup_WithALockedDownload_KeepsItsOwnershipMarkerUntilRetrySucceeds()
    {
        if (!OperatingSystem.IsWindows()) return;
        var target = Target("win-x64");
        File.WriteAllText(target.Executable, "old program");
        var package = Zip(("QueueLoom.exe", "new program"));
        var updater = new AppUpdater(Serve(package, Sha(package)), Path.Combine(_root, "download"));
        var staging = await updater.DownloadAsync(Update, target, null, CancellationToken.None);
        AppUpdater.Install(target, staging);
        var path = UpdateRestart.ReceiptPath(target);
        var receipt = System.Text.Json.JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
        var archive = Directory.GetFiles(receipt.DownloadDirectory, "*.zip").Single();
        var locked = new FileStream(archive, FileMode.Open, FileAccess.Read, FileShare.None);
        try
        {
            var cleaning = UpdateRestart.CleanAsync(receipt, path, TimeSpan.FromSeconds(5));
            await Task.Delay(250);
            Assert.False(cleaning.IsCompleted);
            Assert.Equal(receipt.Id, File.ReadAllText(Path.Combine(receipt.DownloadDirectory, UpdateRestart.DownloadMarker)));
            locked.Dispose();
            await cleaning;
            Assert.False(Directory.Exists(receipt.DownloadDirectory));
            Assert.False(File.Exists(path));
        }
        finally { locked.Dispose(); }
    }

    [Fact]
    public async Task SuccessfulStartup_RemovesOnlyItsOwnDownloadAndRecordedBackups()
    {
        var target = Target("win-x64");
        File.WriteAllText(target.Executable, "old program");
        var downloads = Path.Combine(_root, "download");
        var package = Zip(("QueueLoom.exe", "new program"));
        var updater = new AppUpdater(Serve(package, Sha(package)), downloads);
        var staging = await updater.DownloadAsync(Update, target, null, CancellationToken.None);
        AppUpdater.Install(target, staging);
        var other = Path.Combine(downloads, "other-installation");
        Directory.CreateDirectory(other);
        File.WriteAllText(Path.Combine(other, "keep"), "keep");
        MarkInstallerExited(target);
        updater.CleanUpPreviousUpdate(target);
        Assert.False(Directory.Exists(Path.GetDirectoryName(staging)));
        Assert.Equal("keep", File.ReadAllText(Path.Combine(other, "keep")));
    }
    [Fact]
    public async Task Cleanup_AfterDeletingDownloadWithLockedReceipt_CanRetryAndUpdateAgain()
    {
        if (!OperatingSystem.IsWindows()) return;
        var (target, updater, receipt) = await DownloadAndInstall();
        var path = UpdateRestart.ReceiptPath(target);
        using (var locked = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            await UpdateRestart.CleanAsync(receipt, path, TimeSpan.Zero);
            Assert.False(Directory.Exists(receipt.DownloadDirectory));
            Assert.True(File.Exists(path));
        }
        MarkInstallerExited(target);
        updater.CleanUpPreviousUpdate(target);
        Assert.False(File.Exists(path));
        AppUpdater.Install(target, Staging(("QueueLoom.exe", "next program")));
        Assert.Equal("next program", File.ReadAllText(target.Executable));
    }

    [Fact]
    public async Task Cleanup_WhenTempDownloadWasAlreadyRemoved_CanUpdateAgain()
    {
        var (target, updater, receipt) = await DownloadAndInstall();
        Directory.Delete(receipt.DownloadDirectory, true);
        MarkInstallerExited(target);
        updater.CleanUpPreviousUpdate(target);
        Assert.False(File.Exists(UpdateRestart.ReceiptPath(target)));
        AppUpdater.Install(target, Staging(("QueueLoom.exe", "next program")));
        Assert.Equal("next program", File.ReadAllText(target.Executable));
    }

    [Fact]
    public async Task Cleanup_WhenExistingDownloadHasLostOwnership_PreservesIt()
    {
        var (target, _, receipt) = await DownloadAndInstall();
        File.Delete(Path.Combine(receipt.DownloadDirectory, UpdateRestart.DownloadMarker));
        var unrelated = Path.Combine(receipt.DownloadDirectory, "user-file");
        File.WriteAllText(unrelated, "keep");
        await Assert.ThrowsAsync<InvalidDataException>(() =>
            UpdateRestart.CleanAsync(receipt, UpdateRestart.ReceiptPath(target), TimeSpan.Zero));
        Assert.Equal("keep", File.ReadAllText(unrelated));
        Assert.True(File.Exists(UpdateRestart.ReceiptPath(target)));
    }

    [Fact]
    public void Install_WhileAnotherTransactionOwnerIsCleaning_LeavesInstallationUntouched()
    {
        var target = Target("win-x64");
        File.WriteAllText(target.Executable, "old program");
        using var cleaning = new FileStream(UpdateRestart.ReceiptPath(target) + ".handoff", FileMode.OpenOrCreate,
            FileAccess.ReadWrite, FileShare.None, 1, FileOptions.DeleteOnClose);
        Assert.Throws<IOException>(() => AppUpdater.Install(target, Staging(("QueueLoom.exe", "new program"))));
        Assert.Equal("old program", File.ReadAllText(target.Executable));
        Assert.False(File.Exists(UpdateRestart.ReceiptPath(target)));
    }

    [Fact]
    public async Task StartupCleanup_WhileInstallerIsAlive_PreservesThePendingHandoff()
    {
        var (target, updater, receipt) = await DownloadAndInstall();
        updater.CleanUpPreviousUpdate(target);
        Assert.True(File.Exists(UpdateRestart.ReceiptPath(target)));
        Assert.True(File.Exists(receipt.Entries.Single().Backup));
        Assert.True(Directory.Exists(receipt.DownloadDirectory));
    }
    private async Task<(UpdateTarget Target, AppUpdater Updater, UpdateRestart.Receipt Receipt)> DownloadAndInstall()
    {
        var target = Target("win-x64");
        File.WriteAllText(target.Executable, "old program");
        var package = Zip(("QueueLoom.exe", "new program"));
        var updater = new AppUpdater(Serve(package, Sha(package)), Path.Combine(_root, "download"));
        var staging = await updater.DownloadAsync(Update, target, null, CancellationToken.None);
        AppUpdater.Install(target, staging);
        var receipt = System.Text.Json.JsonSerializer.Deserialize<UpdateRestart.Receipt>(
            File.ReadAllText(UpdateRestart.ReceiptPath(target)))!;
        return (target, updater, receipt);
    }
    private static void MarkInstallerExited(UpdateTarget target)
    {
        // These file-only tests simulate the successful next startup after the installer has exited.
        var path = UpdateRestart.ReceiptPath(target);
        var receipt = System.Text.Json.JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
        File.WriteAllText(path, System.Text.Json.JsonSerializer.Serialize(receipt with { InstallerPid = int.MaxValue }));
    }
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
        var applications = Path.Combine(_root, "Applications");
        var target = AppUpdater.TargetFor("osx-arm64", Path.Combine(applications, "QueueLoom.app", "Contents", "MacOS", "QueueLoom"));

        Assert.Equal(Path.Combine(applications, "QueueLoom.app"), target.Bundle);
        Assert.Equal(applications, target.InstallDirectory);
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

        var error = await Assert.ThrowsAsync<UpdateStageException>(() =>
            updater.DownloadAsync(Update, Target("win-x64"), null, CancellationToken.None));
        Assert.Equal(UpdatePhase.Verification, error.Phase);
        Assert.True(error.SafeToRetry);

        Assert.Contains("checksum", error.Message, StringComparison.Ordinal);
        Assert.Empty(Directory.GetDirectories(Path.Combine(_root, "download")));
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
        Assert.Equal("old program", File.ReadAllText(Directory.GetFiles(target.InstallDirectory, "QueueLoom.exe.*.old").Single()));
        Assert.Equal("new readme", File.ReadAllText(Path.Combine(target.InstallDirectory, "README.md")));

        MarkInstallerExited(target);
        new AppUpdater(new HttpClient(), Path.Combine(_root, "download")).CleanUpPreviousUpdate(target);
        Assert.Empty(Directory.GetFiles(target.InstallDirectory, "QueueLoom.exe.*.old"));
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
        Assert.Single(Directory.GetDirectories(target.InstallDirectory, "QueueLoom.app.*.old"));
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
