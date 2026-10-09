using System.Diagnostics;
using System.Formats.Tar;
using System.IO.Compression;
using System.Net;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.Core.Updates;
using QueueLoom.Tests.Infrastructure;
using InstallationFixture = QueueLoom.Tests.StableLauncherTests.InstallationFixture;

namespace QueueLoom.Tests;

public sealed class StableUpdateCleanupRegressionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LockedNestedStagingFileCannotPreventLaunchOrRollBackAHealthyCandidate(bool pending)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows file sharing semantics."); return; }
        using var fixture = new InstallationFixture();
        var installation = new VersionInstallation(fixture.Launcher);
        var package = fixture.Package("healthy");
        installation.StagePackage(package.Path);
        if (!pending)
        {
            var selected = installation.SelectForLaunch();
            installation.Acknowledge(selected.Version, selected.Attempt!);
            installation.Confirm(selected);
        }
        var id = Guid.NewGuid().ToString("N");
        var orphan = Path.Combine(installation.Store, "staging", id);
        Directory.CreateDirectory(Path.Combine(orphan, "nested"));
        var marker = Path.Combine(orphan, ".staging-owner");
        File.WriteAllText(marker, id);
        var nested = Path.Combine(orphan, "nested", "partial.bin");
        File.WriteAllText(nested, "interrupted extraction");
        (int Exit, string Output, string Error) first;
        using (var scanner = new FileStream(nested, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            // The marker is available. Only a nested payload file denies deletion throughout the real launch.
            first = await fixture.RunLauncher("--payload-fixture", "--mcp");
            Assert.True(File.Exists(nested));
            if (first.Exit == 0) Assert.Equal(id, File.ReadAllText(marker));
        }
        var second = await fixture.RunLauncher("--payload-fixture", "--mcp");
        Assert.Equal(0, first.Exit);
        Assert.Equal(package.Descriptor.Id, JsonDocument.Parse(first.Output).RootElement.GetProperty("version").GetString());
        Assert.Equal(0, second.Exit);
        Assert.Equal(package.Descriptor.Id, JsonDocument.Parse(second.Output).RootElement.GetProperty("version").GetString());
        Assert.False(Directory.Exists(orphan));
        Assert.False(installation.HasPendingActivation());
    }

    [Fact]
    public async Task ReadOnlyStableRootStillOffersInstallationIntoItsWritableUserStore()
    {
        using var fixture = new InstallationFixture();
        using var data = new TemporaryDirectory();
        var package = fixture.Package("writable-store");
        var root = new DirectoryInfo(fixture.Root);
        var legacyDirectory = Path.Combine(fixture.Root, "legacy");
        Directory.CreateDirectory(legacyDirectory);
        File.WriteAllText(Path.Combine(legacyDirectory, OperatingSystem.IsWindows() ? "QueueLoom.exe" : "QueueLoom"), "legacy fixture");
        DirectorySecurity? originalAcl = null;
        UnixFileMode originalMode = default;
        UnixFileMode originalLegacyMode = default;
        try
        {
            if (OperatingSystem.IsWindows())
            {
                originalAcl = root.GetAccessControl();
                var readOnly = root.GetAccessControl();
                readOnly.AddAccessRule(new FileSystemAccessRule(WindowsIdentity.GetCurrent().User!,
                    FileSystemRights.CreateFiles | FileSystemRights.CreateDirectories,
                    InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Deny));
                root.SetAccessControl(readOnly);
            }
            else
            {
                originalMode = File.GetUnixFileMode(fixture.Root);
                originalLegacyMode = File.GetUnixFileMode(legacyDirectory);
                File.SetUnixFileMode(fixture.Root, UnixFileMode.UserRead | UnixFileMode.UserExecute);
                File.SetUnixFileMode(legacyDirectory, UnixFileMode.UserRead | UnixFileMode.UserExecute);
            }
            // The probe and real staging run in a child with isolated per-user data, never real application data.
            var result = await RunUpdaterFixture(fixture, data.Path, "--stable-update-probe", package.Path);
            Assert.Equal(0, result.Exit);
            using var output = JsonDocument.Parse(result.Output);
            Assert.False(output.RootElement.GetProperty("LegacyCanInstall").GetBoolean());
            Assert.True(output.RootElement.GetProperty("CanInstall").GetBoolean());
            var store = output.RootElement.GetProperty("Store").GetString()!;
            Assert.StartsWith(data.Path + Path.DirectorySeparatorChar, store, StringComparison.Ordinal);
            Assert.True(File.Exists(Path.Combine(store, "versions", package.Descriptor.Id, "manifest.json")));
        }
        finally
        {
            if (OperatingSystem.IsWindows() && originalAcl is not null) root.SetAccessControl(originalAcl);
            else if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(fixture.Root, originalMode);
                File.SetUnixFileMode(legacyDirectory, originalLegacyMode);
            }
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StableInstallationRemovesOnlyItsOwnedDownloadOnSuccessAndFailure(bool invalidPackage)
    {
        using var fixture = new InstallationFixture();
        using var downloads = new TemporaryDirectory();
        var package = fixture.Package("downloaded");
        if (invalidPackage) File.WriteAllText(InstallationFixture.DescriptorFile(package.Path), "{}");
        var bytes = Archive(package.Path);
        var handler = new PackageHandler(bytes);
        using var http = new HttpClient(handler);
        var updater = new AppUpdater(http, downloads.Path);
        var target = AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher);
        var update = new UpdateCheckResult(new Version(9, 1, 0), "v9.1.0", new Uri("https://github.com/bitcodepro/QueueLoom/releases/tag/v9.1.0"));
        var staging = await updater.DownloadAsync(update, target, null, default);
        var owned = Path.GetDirectoryName(staging)!;
        var otherStaging = await updater.DownloadAsync(update, target, null, default);
        var other = Path.GetDirectoryName(otherStaging)!;
        var otherArchive = Directory.GetFiles(other, "QueueLoom-*.*").Single();
        var otherHash = File.ReadAllBytes(otherArchive);
        var foreign = Path.Combine(downloads.Path, "foreign");
        Directory.CreateDirectory(foreign);
        File.WriteAllText(Path.Combine(foreign, UpdateRestart.DownloadMarker), "foreign ownership");
        File.WriteAllText(Path.Combine(foreign, "keep"), "user data");
        handler.HoldNextChecksum = true;
        var pendingDownload = updater.DownloadAsync(update, target, null, default);
        await handler.ChecksumStarted.Task.WaitAsync(TimeSpan.FromSeconds(30));
        var active = Assert.Single(Directory.GetDirectories(downloads.Path), path => path != owned && path != other && path != foreign);
        var installation = new VersionInstallation(fixture.Launcher);
        try
        {
            if (invalidPackage) Assert.Throws<InvalidDataException>(() => AppUpdater.Install(target, staging));
            else
            {
                AppUpdater.Install(target, staging);
                var selected = installation.SelectForLaunch();
                Assert.Equal(package.Descriptor.Id, selected.Version.Id);
                installation.Acknowledge(selected.Version, selected.Attempt!);
                installation.Confirm(selected);
                Assert.True(File.Exists(installation.Verify(installation.Bootstrap)));
                Assert.True(File.Exists(installation.Verify(selected.Version)));
            }
            Assert.False(Directory.Exists(owned)); // Both success and failure clean without waiting for startup.
            updater.CleanUpPreviousUpdate(target);
            Assert.True(File.Exists(Path.Combine(active, UpdateRestart.DownloadMarker)));
            Assert.False(pendingDownload.IsCompleted);
            Assert.True(Directory.Exists(otherStaging));
            Assert.Equal(otherHash, File.ReadAllBytes(otherArchive));
            Assert.Equal("user data", File.ReadAllText(Path.Combine(foreign, "keep")));
            if (invalidPackage) Assert.Equal(installation.Bootstrap, installation.SelectForLaunch().Version);
        }
        finally { handler.ReleaseChecksum.TrySetResult(true); await pendingDownload; }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LockedOwnedDownloadKeepsItsMarkerAndReceiptForStartupRetry(bool invalidPackage)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows file sharing semantics."); return; }
        using var fixture = new InstallationFixture();
        using var downloads = new TemporaryDirectory();
        var package = fixture.Package("locked-download");
        if (invalidPackage) File.WriteAllText(InstallationFixture.DescriptorFile(package.Path), "{}");
        using var http = new HttpClient(new PackageHandler(Archive(package.Path)));
        var updater = new AppUpdater(http, downloads.Path);
        var target = AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher);
        var update = new UpdateCheckResult(new Version(9, 1, 0), "v9.1.0", new Uri("https://github.com/bitcodepro/QueueLoom/releases/tag/v9.1.0"));
        var staging = await updater.DownloadAsync(update, target, null, default);
        var directory = Path.GetDirectoryName(staging)!;
        var marker = Path.Combine(directory, UpdateRestart.DownloadMarker);
        var id = File.ReadAllText(marker);
        var archive = Directory.GetFiles(directory, "QueueLoom-*.*").Single();
        var installation = new VersionInstallation(fixture.Launcher);
        var receipt = Path.Combine(installation.Store, "download-cleanup", id + ".json");
        using (var scanner = new FileStream(archive, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            if (invalidPackage) Assert.Throws<InvalidDataException>(() => AppUpdater.Install(target, staging));
            else AppUpdater.Install(target, staging);
            updater.CleanUpPreviousUpdate(target);
            Assert.Equal(id, File.ReadAllText(marker));
            Assert.True(File.Exists(receipt));
            Assert.True(File.Exists(archive));
        }
        updater.CleanUpPreviousUpdate(target);
        updater.CleanUpPreviousUpdate(target);
        Assert.False(Directory.Exists(directory));
        Assert.False(File.Exists(receipt));
        Assert.True(File.Exists(installation.Verify(installation.Bootstrap)));
        Assert.Equal(invalidPackage ? installation.Bootstrap.Id : package.Descriptor.Id, installation.SelectForLaunch().Version.Id);
    }

    [Fact]
    public void StableInstallationKeepsAnUnownedPackageSource()
    {
        using var fixture = new InstallationFixture();
        var package = fixture.Package("unowned-source");
        var archive = Path.Combine(package.Path, VersionInstallation.ArchiveName);
        if (OperatingSystem.IsMacOS()) archive = Path.Combine(package.Path, "QueueLoom.app", "Contents", "Resources", "initial", "manifest.json");
        var original = File.ReadAllBytes(archive);
        File.WriteAllText(Path.Combine(package.Path, "keep"), "unowned source");
        AppUpdater.Install(AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher), package.Path);
        Assert.Equal(original, File.ReadAllBytes(archive));
        Assert.Equal("unowned source", File.ReadAllText(Path.Combine(package.Path, "keep")));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StartupRetainsAnInstallationDownloadUntilItsOwnerCompletes(bool heldTransaction)
    {
        using var fixture = new InstallationFixture();
        using var downloads = new TemporaryDirectory();
        var package = fixture.Package("active-installation");
        using var http = new HttpClient(new PackageHandler(Archive(package.Path)));
        var updater = new AppUpdater(http, downloads.Path);
        var target = AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher);
        var update = new UpdateCheckResult(new Version(9, 1, 0), "v9.1.0", new Uri("https://github.com/bitcodepro/QueueLoom/releases/tag/v9.1.0"));
        var staging = await updater.DownloadAsync(update, target, null, default);
        var directory = Path.GetDirectoryName(staging)!;
        var id = File.ReadAllText(Path.Combine(directory, UpdateRestart.DownloadMarker));
        var installation = new VersionInstallation(fixture.Launcher);
        installation.EnsureBootstrap();
        var records = Path.Combine(installation.Store, "download-cleanup");
        Directory.CreateDirectory(records);
        var path = Path.Combine(records, id + ".json");
        using var process = Process.GetCurrentProcess();
        var receipt = new UpdateRestart.Receipt(id, target, directory, [],
            InstallerPid: heldTransaction ? 0 : process.Id, InstallerStartTicks: process.StartTime.ToUniversalTime().Ticks);
        File.WriteAllText(path, JsonSerializer.Serialize(receipt));
        using (var lease = heldTransaction ? UpdateRestart.OwnTransaction(path) : null)
        {
            updater.CleanUpPreviousUpdate(target);
            Assert.True(Directory.Exists(staging));
            Assert.True(File.Exists(path));
            Assert.Equal(id, File.ReadAllText(Path.Combine(directory, UpdateRestart.DownloadMarker)));
        }
        File.WriteAllText(path, JsonSerializer.Serialize(receipt with { InstallerPid = 0, InstallerStartTicks = 0 }));
        updater.CleanUpPreviousUpdate(target);
        Assert.False(Directory.Exists(directory));
        Assert.False(File.Exists(path));
    }

    [Fact]
    public void OwnedStagingWithLinkedContentIsRefusedBeforeDeletion()
    {
        if (OperatingSystem.IsWindows()) { Assert.Skip("Unix link fixture; no Windows privilege changes."); return; }
        using var fixture = new InstallationFixture();
        using var external = new TemporaryDirectory();
        var installation = new VersionInstallation(fixture.Launcher);
        installation.EnsureBootstrap();
        var id = Guid.NewGuid().ToString("N");
        var orphan = Path.Combine(installation.Store, "staging", id);
        Directory.CreateDirectory(orphan);
        File.WriteAllText(Path.Combine(orphan, ".staging-owner"), id);
        File.WriteAllText(Path.Combine(external.Path, "keep"), "external data");
        Directory.CreateSymbolicLink(Path.Combine(orphan, "linked"), external.Path);
        Assert.Throws<InvalidDataException>(() => installation.CleanupStaging());
        Assert.Equal("external data", File.ReadAllText(Path.Combine(external.Path, "keep")));
        Assert.Equal(id, File.ReadAllText(Path.Combine(orphan, ".staging-owner")));
    }

    private static byte[] Archive(string directory)
    {
        using var output = new MemoryStream();
        if (OperatingSystem.IsLinux())
        {
            using var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true);
            TarFile.CreateFromDirectory(directory, gzip, includeBaseDirectory: false);
        }
        else
        {
            using var zip = new ZipArchive(output, ZipArchiveMode.Create, leaveOpen: true);
            foreach (var file in Directory.GetFiles(directory, "*", SearchOption.AllDirectories))
            {
                var entry = zip.CreateEntryFromFile(file, Path.GetRelativePath(directory, file).Replace('\\', '/'));
                if (!OperatingSystem.IsWindows()) entry.ExternalAttributes = ((int)File.GetUnixFileMode(file) | 0x8000) << 16;
            }
        }
        return output.ToArray();
    }

    private sealed class PackageHandler(byte[] bytes) : HttpMessageHandler
    {
        public bool HoldNextChecksum { get; set; }
        public TaskCompletionSource<bool> ChecksumStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> ReleaseChecksum { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var checksum = request.RequestUri!.AbsolutePath.EndsWith(".sha256", StringComparison.Ordinal);
            if (checksum && HoldNextChecksum)
            {
                HoldNextChecksum = false;
                ChecksumStarted.TrySetResult(true);
                await ReleaseChecksum.Task.WaitAsync(cancellationToken);
            }
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = checksum
                    ? new StringContent(Convert.ToHexStringLower(SHA256.HashData(bytes))) : new ByteArrayContent(bytes)
            };
        }
    }

    private static async Task<(int Exit, string Output, string Error)> RunUpdaterFixture(
        InstallationFixture fixture, string data, params string[] arguments)
    {
        var start = new ProcessStartInfo(Path.Combine(AppContext.BaseDirectory, "UpdateFixture",
            OperatingSystem.IsWindows() ? "QueueLoom.UpdateFixture.exe" : "QueueLoom.UpdateFixture"))
        {
            UseShellExecute = false, CreateNoWindow = true, WorkingDirectory = fixture.Root,
            RedirectStandardOutput = true, RedirectStandardError = true
        };
        start.Environment["QUEUELOOM_DATA_DIRECTORY"] = data;
        start.ArgumentList.Add(arguments[0]);
        start.ArgumentList.Add(fixture.Launcher);
        foreach (var argument in arguments.Skip(1)) start.ArgumentList.Add(argument);
        using var process = Process.Start(start)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        try { await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(30)); }
        finally { if (!process.HasExited) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(); } }
        return (process.ExitCode, await output, await error);
    }
}
