using System.Diagnostics;
using System.Formats.Tar;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;

namespace QueueLoom.App.Services;

/// <summary>Where the running QueueLoom lives and how an update replaces it.</summary>
/// <param name="Rid">Package name suffix: win-x64, linux-x64, osx-arm64 or osx-x64.</param>
/// <param name="InstallDirectory">The folder with QueueLoom(.exe), or the folder that holds QueueLoom.app on macOS.</param>
/// <param name="Bundle">QueueLoom.app on macOS; null elsewhere.</param>
public sealed record UpdateTarget(string Rid, string InstallDirectory, string Executable, string? Bundle);

public sealed record UpdateProgress(long Downloaded, long? Total)
{
    public double? Percent => Total is > 0 ? 100.0 * Downloaded / Total.Value : null;
    public UpdatePhase Phase { get; init; } = UpdatePhase.Downloading;
}

public enum UpdatePhase { ChecksumFetch, Downloading, Verification, Extraction, Installation, Restart, Recovery }

public sealed class UpdateStageException(UpdatePhase phase, string message, bool safeToRetry, Exception inner)
    : IOException(message, inner)
{
    public UpdatePhase Phase { get; } = phase;
    public bool SafeToRetry { get; } = safeToRetry;
}

/// <summary>
/// Downloads the release package for this system from GitHub, checks it against the published SHA-256 checksum and
/// puts the new files in place of the running ones. The running files are renamed (".old"), which every OS allows,
/// and removed at the next start; if anything fails, they are renamed back.
/// </summary>
public sealed class AppUpdater(HttpClient httpClient, string? downloadRoot = null, DiagnosticsJournal? diagnostics = null)
{
    static AppUpdater()
    {
        UpdateRestart.RecoveryRecorded += fact =>
        {
            var operation = DiagnosticsJournal.Session.Begin("Update");
            DiagnosticsJournal.Session.Record(operation, DiagnosticStage.Completed, DiagnosticOutcome.Confirmed,
                updateStage: fact == UpdateRestart.RecordedRecovery.Restored ? UpdatePhase.Recovery : UpdatePhase.Restart,
                restart: fact == UpdateRestart.RecordedRecovery.StartupAcknowledged ? DiagnosticRecovery.StartupAcknowledged : DiagnosticRecovery.Unknown,
                rollback: fact == UpdateRestart.RecordedRecovery.Restored ? DiagnosticRecovery.Restored : DiagnosticRecovery.Unknown);
        };
    }
    private readonly DiagnosticsJournal _diagnostics = diagnostics ?? DiagnosticsJournal.Session;
    public const string ReleasesDownload = "https://github.com/bitcodepro/QueueLoom/releases/download";
    private const string DownloadMarker = UpdateRestart.DownloadMarker;

    private readonly string _downloadRoot = downloadRoot ?? DefaultDownloadRoot();

    private const UnixFileMode PrivateDirectoryMode = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;

    /// <summary>
    /// Linux shares /tmp between accounts: anyone could pre-create /tmp/QueueLoom-update and rename or replace a
    /// verified package before Install moves it into the program folder. Windows %TEMP% and macOS $TMPDIR are per user.
    /// </summary>
    private static string DefaultDownloadRoot()
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsMacOS())
        {
            var local = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.Create);
            if (!string.IsNullOrWhiteSpace(local))
            {
                return Path.Combine(local, "QueueLoom", "update-downloads");
            }
        }
        return Path.Combine(Path.GetTempPath(), "QueueLoom-update");
    }

    /// <summary>
    /// Creates the download folder for this user only. Whoever owns the root can rename or replace anything in it
    /// between verification and installation, so a root that belongs to another account is refused: only its owner
    /// can change its mode, which makes the mode change both the ownership check and the repair.
    /// </summary>
    private void CreatePrivateDownloadFolder(string folder)
    {
        if (OperatingSystem.IsWindows())
        {
            Directory.CreateDirectory(folder);
            return;
        }
        if (!Directory.Exists(_downloadRoot))
        {
            Directory.CreateDirectory(_downloadRoot, PrivateDirectoryMode);
        }
        try
        {
            File.SetUnixFileMode(_downloadRoot, PrivateDirectoryMode);
        }
        catch (UnauthorizedAccessException exception)
        {
            throw new InvalidOperationException(
                $"The update download folder belongs to another account and can be changed by other users ({_downloadRoot}), so the update was not downloaded.",
                exception);
        }
        if ((File.GetUnixFileMode(_downloadRoot) & (UnixFileMode.GroupWrite | UnixFileMode.OtherWrite)) != 0)
        {
            throw new InvalidOperationException(
                $"The update download folder can be changed by other users ({_downloadRoot}), so the update was not downloaded.");
        }
        Directory.CreateDirectory(folder, PrivateDirectoryMode);
    }

    /// <summary>The running installation, or null when QueueLoom runs from a build folder or an unknown system.</summary>
    public static UpdateTarget? CurrentTarget()
    {
        if (QueueLoom.Core.Updates.PayloadLaunch.Current is { } launched)
        {
            var installation = launched.Installation;
            return new(installation.Rid, installation.Root, installation.Launcher, installation.Bundle);
        }
        var rid = CurrentRid();
        var executable = Environment.ProcessPath;
        if (rid is null || executable is null || !string.Equals(Path.GetFileNameWithoutExtension(executable), "QueueLoom", StringComparison.Ordinal) ||
            File.Exists(Path.Combine(Path.GetDirectoryName(executable)!, "QueueLoom.dll")))
        {
            // Release packages are single files; a QueueLoom.dll beside the program means a build folder.
            return null;
        }

        return TargetFor(rid, executable);
    }

    public static UpdateTarget TargetFor(string rid, string executable)
    {
        var directory = Path.GetDirectoryName(Path.GetFullPath(executable))!;
        if (rid.StartsWith("osx", StringComparison.Ordinal) &&
            Path.GetFileName(directory) == "MacOS" &&
            Path.GetFileName(Path.GetDirectoryName(directory)) == "Contents")
        {
            var bundle = Path.GetDirectoryName(Path.GetDirectoryName(directory))!;
            return new UpdateTarget(rid, Path.GetDirectoryName(bundle)!, executable, bundle);
        }
        return new UpdateTarget(rid, directory, executable, null);
    }

    public static string? CurrentRid() => RuntimeInformation.OSArchitecture switch
    {
        Architecture.X64 when OperatingSystem.IsWindows() => "win-x64",
        Architecture.X64 when OperatingSystem.IsLinux() => "linux-x64",
        Architecture.Arm64 when OperatingSystem.IsMacOS() => "osx-arm64",
        Architecture.X64 when OperatingSystem.IsMacOS() => "osx-x64",
        _ => null
    };

    public static string PackageName(string version, string rid) =>
        $"QueueLoom-{version}-{rid}{(rid.StartsWith("linux", StringComparison.Ordinal) ? ".tar.gz" : ".zip")}";

    public static Uri PackageUri(string tag, string version, string rid) =>
        new($"{ReleasesDownload}/{Uri.EscapeDataString(tag)}/{PackageName(version, rid)}");

    /// <summary>Whether the update destination is writable: the version store for stable installations.</summary>
    public static bool CanInstall(UpdateTarget target)
    {
        try
        {
            if (QueueLoom.Core.Updates.VersionInstallation.HasDescriptor(target.Executable))
                return new QueueLoom.Core.Updates.VersionInstallation(target.Executable).CanStagePackage();
            var probe = Path.Combine(target.InstallDirectory, $".queueloom-update-{Guid.NewGuid():N}");
            using (File.Create(probe, 1, FileOptions.DeleteOnClose))
            {
            }
            return true;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or InvalidDataException)
        {
            return false;
        }
    }

    /// <summary>Downloads, verifies and unpacks the package; returns the folder with the new files.</summary>
    public async Task<string> DownloadAsync(
        UpdateCheckResult update,
        UpdateTarget target,
        IProgress<UpdateProgress>? progress,
        CancellationToken cancellationToken)
    {
        var version = update.Version.ToString(3);
        var package = PackageUri(update.Tag, version, target.Rid);
        var id = Guid.NewGuid().ToString("N");
        var folder = Path.Combine(_downloadRoot, version + "-" + id);
        var phase = UpdatePhase.ChecksumFetch;
        var diagnosticOperation = _diagnostics.Begin("Update");
        try
        {
            CreatePrivateDownloadFolder(folder);
            File.WriteAllText(Path.Combine(folder, DownloadMarker), id);

            var archive = Path.Combine(folder, PackageName(version, target.Rid));
            progress?.Report(new UpdateProgress(0, null) { Phase = phase });
            _diagnostics.Record(diagnosticOperation, DiagnosticStage.Executing, updateStage: phase);
            var expected = await DownloadChecksumAsync(new Uri(package + ".sha256"), cancellationToken).ConfigureAwait(false);
            phase = UpdatePhase.Downloading;
            _diagnostics.Record(diagnosticOperation, DiagnosticStage.Executing, updateStage: phase);
            progress?.Report(new UpdateProgress(0, null) { Phase = phase });
            using (var response = await httpClient.GetAsync(package, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false))
            {
                if (!response.IsSuccessStatusCode)
                {
                    throw new InvalidOperationException(
                        $"The package for this system could not be downloaded ({(int)response.StatusCode}). Download it from the releases page instead.");
                }

                var total = response.Content.Headers.ContentLength;
                await using var source = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                await using var file = File.Create(archive);
                var buffer = new byte[81_920];
                long downloaded = 0;
                int read;
                while ((read = await source.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) > 0)
                {
                    await file.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                    downloaded += read;
                    progress?.Report(new UpdateProgress(downloaded, total));
                }
            }

            phase = UpdatePhase.Verification;
            _diagnostics.Record(diagnosticOperation, DiagnosticStage.Executing, updateStage: phase);
            progress?.Report(new UpdateProgress(0, null) { Phase = phase });
            await using (var stream = File.OpenRead(archive))
            {
                var actual = Convert.ToHexStringLower(await SHA256.HashDataAsync(stream, cancellationToken).ConfigureAwait(false));
                if (!string.Equals(actual, expected, StringComparison.OrdinalIgnoreCase))
                {
                    _diagnostics.Record(diagnosticOperation, DiagnosticStage.Failed, updateStage: phase, checksum: DiagnosticCheck.Mismatch);
                    throw new InvalidOperationException("The downloaded package does not match its published checksum, so it was not installed.");
                }
            }

            phase = UpdatePhase.Extraction;
            _diagnostics.Record(diagnosticOperation, DiagnosticStage.Executing, updateStage: phase, checksum: DiagnosticCheck.Verified);
            progress?.Report(new UpdateProgress(0, null) { Phase = phase });
            var staging = Path.Combine(folder, "files");
            Directory.CreateDirectory(staging);
            if (archive.EndsWith(".tar.gz", StringComparison.Ordinal))
            {
                await using var stream = File.OpenRead(archive);
                await using var gzip = new GZipStream(stream, CompressionMode.Decompress);
                await TarFile.ExtractToDirectoryAsync(gzip, staging, overwriteFiles: true, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await ZipFile.ExtractToDirectoryAsync(archive, staging, overwriteFiles: true, cancellationToken).ConfigureAwait(false);
            }
            return staging;
        }
        catch (Exception exception)
        {
            _diagnostics.Record(diagnosticOperation, DiagnosticStage.Failed, error: exception, updateStage: phase);
            TryDelete(folder);
            if (exception is OperationCanceledException && cancellationToken.IsCancellationRequested) throw;
            throw new UpdateStageException(phase, exception.Message, true, exception);
        }
    }

    /// <summary>Retry after replacement is allowed only with no outstanding transaction receipt.</summary>
    public static async Task InstallWithProgressAsync(UpdateTarget target, string staging, IProgress<UpdateProgress> progress, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        var diagnosticOperation = DiagnosticsJournal.Session.Begin("Update");
        DiagnosticsJournal.Session.Record(diagnosticOperation, DiagnosticStage.Executing, updateStage: UpdatePhase.Installation);
        progress.Report(new UpdateProgress(0, null) { Phase = UpdatePhase.Installation });
        try
        {
            await Task.Run(() => Install(target, staging), CancellationToken.None);
            DiagnosticsJournal.Session.Record(diagnosticOperation, DiagnosticStage.Completed, DiagnosticOutcome.Confirmed, updateStage: UpdatePhase.Installation);
        }
        catch (Exception exception)
        {
            var safe = !File.Exists(UpdateRestart.ReceiptPath(target));
            if (QueueLoom.Core.Updates.VersionInstallation.HasDescriptor(target.Executable))
            {
                try { safe &= !new QueueLoom.Core.Updates.VersionInstallation(target.Executable).HasPendingActivation(); }
                catch (Exception) { safe = false; } // Damaged activation evidence requires recovery before retry.
            }
            DiagnosticsJournal.Session.Record(diagnosticOperation, DiagnosticStage.Failed, error: exception, updateStage: safe ? UpdatePhase.Installation : UpdatePhase.Recovery);
            throw new UpdateStageException(safe ? UpdatePhase.Installation : UpdatePhase.Recovery,
                exception.Message, safe, exception);
        }
    }

    /// <summary>Puts the unpacked files in place; the running ones become ".old". Rolls back on any failure.</summary>
    public static void Install(UpdateTarget target, string staging)
    {
        if (QueueLoom.Core.Updates.VersionInstallation.HasDescriptor(target.Executable))
        {
            var installation = new QueueLoom.Core.Updates.VersionInstallation(target.Executable);
            var download = StableDownloadReceipt(installation, target, staging);
            if (download is null) { installation.StagePackage(staging); return; }
            var (stableReceipt, path) = download.Value;
            using var stableOwnership = UpdateRestart.OwnTransaction(path);
            if (File.Exists(path)) throw new IOException("This download already has an installation cleanup transaction.");
            QueueLoom.Core.IO.SafeFileWriter.WriteText(path, JsonSerializer.Serialize(stableReceipt), narrowGroup: false);
            try { installation.StagePackage(staging); }
            finally
            {
                // StagePackage copies into the immutable store; its source is disposable even after failure.
                // Keep the receipt if the download is busy so a later startup retries only this transaction.
                try
                {
                    stableReceipt = stableReceipt with { InstallerPid = 0, InstallerStartTicks = 0 };
                    QueueLoom.Core.IO.SafeFileWriter.WriteText(path, JsonSerializer.Serialize(stableReceipt), narrowGroup: false);
                    CleanStableDownload(installation, target, stableReceipt, path);
                }
                catch (Exception error) when (error is IOException or UnauthorizedAccessException) { }
            }
            return;
        }
        var downloadDirectory = Path.GetDirectoryName(Path.GetFullPath(staging))!;
        var marker = Path.Combine(downloadDirectory, DownloadMarker);
        var id = File.Exists(marker) ? File.ReadAllText(marker) : Guid.NewGuid().ToString("N");
        if (!Guid.TryParseExact(id, "N", out _)) throw new InvalidDataException("Invalid update download marker.");
        if (!File.Exists(marker) || !Path.GetFileName(downloadDirectory).EndsWith("-" + id, StringComparison.Ordinal))
            downloadDirectory = string.Empty;
        var receiptPath = UpdateRestart.ReceiptPath(target);
        using var ownership = UpdateRestart.OwnTransaction(receiptPath);
        var entries = target.Bundle is { } appBundle
            ? new[] { new UpdateRestart.Entry(appBundle, Directory.Exists(appBundle) ? appBundle + "." + id + ".old" : null) }
            : Directory.EnumerateFiles(staging).Select(file =>
            {
                var current = Path.Combine(target.InstallDirectory, Path.GetFileName(file));
                return new UpdateRestart.Entry(current, File.Exists(current) ? current + "." + id + ".old" : null);
            }).ToArray();
        using var installer = Process.GetCurrentProcess();
        var receipt = new UpdateRestart.Receipt(id, target, downloadDirectory, entries,
            InstallerPid: installer.Id, InstallerStartTicks: installer.StartTime.ToUniversalTime().Ticks)
        { BundleBackupDirectories = MacBackupMigration.CaptureDirectories(target) };
        var moves = new List<(string Current, string Old)>();
        var added = new List<string>();
        // A previous unfinished update must be recovered, not overwritten.
        // Written whole and then renamed, so a crash cannot leave a partial receipt; the rename fails if one exists.
        var receiptTemporary = receiptPath + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            using (var stream = new FileStream(receiptTemporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(stream, receipt);
                stream.Flush(flushToDisk: true);
            }
            File.Move(receiptTemporary, receiptPath, overwrite: false);
        }
        finally
        {
            if (File.Exists(receiptTemporary)) File.Delete(receiptTemporary);
        }
        try
        {
            if (target.Bundle is { } bundle)
            {
                var newBundle = Path.Combine(staging, "QueueLoom.app");
                if (!Directory.Exists(newBundle) || !File.Exists(Path.Combine(newBundle, "Contents", "MacOS", "QueueLoom")))
                {
                    throw new InvalidOperationException("The package does not contain a complete QueueLoom.app with its executable.");
                }
                MacBackupMigration.Preserve(receipt, bundle);
                ReplaceDirectory(bundle, newBundle, moves, added, id);
                MakeExecutable(Path.Combine(bundle, "Contents", "MacOS", "QueueLoom"));
            }
            else
            {
                var executableName = Path.GetFileName(target.Executable);
                if (!File.Exists(Path.Combine(staging, executableName)))
                {
                    throw new InvalidOperationException($"The package does not contain {executableName}.");
                }
                foreach (var file in Directory.EnumerateFiles(staging))
                {
                    ReplaceFile(Path.Combine(target.InstallDirectory, Path.GetFileName(file)), file, moves, added, id);
                }
                MakeExecutable(target.Executable);
            }
        }
        catch
        {
            foreach (var path in added)
            {
                TryDelete(path);
            }
            foreach (var (current, old) in Enumerable.Reverse(moves))
            {
                if (Directory.Exists(old))
                {
                    UpdateRestart.MoveRetrying(() => Directory.Move(old, current));
                }
                else if (File.Exists(old))
                {
                    UpdateRestart.MoveRetrying(() => File.Move(old, current, overwrite: true));
                }
            }
            File.Delete(receiptPath);
            throw;
        }
    }

    /// <summary>Starts the installed version; the caller then closes this one.</summary>
    public static void StartInstalled(UpdateTarget target)
    {
        if (QueueLoom.Core.Updates.VersionInstallation.HasDescriptor(target.Executable))
        {
            StartVersionLauncher(target);
            return;
        }
        var operation = DiagnosticsJournal.Session.Begin("Update");
        try
        {
            UpdateRestart.Start(target);
            DiagnosticsJournal.Session.Record(operation, DiagnosticStage.Executing, updateStage: UpdatePhase.Restart,
                restart: DiagnosticRecovery.Requested);
        }
        catch (Exception exception) when (exception is IOException or System.ComponentModel.Win32Exception)
        {
            DiagnosticsJournal.Session.Record(operation, DiagnosticStage.Failed, error: exception, updateStage: UpdatePhase.Restart,
                restart: DiagnosticRecovery.Failed);
            var path = UpdateRestart.ReceiptPath(target);
            using var ownership = UpdateRestart.OwnTransaction(path);
            var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
            UpdateRestart.Read(path, receipt.Id);
            UpdateRestart.Restore(receipt);
            File.WriteAllText(path + ".tmp", JsonSerializer.Serialize(receipt with { Recovered = true }));
            File.Move(path + ".tmp", path, overwrite: true);
            throw new IOException("The restart helper could not start. The previous version was restored and is still running.", exception);
        }
    }

    private static void StartVersionLauncher(UpdateTarget target)
    {
        var installation = new QueueLoom.Core.Updates.VersionInstallation(target.Executable);
        installation.EnsureBootstrap();
        using var parent = Process.GetCurrentProcess();
        var token = Guid.NewGuid().ToString("N");
        var ready = Path.Combine(installation.Store, "restart-" + token + ".ready");
        var start = new ProcessStartInfo(target.Executable) { UseShellExecute = false, CreateNoWindow = true, WorkingDirectory = target.InstallDirectory };
        foreach (var argument in new[] { "--launcher-restart", parent.Id.ToString(System.Globalization.CultureInfo.InvariantCulture),
                     parent.StartTime.ToUniversalTime().Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture), token })
            start.ArgumentList.Add(argument);
        using var launcher = Process.Start(start) ?? throw new IOException("The stable restart launcher could not start.");
        var waiting = Stopwatch.StartNew();
        while (!File.Exists(ready))
        {
            if (launcher.HasExited) throw new IOException("The stable restart launcher exited before accepting the handoff.");
            if (waiting.Elapsed > TimeSpan.FromSeconds(15))
            {
                launcher.Kill(entireProcessTree: true);
                launcher.WaitForExit();
                throw new IOException("The stable restart launcher did not accept the handoff; the previous version remains available.");
            }
            Thread.Sleep(20);
        }
    }

    /// <summary>Removes what the previous update left behind (the ".old" files and the download).</summary>
    public void CleanUpPreviousUpdate(UpdateTarget? target)
    {
        if (target is not null && QueueLoom.Core.Updates.VersionInstallation.HasDescriptor(target.Executable))
        {
            var installation = new QueueLoom.Core.Updates.VersionInstallation(target.Executable);
            installation.CleanupStaging();
            var folder = Path.Combine(installation.Store, "download-cleanup");
            installation.RejectOwnedPath(folder);
            string[] records;
            try { records = Directory.Exists(folder) ? Directory.GetFiles(folder, "*.json") : []; }
            catch (Exception error) when (error is IOException or UnauthorizedAccessException) { records = []; }
            foreach (var path in records)
            {
                if (!Guid.TryParseExact(Path.GetFileNameWithoutExtension(path), "N", out _)) continue;
                try
                {
                    // Reject links before opening the lease or reading the receipt.
                    ValidateStableCleanupPaths(installation, path);
                    using var retainedOwnership = UpdateRestart.OwnTransaction(path);
                    if (!File.Exists(path)) continue;
                    var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path));
                    if (receipt is null) continue;
                    CleanStableDownload(installation, target, receipt, path);
                }
                catch (Exception error) when (error is IOException or UnauthorizedAccessException or
                                                   JsonException or InvalidDataException)
                {
                    // Retain invalid, stale or busy evidence. It cannot authorize deleting a download
                    // or prevent other transactions and update discovery from making progress.
                }
            }
        }
        if (target is not null && File.Exists(UpdateRestart.ReceiptPath(target)))
        {
            var path = UpdateRestart.ReceiptPath(target);
            // Retain ownership through receipt validation and cleanup, including asynchronous retries.
            FileStream ownership;
            try { ownership = UpdateRestart.OwnTransaction(path); }
            catch (IOException) { return; }
            using var retainedOwnership = ownership;
            if (!File.Exists(path)) return;
            UpdateRestart.Receipt? receipt;
            try { receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path)); }
            catch (JsonException) { receipt = null; }
            if (receipt is null)
            {
                // Cut short by a crash while it was written: it names nothing to restore or clean, and left in place
                // it would block every later update. This version started, so keep it aside as evidence.
                File.Move(path, path + $".damaged-{DateTime.UtcNow:yyyyMMddHHmmssfff}-{Guid.NewGuid():N}");
                return;
            }
            UpdateRestart.Read(path, receipt.Id);
            // Protect the installed-but-not-yet-accepted handoff between Install and StartInstalled.
            if (UpdateRestart.InstallerIsAlive(receipt)) return;
            // Startup was successful. Retry only the backups recorded by this update.
            UpdateRestart.CleanAsync(receipt, path, TimeSpan.Zero).GetAwaiter().GetResult();
        }
    }

    private static (UpdateRestart.Receipt Receipt, string Path)? StableDownloadReceipt(
        QueueLoom.Core.Updates.VersionInstallation installation, UpdateTarget target, string staging)
    {
        var source = Path.GetFullPath(staging);
        var directory = Path.GetDirectoryName(source)!;
        var marker = Path.Combine(directory, DownloadMarker);
        if (Path.GetFileName(source) != "files" || !File.Exists(marker)) return null;
        QueueLoom.Core.Updates.VersionInstallation.RejectLinks(marker, Path.GetDirectoryName(directory)!);
        var id = File.ReadAllText(marker);
        if (!Guid.TryParseExact(id, "N", out _)) throw new InvalidDataException("Invalid update download marker.");
        if (!Path.GetFileName(directory).EndsWith("-" + id, StringComparison.Ordinal)) return null;
        if (!installation.CanStagePackage()) throw new IOException("The version store is not writable.");
        var folder = Path.Combine(installation.Store, "download-cleanup");
        installation.RejectOwnedPath(folder);
        if (OperatingSystem.IsWindows()) Directory.CreateDirectory(folder);
        else Directory.CreateDirectory(folder, PrivateDirectoryMode);
        var path = Path.Combine(folder, id + ".json");
        ValidateStableCleanupPaths(installation, path);
        using var installer = Process.GetCurrentProcess();
        return (new UpdateRestart.Receipt(id, target, directory, [], InstallerPid: installer.Id,
            InstallerStartTicks: installer.StartTime.ToUniversalTime().Ticks), path);
    }

    private static void ValidateStableCleanupPaths(QueueLoom.Core.Updates.VersionInstallation installation, string path)
    {
        var id = Path.GetFileNameWithoutExtension(path);
        foreach (var entry in new[] { path, path + ".handoff", path + ".helper", path + "." + id + ".ready",
                     path + "." + id + ".download-cleaned", path + "." + id + ".download-cleaned.tmp" })
            installation.RejectOwnedPath(entry);
    }

    private static void CleanStableDownload(QueueLoom.Core.Updates.VersionInstallation installation, UpdateTarget target,
        UpdateRestart.Receipt receipt, string path)
    {
        if (receipt.Target != target || !Guid.TryParseExact(receipt.Id, "N", out _) ||
            Path.GetFileName(path) != receipt.Id + ".json" || receipt.Entries is not { Length: 0 } ||
            receipt.Recovered || receipt.BundleBackupDirectories is not null || string.IsNullOrEmpty(receipt.DownloadDirectory) ||
            !Path.GetFileName(receipt.DownloadDirectory).EndsWith("-" + receipt.Id, StringComparison.Ordinal))
            throw new InvalidDataException("The download cleanup receipt does not match this installation.");
        ValidateStableCleanupPaths(installation, path);
        if (UpdateRestart.InstallerIsAlive(receipt)) return;
        try
        {
            ValidateDownloadTree(receipt.DownloadDirectory, Path.GetDirectoryName(receipt.DownloadDirectory)!);
            UpdateRestart.CleanAsync(receipt, path, TimeSpan.Zero).GetAwaiter().GetResult();
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException) { }
    }

    private static void ValidateDownloadTree(string directory, string boundary)
    {
        QueueLoom.Core.Updates.VersionInstallation.RejectLinks(directory, boundary);
        if (!Directory.Exists(directory)) return;
        foreach (var entry in Directory.EnumerateFileSystemEntries(directory))
        {
            QueueLoom.Core.Updates.VersionInstallation.RejectLinks(entry, boundary);
            if (Directory.Exists(entry)) ValidateDownloadTree(entry, boundary);
        }
    }

    private async Task<string> DownloadChecksumAsync(Uri uri, CancellationToken cancellationToken)
    {
        using var response = await httpClient.GetAsync(uri, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException("The release has no checksum for this system's package, so it was not installed.");
        }
        var text = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var hash = text.Split([' ', '\t', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        return hash is { Length: 64 } && hash.All(Uri.IsHexDigit)
            ? hash
            : throw new InvalidOperationException("The published checksum could not be read, so the update was not installed.");
    }

    private static void ReplaceFile(string current, string replacement, List<(string, string)> moves, List<string> added, string id)
    {
        if (File.Exists(current))
        {
            var old = current + "." + id + ".old";
            UpdateRestart.MoveRetrying(() => File.Move(current, old));
            moves.Add((current, old));
        }
        else
        {
            added.Add(current);
        }
        UpdateRestart.MoveRetrying(() => File.Move(replacement, current));
    }

    private static void ReplaceDirectory(string current, string replacement, List<(string, string)> moves, List<string> added, string id)
    {
        var old = current + "." + id + ".old";
        UpdateRestart.MoveRetrying(() => Directory.Move(current, old));
        moves.Add((current, old));
        added.Add(current);
        UpdateRestart.MoveRetrying(() => Directory.Move(replacement, current));
    }

    private static void MakeExecutable(string path)
    {
        if (!OperatingSystem.IsWindows() && File.Exists(path))
        {
            File.SetUnixFileMode(path, File.GetUnixFileMode(path) |
                                       UnixFileMode.UserExecute | UnixFileMode.GroupExecute | UnixFileMode.OtherExecute);
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
            else if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // Still in use (the old program may be running for a moment longer); the next start tries again.
        }
    }
}
