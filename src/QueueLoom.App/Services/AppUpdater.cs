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
public sealed class AppUpdater(HttpClient httpClient, string? downloadRoot = null)
{
    public const string ReleasesDownload = "https://github.com/bitcodepro/QueueLoom/releases/download";
    private const string DownloadMarker = UpdateRestart.DownloadMarker;

    private readonly string _downloadRoot = downloadRoot ?? Path.Combine(Path.GetTempPath(), "QueueLoom-update");

    /// <summary>The running installation, or null when QueueLoom runs from a build folder or an unknown system.</summary>
    public static UpdateTarget? CurrentTarget()
    {
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

    /// <summary>False when the program folder cannot be written (for example under Program Files).</summary>
    public static bool CanInstall(UpdateTarget target)
    {
        try
        {
            var probe = Path.Combine(target.InstallDirectory, $".queueloom-update-{Guid.NewGuid():N}");
            using (File.Create(probe, 1, FileOptions.DeleteOnClose))
            {
            }
            return true;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
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
        try
        {
            Directory.CreateDirectory(folder);
            File.WriteAllText(Path.Combine(folder, DownloadMarker), id);

            var archive = Path.Combine(folder, PackageName(version, target.Rid));
            progress?.Report(new UpdateProgress(0, null) { Phase = phase });
            var expected = await DownloadChecksumAsync(new Uri(package + ".sha256"), cancellationToken).ConfigureAwait(false);
            phase = UpdatePhase.Downloading;
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
            progress?.Report(new UpdateProgress(0, null) { Phase = phase });
            await using (var stream = File.OpenRead(archive))
            {
                var actual = Convert.ToHexStringLower(await SHA256.HashDataAsync(stream, cancellationToken).ConfigureAwait(false));
                if (!string.Equals(actual, expected, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("The downloaded package does not match its published checksum, so it was not installed.");
                }
            }

            phase = UpdatePhase.Extraction;
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
            TryDelete(folder);
            if (exception is OperationCanceledException && cancellationToken.IsCancellationRequested) throw;
            throw new UpdateStageException(phase, exception.Message, true, exception);
        }
    }

    /// <summary>Retry after replacement is allowed only with no outstanding transaction receipt.</summary>
    public static async Task InstallWithProgressAsync(UpdateTarget target, string staging, IProgress<UpdateProgress> progress, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        progress.Report(new UpdateProgress(0, null) { Phase = UpdatePhase.Installation });
        try { await Task.Run(() => Install(target, staging), CancellationToken.None); }
        catch (Exception exception)
        {
            var safe = !File.Exists(UpdateRestart.ReceiptPath(target));
            throw new UpdateStageException(safe ? UpdatePhase.Installation : UpdatePhase.Recovery,
                exception.Message, safe, exception);
        }
    }

    /// <summary>Puts the unpacked files in place; the running ones become ".old". Rolls back on any failure.</summary>
    public static void Install(UpdateTarget target, string staging)
    {
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
            InstallerPid: installer.Id, InstallerStartTicks: installer.StartTime.ToUniversalTime().Ticks);
        var moves = new List<(string Current, string Old)>();
        var added = new List<string>();
        // A previous unfinished update must be recovered, not overwritten.
        using (var stream = new FileStream(receiptPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            JsonSerializer.Serialize(stream, receipt);
        try
        {
            if (target.Bundle is { } bundle)
            {
                var newBundle = Path.Combine(staging, "QueueLoom.app");
                if (!Directory.Exists(newBundle) || !File.Exists(Path.Combine(newBundle, "Contents", "MacOS", "QueueLoom")))
                {
                    throw new InvalidOperationException("The package does not contain a complete QueueLoom.app with its executable.");
                }
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
                    Directory.Move(old, current);
                }
                else if (File.Exists(old))
                {
                    File.Move(old, current, overwrite: true);
                }
            }
            File.Delete(receiptPath);
            throw;
        }
    }

    /// <summary>Starts the installed version; the caller then closes this one.</summary>
    public static void StartInstalled(UpdateTarget target)
    {
        try { UpdateRestart.Start(target); }
        catch (Exception exception) when (exception is IOException or System.ComponentModel.Win32Exception)
        {
            var path = UpdateRestart.ReceiptPath(target);
            using var ownership = UpdateRestart.OwnTransaction(path);
            var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
            UpdateRestart.Read(path, receipt.Id);
            UpdateRestart.Restore(receipt);
            File.WriteAllText(path, JsonSerializer.Serialize(receipt with { Recovered = true }));
            throw new IOException("The restart helper could not start. The previous version was restored and is still running.", exception);
        }
    }

    /// <summary>Removes what the previous update left behind (the ".old" files and the download).</summary>
    public void CleanUpPreviousUpdate(UpdateTarget? target)
    {
        if (target is not null && File.Exists(UpdateRestart.ReceiptPath(target)))
        {
            var path = UpdateRestart.ReceiptPath(target);
            // Retain ownership through receipt validation and cleanup, including asynchronous retries.
            FileStream ownership;
            try { ownership = UpdateRestart.OwnTransaction(path); }
            catch (IOException) { return; }
            using var retainedOwnership = ownership;
            if (!File.Exists(path)) return;
            var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
            UpdateRestart.Read(path, receipt.Id);
            // Protect the installed-but-not-yet-accepted handoff between Install and StartInstalled.
            if (UpdateRestart.InstallerIsAlive(receipt)) return;
            // Startup was successful. Retry only the backups recorded by this update.
            UpdateRestart.CleanAsync(receipt, path, TimeSpan.Zero).GetAwaiter().GetResult();
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
            File.Move(current, old);
            moves.Add((current, old));
        }
        else
        {
            added.Add(current);
        }
        File.Move(replacement, current);
    }

    private static void ReplaceDirectory(string current, string replacement, List<(string, string)> moves, List<string> added, string id)
    {
        var old = current + "." + id + ".old";
        Directory.Move(current, old);
        moves.Add((current, old));
        added.Add(current);
        Directory.Move(replacement, current);
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
