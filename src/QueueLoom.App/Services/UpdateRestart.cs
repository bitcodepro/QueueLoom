using System.Diagnostics;
using System.Text.Json;
using QueueLoom.Core.IO;

namespace QueueLoom.App.Services;

/// <summary>An update handoff runs without Avalonia or application storage. Only its own files are removed.</summary>
public static class UpdateRestart
{
    public enum RecordedRecovery { StartupAcknowledged, Restored }
    public static event Action<RecordedRecovery>? RecoveryRecorded;
    private static void RecordRecovery(RecordedRecovery fact)
    {
        try { RecoveryRecorded?.Invoke(fact); }
        catch { /* Optional diagnostics must never interrupt recovery. */ }
    }
    public const string ReceiptName = ".queueloom-update.json";
    public const string DownloadMarker = ".queueloom-download";
    public sealed record Entry(string Current, string? Backup);
    public sealed record Receipt(string Id, UpdateTarget Target, string DownloadDirectory, Entry[] Entries, bool Recovered = false,
        int InstallerPid = 0, long InstallerStartTicks = 0)
    {
        public string[]? BundleBackupDirectories { get; init; }
    }
    private static string? _startupReceipt;
    private static string? _startupId;

    public static string ReceiptPath(UpdateTarget target) => Path.Combine(target.InstallDirectory, ReceiptName);
    public static Receipt Read(string path, string id)
    {
        var receipt = JsonSerializer.Deserialize<Receipt>(File.ReadAllText(path))
            ?? throw new InvalidDataException("The update receipt is empty.");
        if (!Guid.TryParseExact(receipt.Id, "N", out _) || receipt.Id != id ||
            Path.GetFullPath(path) != Path.GetFullPath(ReceiptPath(receipt.Target)))
            throw new InvalidDataException("The update receipt does not match this installation.");
        foreach (var entry in receipt.Entries)
        {
            if (Path.GetDirectoryName(Path.GetFullPath(entry.Current)) != Path.GetFullPath(receipt.Target.InstallDirectory) ||
                (entry.Backup is not null && entry.Backup != entry.Current + "." + id + ".old"))
                throw new InvalidDataException("The update receipt contains an unexpected file.");
        }
        var installed = receipt.Target.Bundle ?? receipt.Target.Executable;
        if (receipt.Target.Bundle is { } bundle && Path.GetFullPath(receipt.Target.Executable) !=
            Path.Combine(Path.GetFullPath(bundle), "Contents", "MacOS", "QueueLoom"))
            throw new InvalidDataException("The update executable is outside its application bundle.");
        if (!receipt.Entries.Any(entry => entry.Current == installed))
            throw new InvalidDataException("The update receipt does not contain the application.");
        return receipt;
    }

    public static int Start(UpdateTarget target)
    {
        var path = ReceiptPath(target);
        var receipt = JsonSerializer.Deserialize<Receipt>(File.ReadAllText(path))!;
        Read(path, receipt.Id);
        using var parent = Process.GetCurrentProcess();
        var start = new ProcessStartInfo(target.Executable) { UseShellExecute = false, WorkingDirectory = target.InstallDirectory };
        foreach (var argument in new[] { "--update-helper", path, receipt.Id, parent.Id.ToString(System.Globalization.CultureInfo.InvariantCulture),
                     parent.StartTime.ToUniversalTime().Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture) })
            start.ArgumentList.Add(argument);
        using var helper = Process.Start(start) ?? throw new IOException("The update restart helper could not start.");
        var started = Stopwatch.StartNew();
        while (!File.Exists(path + ".helper") || File.ReadAllText(path + ".helper") != helper.Id.ToString(System.Globalization.CultureInfo.InvariantCulture))
        {
            if (helper.HasExited) throw new IOException("The update restart helper exited before accepting the handoff.");
            if (started.Elapsed > TimeSpan.FromSeconds(10))
            {
                helper.Kill();
                helper.WaitForExit();
                throw new IOException("The update restart helper did not accept the handoff.");
            }
            Thread.Sleep(20);
        }
        return helper.Id;
    }

    /// <summary>Called before any GUI or MCP initialization.</summary>
    public static bool HandleArguments(string[] args, out int exitCode)
    {
        exitCode = 0;
        if (args.Length == 3 && args[0] == "--update-startup")
        {
            Read(args[1], args[2]);
            _startupReceipt = args[1];
            _startupId = args[2];
            return false;
        }
        if (args.Length == 5 && args[0] == "--update-helper")
        {
            exitCode = RunAsync(args[1], args[2], int.Parse(args[3], System.Globalization.CultureInfo.InvariantCulture),
                long.Parse(args[4], System.Globalization.CultureInfo.InvariantCulture)).GetAwaiter().GetResult();
            return true;
        }
        return false;
    }

    /// <summary>Acknowledge only after the main window and local data have initialized successfully.</summary>
    public static bool AcknowledgeStartup()
    {
        if (_startupReceipt is null || _startupId is null) return false;
        var ready = _startupReceipt + "." + _startupId + ".ready";
        // Flushed before it is renamed into place: a power cut must not leave an empty acknowledgement behind.
        SafeFileWriter.WriteText(ready, _startupId, narrowGroup: false);
        _startupReceipt = null;
        _startupId = null;
        RecordRecovery(RecordedRecovery.StartupAcknowledged);
        return true;
    }

    public static async Task<int> RunAsync(string path, string id, int parentPid, long parentStartTicks,
        TimeSpan? exitTimeout = null, TimeSpan? startupTimeout = null)
    {
        var receipt = Read(path, id);
        var ready = path + "." + id + ".ready";
        using var handoff = OwnTransaction(path);
        // A legacy updater starts the stable entry and expects that process's PID in its readiness file.
        var helperPid = QueueLoom.Core.Updates.PayloadLaunch.Current?.LauncherProcessId ?? Environment.ProcessId;
        SafeFileWriter.WriteText(path + ".helper", helperPid.ToString(System.Globalization.CultureInfo.InvariantCulture),
            narrowGroup: false);
        try
        {
            try
            {
                using var parent = Process.GetProcessById(parentPid);
                // Unix reconstructs StartTime from boot time; tiny cross-process differences must not imply exit.
                // Waiting for any live PID there is conservative, including the rare PID-reuse case.
                if (!OperatingSystem.IsWindows() || parent.StartTime.ToUniversalTime().Ticks == parentStartTicks)
                    await parent.WaitForExitAsync().WaitAsync(exitTimeout ?? TimeSpan.FromMinutes(2));
            }
            catch (ArgumentException) { /* Already exited. */ }
        }
        catch (Exception exception)
        {
            // Never replace files or kill a process when the old application is still running.
            TryWriteError(path, "Waiting for the previous application to exit: " + exception.Message);
            return 1;
        }

        Process? child = null;
        try
        {
            var deadline = Stopwatch.StartNew();
            while (child is null)
            {
                try
                {
                    var start = new ProcessStartInfo(receipt.Target.Executable)
                        { UseShellExecute = false, WorkingDirectory = receipt.Target.InstallDirectory };
                    start.ArgumentList.Add("--update-startup");
                    start.ArgumentList.Add(path);
                    start.ArgumentList.Add(id);
                    child = Process.Start(start) ?? throw new IOException("The updated application could not start.");
                }
                catch (Exception exception) when ((exception is IOException or System.ComponentModel.Win32Exception) &&
                                                  deadline.Elapsed < TimeSpan.FromSeconds(30))
                {
                    await Task.Delay(200);
                }
            }
            var startup = Stopwatch.StartNew();
            while (!Acknowledged(ready, id))
            {
                if (child.HasExited)
                {
                    // It may have acknowledged and then closed while its acknowledgement was briefly held open.
                    if (await AcknowledgedWithinAsync(ready, id, TimeSpan.FromSeconds(5))) break;
                    throw new IOException($"The updated application exited before startup completed ({child.ExitCode}).");
                }
                if (startup.Elapsed >= (startupTimeout ?? TimeSpan.FromMinutes(2)))
                    throw new TimeoutException("The updated application did not confirm startup.");
                await Task.Delay(100);
            }
        }
        catch (Exception exception)
        {
            if (child is not null && !child.HasExited)
            {
                child.Kill();
                await child.WaitForExitAsync();
            }
            TryWriteError(path, "Updated application failed; restoring the previous version: " + exception.Message);
            Restore(receipt);
            // Written through a temporary file: a crash here must not leave the receipt empty or cut short.
            SafeFileWriter.WriteText(path, JsonSerializer.Serialize(receipt with { Recovered = true }), narrowGroup: false);
            // The helper may still map the failed executable on Windows. Keep it as evidence, never delete the backup first.
            using var recovered = Process.Start(new ProcessStartInfo(receipt.Target.Executable)
                { UseShellExecute = false, WorkingDirectory = receipt.Target.InstallDirectory });
            return 1;
        }
        finally { child?.Dispose(); }

        // Cleanup is outside the startup-failure handler: it cannot kill or roll back a healthy child.
        try { await CleanAsync(receipt, path, TimeSpan.FromSeconds(30)); }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or InvalidDataException)
        {
            TryWriteError(path, "Update started successfully; cleanup will be retried: " + exception.Message);
        }
        return 0;
    }

    public static void Restore(Receipt receipt)
    {
        // Do not report recovery after silently skipping a missing previous file.
        // Check every recorded backup before moving anything, retaining the receipt on failure.
        foreach (var entry in receipt.Entries)
            if (entry.Backup is not null && !File.Exists(entry.Backup) && !Directory.Exists(entry.Backup))
                throw new IOException($"Recovery cannot verify the previous installation: backup is missing for {Path.GetFileName(entry.Current)}. Keep the update receipt and remaining files.");
        foreach (var entry in receipt.Entries.Reverse())
        {
            if (entry.Current == receipt.Target.Bundle)
                MacBackupMigration.Preserve(receipt, entry.Current);
            var failed = entry.Current + "." + receipt.Id + ".failed";
            if ((File.Exists(failed) || Directory.Exists(failed)) &&
                (File.Exists(entry.Current) || Directory.Exists(entry.Current)))
            {
                // A prior entry may already be restored. Require the retained backup to match; the
                // mere presence of a current file must never turn a missing or tampered backup into success.
                if (entry.Backup is not null && RecoveryMatches(entry.Backup, entry.Current)) continue;
                throw new IOException($"Recovery cannot verify the partially restored {Path.GetFileName(entry.Current)}. Keep the update receipt and remaining files.");
            }
            if (Directory.Exists(entry.Current)) MoveRetrying(() => Directory.Move(entry.Current, failed));
            else if (File.Exists(entry.Current)) MoveRetrying(() => File.Move(entry.Current, failed));
            if (entry.Backup is not null)
            {
                // Stage a complete copy alongside the installation, then publish it. The recorded backup
                // stays intact through every entry and retry, including a crash during the copy.
                var staged = entry.Current + "." + receipt.Id + ".restore-" + Guid.NewGuid().ToString("N");
                try
                {
                    CopyRecoveryPath(entry.Backup, staged);
                    if (Directory.Exists(staged)) MoveRetrying(() => Directory.Move(staged, entry.Current));
                    else MoveRetrying(() => File.Move(staged, entry.Current));
                }
                finally { TryDelete(staged); }
            }
        }
        if (!File.Exists(receipt.Target.Executable))
            throw new IOException("Recovery did not restore a runnable previous executable. Keep the update receipt and remaining files.");
        RecordRecovery(RecordedRecovery.Restored);
    }

    private static void CopyRecoveryPath(string source, string destination)
    {
        var attributes = File.GetAttributes(source);
        var directory = (attributes & FileAttributes.Directory) != 0;
        if ((attributes & FileAttributes.ReparsePoint) != 0)
        {
            // Preserve bundle links themselves; never recursively follow a link outside the recorded backup.
            var target = (directory ? (FileSystemInfo)new DirectoryInfo(source) : new FileInfo(source)).LinkTarget
                ?? throw new IOException("Recovery cannot copy an unsupported filesystem link.");
            if (directory) Directory.CreateSymbolicLink(destination, target);
            else File.CreateSymbolicLink(destination, target);
        }
        else if (directory)
        {
            Directory.CreateDirectory(destination);
            foreach (var child in Directory.EnumerateFileSystemEntries(source))
                CopyRecoveryPath(child, Path.Combine(destination, Path.GetFileName(child)));
            if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(destination, File.GetUnixFileMode(source));
        }
        else
        {
            using (var input = File.OpenRead(source))
            using (var output = new FileStream(destination, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                input.CopyTo(output);
                output.Flush(flushToDisk: true);
            }
            if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(destination, File.GetUnixFileMode(source));
        }
    }

    private static bool RecoveryMatches(string backup, string current)
    {
        var backupAttributes = File.GetAttributes(backup);
        var currentAttributes = File.GetAttributes(current);
        const FileAttributes kind = FileAttributes.Directory | FileAttributes.ReparsePoint;
        if ((backupAttributes & kind) != (currentAttributes & kind)) return false;
        var directory = (backupAttributes & FileAttributes.Directory) != 0;
        if ((backupAttributes & FileAttributes.ReparsePoint) != 0)
        {
            FileSystemInfo left = directory ? new DirectoryInfo(backup) : new FileInfo(backup);
            FileSystemInfo right = directory ? new DirectoryInfo(current) : new FileInfo(current);
            return left.LinkTarget is not null && left.LinkTarget == right.LinkTarget;
        }
        if (!OperatingSystem.IsWindows() && File.GetUnixFileMode(backup) != File.GetUnixFileMode(current)) return false;
        if (directory)
        {
            var left = Directory.EnumerateFileSystemEntries(backup).OrderBy(Path.GetFileName, StringComparer.Ordinal).ToArray();
            var right = Directory.EnumerateFileSystemEntries(current).OrderBy(Path.GetFileName, StringComparer.Ordinal).ToArray();
            return left.Length == right.Length && left.Zip(right).All(pair =>
                Path.GetFileName(pair.First) == Path.GetFileName(pair.Second) && RecoveryMatches(pair.First, pair.Second));
        }
        if (new FileInfo(backup).Length != new FileInfo(current).Length) return false;
        using var backupStream = File.OpenRead(backup);
        using var currentStream = File.OpenRead(current);
        return System.Security.Cryptography.SHA256.HashData(backupStream).AsSpan()
            .SequenceEqual(System.Security.Cryptography.SHA256.HashData(currentStream));
    }

    public static async Task CleanAsync(Receipt receipt, string path, TimeSpan timeout)
    {
        var deadline = Stopwatch.StartNew();
        do
        {
            foreach (var entry in receipt.Entries)
            {
                // Copy again after the old process exited: it may have written a final backup during handoff.
                // Any preservation failure leaves the old bundle and receipt available for retry.
                if (entry.Current == receipt.Target.Bundle)
                {
                    if (entry.Backup is not null) MacBackupMigration.Preserve(receipt, entry.Backup);
                    if (receipt.Recovered) MacBackupMigration.Preserve(receipt, entry.Current + "." + receipt.Id + ".failed");
                }
                if (entry.Backup is not null) TryDelete(entry.Backup);
                if (receipt.Recovered) TryDelete(entry.Current + "." + receipt.Id + ".failed");
            }
            if (receipt.Entries.All(entry => (entry.Backup is null || (!File.Exists(entry.Backup) && !Directory.Exists(entry.Backup))) &&
                (!receipt.Recovered || (!File.Exists(entry.Current + "." + receipt.Id + ".failed") && !Directory.Exists(entry.Current + "." + receipt.Id + ".failed")))))
            {
                if (!string.IsNullOrEmpty(receipt.DownloadDirectory))
                {
                    var marker = Path.Combine(receipt.DownloadDirectory, DownloadMarker);
                    var cleanupProof = path + "." + receipt.Id + ".download-cleaned";
                    if (!Path.GetFileName(receipt.DownloadDirectory).EndsWith("-" + receipt.Id, StringComparison.Ordinal) ||
                        File.Exists(receipt.DownloadDirectory))
                        throw new InvalidDataException("The update download is not owned by this transaction.");
                    // A previous cleanup or OS temp maintenance may already have removed this owned directory.
                    // Payload deletion requires the exact marker; terminal cleanup proof permits only empty-directory removal.
                    if (Directory.Exists(receipt.DownloadDirectory))
                    {
                        if (File.Exists(marker) && File.ReadAllText(marker) == receipt.Id)
                        {
                            // Keep the ownership marker throughout every payload deletion, including retries.
                            foreach (var entry in Directory.EnumerateFileSystemEntries(receipt.DownloadDirectory)
                                         .Where(entry => Path.GetFileName(entry) != DownloadMarker)) TryDelete(entry);
                            if (!Directory.EnumerateFileSystemEntries(receipt.DownloadDirectory)
                                    .Any(entry => Path.GetFileName(entry) != DownloadMarker))
                            {
                                PublishDownloadCleanupProof(cleanupProof, receipt.Id);
                                TryDelete(marker);
                                TryDeleteEmptyDirectory(receipt.DownloadDirectory);
                            }
                        }
                        else if (File.Exists(cleanupProof) && File.ReadAllText(cleanupProof) == receipt.Id &&
                                 !Directory.EnumerateFileSystemEntries(receipt.DownloadDirectory).Any())
                        {
                            // Crash after removing the final marker: durable proof permits only empty-directory removal.
                            TryDeleteEmptyDirectory(receipt.DownloadDirectory);
                        }
                        else throw new InvalidDataException("The update download is not owned by this transaction.");
                    }
                    if (Directory.Exists(receipt.DownloadDirectory))
                    {
                        if (deadline.Elapsed >= timeout) return;
                        await Task.Delay(200);
                        continue;
                    }
                }
                TryDelete(path + "." + receipt.Id + ".ready");
                TryDelete(path + ".helper");
                TryDelete(path);
                if (!File.Exists(path)) TryDelete(path + "." + receipt.Id + ".download-cleaned");
                return;
            }
            if (deadline.Elapsed >= timeout) return; // Keep the receipt for the next successful startup.
            await Task.Delay(200);
        } while (true);
    }

    public static FileStream OwnTransaction(string path) => new(path + ".handoff", FileMode.OpenOrCreate,
        FileAccess.ReadWrite, FileShare.None, 1, FileOptions.DeleteOnClose);

    private static void PublishDownloadCleanupProof(string path, string id)
    {
        if (File.Exists(path))
        {
            if (File.ReadAllText(path) != id) throw new InvalidDataException("The download cleanup proof belongs to another transaction.");
            return;
        }
        var temporary = path + ".tmp";
        try
        {
            using (var stream = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                stream.Write(System.Text.Encoding.UTF8.GetBytes(id));
                stream.Flush(flushToDisk: true);
            }
            File.Move(temporary, path);
        }
        finally { TryDelete(temporary); }
    }

    private static void TryDeleteEmptyDirectory(string path)
    {
        try { Directory.Delete(path); }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException) { }
    }

    public static bool InstallerIsAlive(Receipt receipt)
    {
        if (receipt.InstallerPid <= 0 || receipt.Recovered) return false;
        try
        {
            using var process = Process.GetProcessById(receipt.InstallerPid);
            return !process.HasExited && (!OperatingSystem.IsWindows() ||
                process.StartTime.ToUniversalTime().Ticks == receipt.InstallerStartTicks);
        }
        catch (ArgumentException) { return false; }
    }

    /// <summary>
    /// Whether the new application has acknowledged its startup. An acknowledgement another program briefly holds
    /// open (an antivirus scan, an indexer) is read again on the next check, like one still to come: a sharing
    /// violation is not a failed startup, and treating it as one rolled back a healthy update.
    /// </summary>
    private static bool Acknowledged(string ready, string id)
    {
        try
        {
            return File.Exists(ready) && File.ReadAllText(ready) == id;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    /// <summary>
    /// After the new application exited: whether it acknowledged first. Only an acknowledgement that exists but
    /// cannot be read yet is waited for; a missing one fails at once.
    /// </summary>
    private static async Task<bool> AcknowledgedWithinAsync(string ready, string id, TimeSpan wait)
    {
        var watch = Stopwatch.StartNew();
        while (true)
        {
            if (Acknowledged(ready, id)) return true;
            if (!File.Exists(ready) || watch.Elapsed >= wait) return false;
            await Task.Delay(100);
        }
    }

    private static void TryWriteError(string path, string message)
    {
        try { File.WriteAllText(path + ".error", message); }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException) { }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
            else File.Delete(path);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException) { }
    }

    /// <summary>How long a rename may wait for a file another program has open (Windows only).</summary>
    internal static readonly TimeSpan SharingRetryTime = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Renames, retrying for a while when Windows reports the file in use. An antivirus scanner (Microsoft Defender on a
    /// freshly extracted program, for example) or a search indexer opens new files briefly without sharing deletion,
    /// and a rename in that moment fails with a sharing violation although nothing is wrong. Other errors, and every
    /// error on other systems, are thrown at once.
    /// </summary>
    internal static void MoveRetrying(Action move)
    {
        var started = Environment.TickCount64;
        while (true)
        {
            try
            {
                move();
                return;
            }
            catch (IOException exception) when (OperatingSystem.IsWindows() && (exception.HResult & 0xFFFF) is 32 or 33 &&
                                                Environment.TickCount64 - started < SharingRetryTime.TotalMilliseconds)
            {
                Thread.Sleep(50);
            }
        }
    }
}
