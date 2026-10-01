using System.Diagnostics;
using System.Text.Json;

namespace QueueLoom.App.Services;

/// <summary>An update handoff runs without Avalonia or application storage. Only its own files are removed.</summary>
public static class UpdateRestart
{
    public const string ReceiptName = ".queueloom-update.json";
    public const string DownloadMarker = ".queueloom-download";
    public sealed record Entry(string Current, string? Backup);
    public sealed record Receipt(string Id, UpdateTarget Target, string DownloadDirectory, Entry[] Entries, bool Recovered = false);
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
        File.WriteAllText(ready + ".tmp", _startupId);
        File.Move(ready + ".tmp", ready, overwrite: true);
        _startupReceipt = null;
        _startupId = null;
        return true;
    }

    public static async Task<int> RunAsync(string path, string id, int parentPid, long parentStartTicks,
        TimeSpan? exitTimeout = null, TimeSpan? startupTimeout = null)
    {
        var receipt = Read(path, id);
        var ready = path + "." + id + ".ready";
        using var handoff = new FileStream(path + ".handoff", FileMode.OpenOrCreate, FileAccess.ReadWrite,
            FileShare.None, 1, FileOptions.DeleteOnClose);
        File.WriteAllText(path + ".helper.tmp", Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));
        File.Move(path + ".helper.tmp", path + ".helper", overwrite: true);
        try
        {
            try
            {
                using var parent = Process.GetProcessById(parentPid);
                if (parent.StartTime.ToUniversalTime().Ticks == parentStartTicks)
                    await parent.WaitForExitAsync().WaitAsync(exitTimeout ?? TimeSpan.FromMinutes(2));
            }
            catch (ArgumentException) { /* Already exited. */ }
        }
        catch (Exception exception)
        {
            // Never replace files or kill a process when the old application is still running.
            File.WriteAllText(path + ".error", "Waiting for the previous application to exit: " + exception.Message);
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
            while (!File.Exists(ready) || File.ReadAllText(ready) != id)
            {
                if (child.HasExited) throw new IOException($"The updated application exited before startup completed ({child.ExitCode}).");
                if (startup.Elapsed >= (startupTimeout ?? TimeSpan.FromMinutes(2)))
                    throw new TimeoutException("The updated application did not confirm startup.");
                await Task.Delay(100);
            }
            try { await CleanAsync(receipt, path, TimeSpan.FromSeconds(30)); }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or InvalidDataException)
            {
                // A healthy updated application must never be rolled back because cleanup failed.
                File.WriteAllText(path + ".error", "Update started successfully; cleanup will be retried: " + exception.Message);
            }
            return 0;
        }
        catch (Exception exception)
        {
            if (child is not null && !child.HasExited)
            {
                child.Kill();
                await child.WaitForExitAsync();
            }
            File.WriteAllText(path + ".error", "Updated application failed; restoring the previous version: " + exception.Message);
            Restore(receipt);
            File.WriteAllText(path, JsonSerializer.Serialize(receipt with { Recovered = true }));
            // The helper may still map the failed executable on Windows. Keep it as evidence, never delete the backup first.
            using var recovered = Process.Start(new ProcessStartInfo(receipt.Target.Executable)
                { UseShellExecute = false, WorkingDirectory = receipt.Target.InstallDirectory });
            return 1;
        }
        finally { child?.Dispose(); }
    }

    public static void Restore(Receipt receipt)
    {
        foreach (var entry in receipt.Entries.Reverse())
        {
            if (entry.Backup is not null && !File.Exists(entry.Backup) && !Directory.Exists(entry.Backup)) continue;
            var failed = entry.Current + "." + receipt.Id + ".failed";
            if (Directory.Exists(entry.Current)) Directory.Move(entry.Current, failed);
            else if (File.Exists(entry.Current)) File.Move(entry.Current, failed);
            if (entry.Backup is not null)
            {
                if (Directory.Exists(entry.Backup)) Directory.Move(entry.Backup, entry.Current);
                else File.Move(entry.Backup, entry.Current);
            }
        }
    }

    public static async Task CleanAsync(Receipt receipt, string path, TimeSpan timeout)
    {
        var deadline = Stopwatch.StartNew();
        do
        {
            foreach (var entry in receipt.Entries)
            {
                if (entry.Backup is not null) TryDelete(entry.Backup);
                if (receipt.Recovered) TryDelete(entry.Current + "." + receipt.Id + ".failed");
            }
            if (receipt.Entries.All(entry => (entry.Backup is null || (!File.Exists(entry.Backup) && !Directory.Exists(entry.Backup))) &&
                (!receipt.Recovered || (!File.Exists(entry.Current + "." + receipt.Id + ".failed") && !Directory.Exists(entry.Current + "." + receipt.Id + ".failed")))))
            {
                if (!string.IsNullOrEmpty(receipt.DownloadDirectory))
                {
                    var marker = Path.Combine(receipt.DownloadDirectory, DownloadMarker);
                    if (!Path.GetFileName(receipt.DownloadDirectory).EndsWith("-" + receipt.Id, StringComparison.Ordinal) ||
                        !File.Exists(marker) || File.ReadAllText(marker) != receipt.Id)
                        throw new InvalidDataException("The update download is not owned by this transaction.");
                    TryDelete(receipt.DownloadDirectory);
                    if (Directory.Exists(receipt.DownloadDirectory))
                    {
                        // Recursive deletion can remove the marker before reaching a locked archive.
                        // Keep ownership verifiable while this exact transaction retries the remainder.
                        if (!File.Exists(marker)) File.WriteAllText(marker, receipt.Id);
                        if (deadline.Elapsed >= timeout) return;
                        await Task.Delay(200);
                        continue;
                    }
                }
                TryDelete(path + "." + receipt.Id + ".ready");
                TryDelete(path + ".helper");
                TryDelete(path);
                return;
            }
            if (deadline.Elapsed >= timeout) return; // Keep the receipt for the next successful startup.
            await Task.Delay(200);
        } while (true);
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
}
