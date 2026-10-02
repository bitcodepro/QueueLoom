using System.ComponentModel;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Text;

namespace QueueLoom.App.Services;

/// <summary>Frozen export bytes: preview and ZIP always refer to this same capture.</summary>
public sealed class DiagnosticsPreview
{
    public const int MaximumOutputBytes = 256 * 1024;
    public string Report { get; }
    public string Json { get; }
    internal DiagnosticsPreview(string report, string json)
    {
        if (Encoding.UTF8.GetByteCount(report) + Encoding.UTF8.GetByteCount(json) > MaximumOutputBytes)
            throw new InvalidDataException("Diagnostics size limit exceeded.");
        Report = report; Json = json;
    }

    public Task SaveAsync(string destination, CancellationToken token = default) =>
        SaveAsync(destination, token, PublishCreateOnly);

    // Per-call publication seam: tests can arrange external creation/replacement and I/O failures
    // after a complete ZIP has been staged, without shared hooks or changing production publication.
    internal async Task SaveAsync(string destination, CancellationToken token, Action<string, string> publish)
    {
        token.ThrowIfCancellationRequested();
        var full = Path.GetFullPath(destination);
        if (OperatingSystem.IsWindows() && Path.GetPathRoot(full)?.StartsWith("\\\\", StringComparison.Ordinal) == true)
            throw new InvalidDataException("Choose a local ZIP destination.");
        if (!string.Equals(Path.GetExtension(full), ".zip", StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Choose a local ZIP destination.");
        var directory = Path.GetDirectoryName(full)!;
        var staging = Path.Combine(directory, $".queueloom-diagnostics-{Guid.NewGuid():N}.tmp");
        var ownsStaging = false;
        try
        {
            await using (var file = new FileStream(staging, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None))
            {
                ownsStaging = true;
                using (var zip = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
                {
                    await Write(zip, "report.txt", Report, token);
                    await Write(zip, "diagnostics.json", Json, token);
                }
                await file.FlushAsync(token);
            }
            token.ThrowIfCancellationRequested();
            publish(staging, full);
        }
        finally
        {
            if (ownsStaging && File.Exists(staging)) File.Delete(staging);
        }
    }

    internal static void PublishCreateOnly(string staging, string destination)
    {
        if (OperatingSystem.IsWindows())
        {
            // Windows' move is a native no-replace operation. Unix File.Move instead checks
            // existence before rename(), which can overwrite a file created between those calls.
            File.Move(staging, destination, overwrite: false);
            return;
        }
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS())
            throw new PlatformNotSupportedException("Create-only diagnostics publication is unavailable on this platform.");

        // POSIX link() creates the destination atomically and fails if any entry already exists.
        // The same-directory staging file is complete and closed; unlinking only its temporary
        // name afterward leaves the ZIP intact. Do not fall back to check-then-rename or copying.
        if (Link(staging, destination) != 0)
        {
            var error = Marshal.GetLastPInvokeError();
            throw new IOException("Could not publish diagnostics without replacing a destination. " +
                "Choose a new filename on a local filesystem that supports hard links.", new Win32Exception(error));
        }
    }

    [DllImport("libc", EntryPoint = "link", CharSet = CharSet.Ansi, SetLastError = true)]
    private static extern int Link(string existing, string destination);

    private static async Task Write(ZipArchive archive, string name, string content, CancellationToken token)
    {
        var entry = archive.CreateEntry(name, CompressionLevel.Optimal);
        entry.ExternalAttributes = 0;
        entry.LastWriteTime = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await using var stream = entry.Open();
        await stream.WriteAsync(Encoding.UTF8.GetBytes(content), token);
    }
}
