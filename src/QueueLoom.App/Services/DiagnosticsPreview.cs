using System.IO.Compression;
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

    public async Task SaveAsync(string destination, CancellationToken token = default)
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
            // Atomic create only: even a save-picker overwrite confirmation must never replace an unrelated file.
            File.Move(staging, full, overwrite: false);
        }
        finally
        {
            if (ownsStaging && File.Exists(staging)) File.Delete(staging);
        }
    }

    private static async Task Write(ZipArchive archive, string name, string content, CancellationToken token)
    {
        var entry = archive.CreateEntry(name, CompressionLevel.Optimal);
        entry.LastWriteTime = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await using var stream = entry.Open();
        await stream.WriteAsync(Encoding.UTF8.GetBytes(content), token);
    }
}
