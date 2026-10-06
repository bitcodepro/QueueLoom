using System.Security.Cryptography;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.App.Services;

/// <summary>Copies message backups out of replaced bundles without overwriting either copy.</summary>
internal static class MacBackupMigration
{
    private static readonly string LegacyDirectory = Path.Combine("Contents", "MacOS", "backups");

    internal static string[]? CaptureDirectories(UpdateTarget target, string? backupOverride = null)
    {
        if (target.Bundle is not { } bundle) return null;
        var directories = new List<string> { LegacyDirectory };
        backupOverride ??= Environment.GetEnvironmentVariable("QUEUELOOM_BACKUP_DIRECTORY");
        if (!string.IsNullOrWhiteSpace(backupOverride))
        {
            var relative = Path.GetRelativePath(Path.GetFullPath(bundle), Path.GetFullPath(backupOverride));
            if (relative == ".") throw new IOException("The application bundle itself cannot be used as a backup directory.");
            if (!Path.IsPathRooted(relative) && relative != ".." && !relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                directories.Add(relative);
        }
        return directories.Distinct(StringComparer.Ordinal).ToArray();
    }

    internal static void Preserve(UpdateRestart.Receipt receipt, string sourceBundle)
    {
        if (receipt.Target.Bundle is not { } bundle || !Directory.Exists(sourceBundle)) return;
        foreach (var relative in receipt.BundleBackupDirectories ?? [LegacyDirectory])
        {
            var source = Path.GetFullPath(Path.Combine(sourceBundle, relative));
            var sourceRelative = Path.GetRelativePath(Path.GetFullPath(sourceBundle), source);
            if (sourceRelative == "." || Path.IsPathRooted(sourceRelative) || sourceRelative == ".." ||
                sourceRelative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                throw new InvalidDataException("The update receipt contains an unexpected backup directory.");
            var destination = QueueLoomPaths.OutsideApplicationBundle(Path.Combine(bundle, relative),
                Path.Combine(bundle, "Contents", "MacOS"));
            if (Directory.Exists(source)) CopyDirectory(source, destination);
        }
    }

    private static void RejectLinks(string path)
    {
        for (var current = Path.GetFullPath(path); current is not null; current = Path.GetDirectoryName(current))
            if ((File.Exists(current) || Directory.Exists(current)) && (File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0)
                throw new IOException($"Backup preservation cannot follow a link: {current}");
    }

    private static void CopyDirectory(string source, string destination)
    {
        RejectLinks(source);
        RejectLinks(destination);
        Directory.CreateDirectory(destination);
        foreach (var entry in Directory.EnumerateFileSystemEntries(source))
        {
            RejectLinks(entry);
            var target = Path.Combine(destination, Path.GetFileName(entry));
            if (Directory.Exists(entry)) CopyDirectory(entry, target);
            else CopyFile(entry, target);
        }
    }

    private static void CopyFile(string source, string destination)
    {
        RejectLinks(destination);
        using var input = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read);
        var hash = SHA256.HashData(input);
        if (File.Exists(destination))
        {
            using var existing = new FileStream(destination, FileMode.Open, FileAccess.Read, FileShare.Read);
            if (SHA256.HashData(existing).AsSpan().SequenceEqual(hash)) return;
            destination = Path.Combine(Path.GetDirectoryName(destination)!,
                Path.GetFileNameWithoutExtension(destination) + ".recovered-" + Convert.ToHexString(hash) + Path.GetExtension(destination));
            RejectLinks(destination);
            if (File.Exists(destination))
            {
                using var recovered = File.OpenRead(destination);
                if (SHA256.HashData(recovered).AsSpan().SequenceEqual(hash)) return;
                throw new IOException("A conflicting recovered backup already exists; the old bundle was retained.");
            }
        }
        input.Position = 0;
        var temporary = destination + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            using (var output = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            { input.CopyTo(output); output.Flush(flushToDisk: true); }
            File.Move(temporary, destination, overwrite: false);
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }
}
