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
            if (relative == ".") return directories.ToArray(); // Invalid override: preserve only actual backup directories.
            if (!Path.IsPathRooted(relative) && relative != ".." && !relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                directories.Add(relative);
        }
        return directories.Distinct(StringComparer.Ordinal).ToArray();
    }

    internal static void Preserve(UpdateRestart.Receipt receipt, string sourceBundle, string? backupOverride = null)
    {
        if (receipt.Target.Bundle is not { } bundle || !Directory.Exists(sourceBundle)) return;
        // An old installer launches the new helper with an older receipt. Recover its inherited custom override too.
        foreach (var relative in (receipt.BundleBackupDirectories ?? CaptureDirectories(receipt.Target, backupOverride) ?? [])
                     .Append(LegacyDirectory).Distinct(StringComparer.Ordinal))
        {
            var source = Path.GetFullPath(Path.Combine(sourceBundle, relative));
            var sourceRelative = Path.GetRelativePath(Path.GetFullPath(sourceBundle), source);
            if (sourceRelative == "." || Path.IsPathRooted(sourceRelative) || sourceRelative == ".." ||
                sourceRelative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                throw new InvalidDataException("The update receipt contains an unexpected backup directory.");
            var destination = QueueLoomPaths.OutsideApplicationBundle(Path.Combine(bundle, relative),
                Path.Combine(bundle, "Contents", "MacOS"));
            if (Directory.Exists(source)) CopyDirectory(source, destination, sourceBundle,
                Path.Combine(Path.GetDirectoryName(Path.GetFullPath(bundle))!, "backups"));
        }
    }

    private static void RejectLinks(string path, string boundary)
    {
        var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(boundary));
        var current = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        var relative = Path.GetRelativePath(root, current);
        if (Path.IsPathRooted(relative) || relative == ".." || relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new IOException("Backup preservation would leave its installation directory.");
        while (true)
        {
            if ((File.Exists(current) || Directory.Exists(current)) && (File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0)
                throw new IOException($"Backup preservation cannot follow a link: {current}");
            // Ancestors above the installation may be OS aliases, such as /var -> /private/var on macOS.
            if (string.Equals(current, root, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)) return;
            current = Path.GetDirectoryName(current) ?? throw new IOException("Backup preservation has no installation boundary.");
        }
    }

    internal static int CopyLegacyDirectory(string source, string destination, string sourceBundle, CancellationToken token, CopyCache? cache = null)
    {
        var relative = Path.GetRelativePath(Path.GetFullPath(sourceBundle), Path.GetFullPath(destination));
        if (!Path.IsPathRooted(relative) && relative != ".." && !relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new IOException("Legacy backups must be copied outside the application bundle.");
        return CopyDirectory(source, destination, sourceBundle, destination, token, cache);
    }

    private static int CopyDirectory(string source, string destination, string sourceBoundary, string destinationBoundary, CancellationToken token = default, CopyCache? cache = null)
    {
        token.ThrowIfCancellationRequested();
        RejectLinks(source, sourceBoundary);
        RejectLinks(destination, destinationBoundary);
        Directory.CreateDirectory(destination);
        var copied = 0;
        foreach (var entry in Directory.EnumerateFileSystemEntries(source))
        {
            token.ThrowIfCancellationRequested();
            RejectLinks(entry, sourceBoundary);
            var target = Path.Combine(destination, Path.GetFileName(entry));
            copied += Directory.Exists(entry) ? CopyDirectory(entry, target, sourceBoundary, destinationBoundary, token, cache)
                : CopyFile(entry, target, destinationBoundary, token, cache);
        }
        return copied;
    }

    private static int CopyFile(string source, string destination, string destinationBoundary, CancellationToken token, CopyCache? cache)
    {
        RejectLinks(destination, destinationBoundary);
        var originalDestination = destination;
        var sourceStamp = cache is null ? null : FileStamp.Read(source);
        if (cache?.IsCurrent(source, destination, destinationBoundary, sourceStamp) == true) return 0;
        int Verified(int copied)
        {
            cache?.Remember(source, originalDestination, destination, sourceStamp);
            return copied;
        }
        using var input = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read);
        var hash = Hash(input, token);
        if (File.Exists(destination))
        {
            using var existing = new FileStream(destination, FileMode.Open, FileAccess.Read, FileShare.Read);
            if (Hash(existing, token).AsSpan().SequenceEqual(hash)) return Verified(0);
            destination = Path.Combine(Path.GetDirectoryName(destination)!,
                Path.GetFileNameWithoutExtension(destination) + ".recovered-" + Convert.ToHexString(hash) + Path.GetExtension(destination));
            RejectLinks(destination, destinationBoundary);
            if (File.Exists(destination))
            {
                using var recovered = File.OpenRead(destination);
                if (Hash(recovered, token).AsSpan().SequenceEqual(hash)) return Verified(0);
                throw new IOException("A conflicting recovered backup already exists; the old bundle was retained.");
            }
        }
        input.Position = 0;
        var temporary = destination + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            using (var output = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            { Copy(input, output, token); output.Flush(flushToDisk: true); }
            token.ThrowIfCancellationRequested();
            File.Move(temporary, destination, overwrite: false);
            return Verified(1);
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }

    // Hashing and copying check the token between chunks: a first migration of a large backup must not hold the MCP
    // server's (or the window's) shutdown until the whole file has been read.
    private static byte[] Hash(Stream stream, CancellationToken token)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = new byte[81_920];
        int read;
        while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
        {
            token.ThrowIfCancellationRequested();
            hash.AppendData(buffer, 0, read);
        }
        return hash.GetHashAndReset();
    }

    private static void Copy(Stream input, Stream output, CancellationToken token)
    {
        var buffer = new byte[81_920];
        int read;
        while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
        {
            token.ThrowIfCancellationRequested();
            output.Write(buffer, 0, read);
        }
    }

    internal sealed record FileStamp(long Length, long ModifiedTicks, long CreatedTicks)
    {
        internal static FileStamp? Read(string path)
        {
            var file = new FileInfo(path);
            return file.Exists ? new(file.Length, file.LastWriteTimeUtc.Ticks, file.CreationTimeUtc.Ticks) : null;
        }
    }

    internal sealed record VerifiedCopy(FileStamp Source, string Destination, FileStamp Target);

    // Only startup uses this metadata hint. Destructive update/rollback/cleanup always re-verify file contents.
    internal sealed class CopyCache(Dictionary<string, VerifiedCopy> entries)
    {
        internal Dictionary<string, VerifiedCopy> Entries { get; } = entries;
        internal bool Changed { get; private set; }
        private static string Key(string source, string destination) => Path.GetFullPath(source) + "\n" + Path.GetFullPath(destination);

        internal bool IsCurrent(string source, string destination, string boundary, FileStamp? sourceStamp)
        {
            if (sourceStamp is null || !Entries.TryGetValue(Key(source, destination), out var copy) || copy is null ||
                copy.Source != sourceStamp || string.IsNullOrEmpty(copy.Destination) || copy.Target is null) return false;
            // A hint naming a place outside the backup folder or a link (a stale entry after the backup folder changed,
            // or an edited file) is not trusted: the file is verified again and the hint replaced, instead of the whole
            // migration failing at every start.
            try
            {
                RejectLinks(copy.Destination, boundary);
            }
            catch (IOException)
            {
                return false;
            }
            return copy.Target == FileStamp.Read(copy.Destination);
        }

        internal void Remember(string source, string requestedDestination, string verifiedDestination, FileStamp? before)
        {
            if (before is null || before != FileStamp.Read(source) || FileStamp.Read(verifiedDestination) is not { } target) return;
            var copy = new VerifiedCopy(before, Path.GetFullPath(verifiedDestination), target);
            var key = Key(source, requestedDestination);
            if (Entries.TryGetValue(key, out var prior) && prior == copy) return;
            Entries[key] = copy;
            Changed = true;
        }
    }
}
