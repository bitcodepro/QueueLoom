using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;

namespace QueueLoom.App.Services;

/// <summary>
/// Replaces a file only once its new contents are complete and flushed to disk, so a failure, a cancellation or a
/// crash while producing them leaves the previous file as it was instead of empty or cut short.
/// <para>
/// The new contents are first written to a temporary file next to the target that only the current user can open, so
/// they are never readable more widely than before, even while being written. The existing file's access is then kept:
/// on Windows, <see cref="File.Replace(string, string, string?)"/> gives the result the replaced file's security
/// descriptor; on Unix, a file whose mode grants its group anything is updated in place (its owner, group and mode stay
/// exactly as they were, which a rename could not keep, since a new file takes the process's group), and any other
/// file is renamed over with its mode.
/// </para>
/// </summary>
internal static class SafeFileWriter
{
    private const UnixFileMode GroupBits = UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute;
    private const UnixFileMode OwnerOnly = UnixFileMode.UserRead | UnixFileMode.UserWrite;

    public static Task WriteTextAsync(string path, string contents, CancellationToken cancellationToken) =>
        WriteAsync(path, (stream, token) => stream.WriteAsync(new UTF8Encoding(false).GetBytes(contents), token).AsTask(),
            cancellationToken);

    public static async Task WriteAsync(string path, Func<Stream, CancellationToken, Task> write, CancellationToken cancellationToken)
    {
        var temporary = TemporaryPathFor(path);
        try
        {
            await using (var stream = CreatePrivate(temporary, FileOptions.Asynchronous))
            {
                await write(stream, cancellationToken).ConfigureAwait(false);
                stream.Flush(flushToDisk: true);
            }
            cancellationToken.ThrowIfCancellationRequested();
            Publish(temporary, path);
        }
        finally
        {
            TryDelete(temporary);
        }
    }

    public static void WriteText(string path, string contents)
    {
        var temporary = TemporaryPathFor(path);
        try
        {
            using (var stream = CreatePrivate(temporary, FileOptions.None))
            {
                stream.Write(new UTF8Encoding(false).GetBytes(contents));
                stream.Flush(flushToDisk: true);
            }
            Publish(temporary, path);
        }
        finally
        {
            TryDelete(temporary);
        }
    }

    /// <summary>A new file only the current user can open, restricted before anything is written to it.</summary>
    private static FileStream CreatePrivate(string temporary, FileOptions options)
    {
        var settings = new FileStreamOptions
        {
            Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None, BufferSize = 64 * 1024, Options = options
        };
        if (!OperatingSystem.IsWindows())
        {
            settings.UnixCreateMode = OwnerOnly;
        }
        var stream = new FileStream(temporary, settings);
        try
        {
            if (OperatingSystem.IsWindows())
            {
                RestrictToCurrentUser(temporary);
            }
        }
        catch
        {
            stream.Dispose();
            throw;
        }
        return stream;
    }

    /// <summary>Puts the complete temporary file in place of <paramref name="path"/>, keeping the existing file's access.</summary>
    private static void Publish(string temporary, string path)
    {
        if (!File.Exists(path))
        {
            File.Move(temporary, path);
            return;
        }
        if (OperatingSystem.IsWindows())
        {
            // ReplaceFile keeps the replaced file's security descriptor (its DACL, protection and owner).
            File.Replace(temporary, path, destinationBackupFileName: null, ignoreMetadataErrors: false);
            return;
        }
        var mode = File.GetUnixFileMode(path);
        if ((mode & GroupBits) != 0)
        {
            // The file's group matters to who can read it. The contents are complete and flushed already; copying them
            // over the file keeps it the same file (owner, group, mode), where a rename would hand it the process's group.
            using (var source = new FileStream(temporary, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var target = new FileStream(path, FileMode.Truncate, FileAccess.Write, FileShare.None))
            {
                source.CopyTo(target);
                target.Flush(flushToDisk: true);
            }
            return;
        }
        File.SetUnixFileMode(temporary, mode);
        File.Move(temporary, path, overwrite: true);
    }

    [SupportedOSPlatform("windows")]
    private static void RestrictToCurrentUser(string path)
    {
        var user = WindowsIdentity.GetCurrent().User
            ?? throw new InvalidOperationException("The current Windows user is unknown.");
        var security = new FileSecurity();
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(user, FileSystemRights.FullControl, AccessControlType.Allow));
        new FileInfo(path).SetAccessControl(security);
    }

    private static string TemporaryPathFor(string path)
    {
        var full = Path.GetFullPath(path);
        var directory = Path.GetDirectoryName(full) ?? throw new ArgumentException("The file must have a parent folder.", nameof(path));
        return Path.Combine(directory, $".{Path.GetFileName(full)}.{Guid.NewGuid():N}.tmp");
    }

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
        }
    }
}
