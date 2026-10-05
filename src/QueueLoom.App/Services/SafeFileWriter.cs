using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;

namespace QueueLoom.App.Services;

/// <summary>
/// Replaces a file only once its new contents are complete and flushed to disk, and then in one step, so no failure
/// (while producing the contents or while putting them in place) leaves the previous file empty, cut short or missing.
/// <para>
/// The new contents are first written to a temporary file next to the target that only the current user can open, so
/// they are never readable more widely than before, even while being written. Publishing keeps the existing file's
/// access: on Windows, <see cref="File.Replace(string, string, string?, bool)"/> gives the result the replaced file's
/// security descriptor (with a backup that restores the original if the replacement fails half-way); on Unix the file
/// is renamed over with the existing mode. A rename gives the file the process's group, so when the existing file's
/// group and everyone else had different access, both get only what they had in common (see
/// <see cref="ReplacementMode"/>): access only ever narrows, and the caller is told so it can say so.
/// </para>
/// </summary>
internal static class SafeFileWriter
{
    private const UnixFileMode OwnerOnly = UnixFileMode.UserRead | UnixFileMode.UserWrite;

    /// <summary>Test seam: replaces the Windows <see cref="File.Replace(string, string, string?, bool)"/> call.</summary>
    internal static readonly AsyncLocal<Action<string, string, string>?> ReplaceOverride = new();

    /// <summary>Test seam: runs just before the temporary file is put in place, and may fail it.</summary>
    internal static readonly AsyncLocal<Action<string, string>?> BeforePublish = new();

    /// <returns>True when group access had to be narrowed (see the class summary).</returns>
    public static Task<bool> WriteTextAsync(string path, string contents, CancellationToken cancellationToken) =>
        WriteAsync(path, (stream, token) => stream.WriteAsync(new UTF8Encoding(false).GetBytes(contents), token).AsTask(),
            cancellationToken);

    public static async Task<bool> WriteAsync(string path, Func<Stream, CancellationToken, Task> write, CancellationToken cancellationToken)
    {
        var temporary = TemporaryPathFor(path);
        var keepTemporary = false;
        try
        {
            await using (var stream = CreatePrivate(temporary, FileOptions.Asynchronous))
            {
                await write(stream, cancellationToken).ConfigureAwait(false);
                stream.Flush(flushToDisk: true);
            }
            cancellationToken.ThrowIfCancellationRequested();
            return Publish(temporary, path, narrowGroup: true, ref keepTemporary);
        }
        finally
        {
            if (!keepTemporary)
            {
                TryDelete(temporary);
            }
        }
    }

    /// <summary>
    /// Synchronous variant. <paramref name="narrowGroup"/> false is for QueueLoom's own files (such as update receipts),
    /// whose group grants nobody anything that matters: their mode is kept as it is.
    /// </summary>
    /// <returns>True when group access had to be narrowed.</returns>
    public static bool WriteText(string path, string contents, bool narrowGroup = true)
    {
        var temporary = TemporaryPathFor(path);
        var keepTemporary = false;
        try
        {
            using (var stream = CreatePrivate(temporary, FileOptions.None))
            {
                stream.Write(new UTF8Encoding(false).GetBytes(contents));
                stream.Flush(flushToDisk: true);
            }
            return Publish(temporary, path, narrowGroup, ref keepTemporary);
        }
        finally
        {
            if (!keepTemporary)
            {
                TryDelete(temporary);
            }
        }
    }

    /// <summary>
    /// The mode for the replaced file, whose group becomes the process's. Who falls in the group class and who in the
    /// "others" class then changes both ways: members of the old group become "others", members of the new group stop
    /// being "others". Each class therefore gets only what both had before (group AND others), so nobody gains access;
    /// the owner's bits are kept. Nothing changes when group and others had the same access (such as 0644).
    /// </summary>
    internal static UnixFileMode ReplacementMode(UnixFileMode existing, out bool narrowed)
    {
        var group = ((int)existing >> 3) & 7;
        var others = (int)existing & 7;
        narrowed = group != others;
        if (!narrowed)
        {
            return existing;
        }
        var both = group & others;
        return (UnixFileMode)(((int)existing & ~0b111_111) | (both << 3) | both);
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

    /// <summary>Puts the complete temporary file in place of <paramref name="path"/> in one step.</summary>
    private static bool Publish(string temporary, string path, bool narrowGroup, ref bool keepTemporary)
    {
        BeforePublish.Value?.Invoke(temporary, path);
        if (!File.Exists(path))
        {
            File.Move(temporary, path);
            return false;
        }
        if (OperatingSystem.IsWindows())
        {
            ReplaceOnWindows(temporary, path, ref keepTemporary);
            return false;
        }
        var existing = File.GetUnixFileMode(path);
        var narrowed = false;
        var mode = narrowGroup ? ReplacementMode(existing, out narrowed) : existing;
        File.SetUnixFileMode(temporary, mode);
        File.Move(temporary, path, overwrite: true);
        return narrowed;
    }

    /// <summary>
    /// ReplaceFile keeps the replaced file's security descriptor. It may fail half-way: with
    /// ERROR_UNABLE_TO_MOVE_REPLACEMENT the original is already at the backup name and the replacement still at its
    /// temporary name. The original is then put back; if even that fails, the complete new file is kept, not deleted.
    /// </summary>
    private static void ReplaceOnWindows(string temporary, string path, ref bool keepTemporary)
    {
        var backup = TemporaryPathFor(path) + ".previous";
        try
        {
            if (ReplaceOverride.Value is { } replace)
            {
                replace(temporary, path, backup);
            }
            else
            {
                File.Replace(temporary, path, backup, ignoreMetadataErrors: false);
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            if (!File.Exists(path))
            {
                try
                {
                    if (File.Exists(backup))
                    {
                        File.Move(backup, path);
                    }
                }
                catch (Exception restore) when (restore is IOException or UnauthorizedAccessException)
                {
                    keepTemporary = File.Exists(temporary);
                    // No inner exception: the operator sees an exception's innermost message, and this one says where
                    // the two versions are (the causes are in it too).
                    throw new IOException(
                        $"'{path}' could not be replaced ({exception.Message}), and the previous version could not be " +
                        $"put back ({restore.Message}). It is at '{backup}'" +
                        (keepTemporary ? $"; the new version is at '{temporary}'." : "."));
                }
            }
            throw;
        }
        TryDelete(backup);
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
