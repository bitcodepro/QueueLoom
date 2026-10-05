using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Text;

namespace QueueLoom.App.Services;

/// <summary>
/// Replaces a file only once its new contents are complete and flushed to disk: they are written to a temporary file
/// next to it, which is then renamed over it. A failure, a cancellation or a crash part-way leaves the previous file
/// as it was instead of empty or cut short. The temporary file is created with the existing file's access (Unix mode,
/// or the Windows access rules), so a deliberately restricted file stays restricted and its new contents are never
/// readable more widely, even while being written.
/// </summary>
internal static class SafeFileWriter
{
    public static Task WriteTextAsync(string path, string contents, CancellationToken cancellationToken) =>
        WriteAsync(path, (stream, token) => stream.WriteAsync(new UTF8Encoding(false).GetBytes(contents), token).AsTask(),
            cancellationToken);

    public static async Task WriteAsync(string path, Func<Stream, CancellationToken, Task> write, CancellationToken cancellationToken)
    {
        var temporary = TemporaryPathFor(path);
        try
        {
            await using (var stream = Create(path, temporary, FileOptions.Asynchronous))
            {
                await write(stream, cancellationToken).ConfigureAwait(false);
                stream.Flush(flushToDisk: true);
            }
            cancellationToken.ThrowIfCancellationRequested();
            File.Move(temporary, path, overwrite: true);
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
            using (var stream = Create(path, temporary, FileOptions.None))
            {
                stream.Write(new UTF8Encoding(false).GetBytes(contents));
                stream.Flush(flushToDisk: true);
            }
            File.Move(temporary, path, overwrite: true);
        }
        finally
        {
            TryDelete(temporary);
        }
    }

    private static FileStream Create(string path, string temporary, FileOptions options)
    {
        var settings = new FileStreamOptions
        {
            Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None, BufferSize = 64 * 1024, Options = options
        };
        if (!OperatingSystem.IsWindows() && File.Exists(path))
        {
            // Set at creation (subject to umask, which only narrows it), so the content is never more widely readable.
            settings.UnixCreateMode = File.GetUnixFileMode(path);
        }
        var stream = new FileStream(temporary, settings);
        try
        {
            if (OperatingSystem.IsWindows() && File.Exists(path))
            {
                CopyAccessRules(path, temporary);
            }
            else if (!OperatingSystem.IsWindows() && settings.UnixCreateMode is { } mode)
            {
                // umask may have narrowed the mode further; the replaced file keeps exactly the mode it had.
                File.SetUnixFileMode(temporary, mode);
            }
        }
        catch
        {
            stream.Dispose();
            throw;
        }
        return stream;
    }

    [SupportedOSPlatform("windows")]
    private static void CopyAccessRules(string from, string to)
    {
        var security = new FileInfo(from).GetAccessControl(AccessControlSections.Access);
        new FileInfo(to).SetAccessControl(security);
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
