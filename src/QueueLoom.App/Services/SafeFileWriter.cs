using System.Text;

namespace QueueLoom.App.Services;

/// <summary>
/// Replaces a file only once its new contents are complete and flushed to disk: they are written to a temporary file
/// next to it, which is then renamed over it. A failure, a cancellation or a crash part-way leaves the previous file
/// as it was instead of empty or cut short.
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
            await using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                64 * 1024, FileOptions.Asynchronous))
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
            using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
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
