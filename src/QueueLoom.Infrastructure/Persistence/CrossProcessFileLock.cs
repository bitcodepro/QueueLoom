namespace QueueLoom.Infrastructure.Persistence;

internal sealed class CrossProcessFileLock : IAsyncDisposable, IDisposable
{
    private readonly FileStream _stream;

    private CrossProcessFileLock(FileStream stream)
    {
        _stream = stream;
    }

    public static async ValueTask<CrossProcessFileLock> AcquireAsync(
        string path,
        CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(path)
            ?? throw new ArgumentException("The lock file must have a parent directory.", nameof(path));
        Directory.CreateDirectory(directory);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var stream = new FileStream(
                    path,
                    FileMode.OpenOrCreate,
                    FileAccess.ReadWrite,
                    FileShare.None,
                    bufferSize: 1,
                    FileOptions.Asynchronous | FileOptions.WriteThrough);
                AtomicFile.RestrictToCurrentUser(path);
                return new CrossProcessFileLock(stream);
            }
            catch (IOException) when (DateTimeOffset.UtcNow < deadline)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (IOException exception)
            {
                throw new IOException(
                    "QueueLoom local storage is busy in another process or cannot be locked.",
                    exception);
            }
        }
    }

    /// <summary>Takes the lock only if nobody holds it right now; null when another window or process owns it.
    /// For background housekeeping that must never wait for, or block, real work.</summary>
    public static CrossProcessFileLock? TryAcquire(string path)
    {
        FileStream stream;
        try
        {
            stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, bufferSize: 1,
                FileOptions.WriteThrough);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            return null;
        }
        try
        {
            AtomicFile.RestrictToCurrentUser(path);
            return new CrossProcessFileLock(stream);
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    public ValueTask DisposeAsync() => _stream.DisposeAsync();

    public void Dispose() => _stream.Dispose();
}
