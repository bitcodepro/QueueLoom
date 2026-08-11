namespace QueueLoom.Infrastructure.Persistence;

internal sealed class CrossProcessFileLock : IAsyncDisposable
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

    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}
