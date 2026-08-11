namespace QueueLoom.Infrastructure.Persistence;

public sealed record QueueLoomPaths(
    string RootDirectory,
    string ProfilesFile,
    string SecretsFile,
    string InstallationIdFile,
    string ProtectedMasterKeyFile,
    string StorageLockFile)
{
    public static QueueLoomPaths CreateDefault()
    {
        var localData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localData))
        {
            throw new InvalidOperationException("The operating system did not provide a local application-data directory.");
        }

        return ForRoot(Path.Combine(localData, "QueueLoom"));
    }

    public static QueueLoomPaths ForRoot(string rootDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        var root = Path.GetFullPath(rootDirectory);
        return new QueueLoomPaths(
            root,
            Path.Combine(root, "profiles.v1.json"),
            Path.Combine(root, "secrets.v1.json"),
            Path.Combine(root, "installation.id"),
            Path.Combine(root, "vault-key.dpapi"),
            Path.Combine(root, ".storage.lock"));
    }

    public void EnsureCreated()
    {
        Directory.CreateDirectory(RootDirectory);

        if (!OperatingSystem.IsWindows())
        {
            try
            {
                File.SetUnixFileMode(
                    RootDirectory,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            }
            catch (PlatformNotSupportedException)
            {
            }
        }
    }
}
