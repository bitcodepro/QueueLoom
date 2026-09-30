namespace QueueLoom.Infrastructure.Persistence;

public sealed record QueueLoomPaths(
    string RootDirectory,
    string BackupsDirectory,
    string ProfilesFile,
    string SecretsFile,
    string InstallationIdFile,
    string ProtectedMasterKeyFile,
    string SettingsFile,
    string StorageLockFile)
{
    public static QueueLoomPaths CreateDefault()
    {
        // Create: a new Linux or macOS account may not have ~/.local/share yet, and without this option .NET
        // returns an empty path for a folder that does not exist.
        var localData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.Create);
        if (string.IsNullOrWhiteSpace(localData))
        {
            throw new InvalidOperationException("The operating system did not provide a local application-data directory.");
        }

        var rootOverride = Environment.GetEnvironmentVariable("QUEUELOOM_DATA_DIRECTORY");
        var persistentPaths = ForRoot(string.IsNullOrWhiteSpace(rootOverride) ? Path.Combine(localData, "QueueLoom") : rootOverride);
        var backupOverride = Environment.GetEnvironmentVariable("QUEUELOOM_BACKUP_DIRECTORY");
        return persistentPaths with
        {
            // Backups live in a "backups" folder next to the program on every OS, so they are easy to find
            // and travel with a portable copy. Where that folder cannot be written (for example Program Files),
            // they fall back to the data folder.
            BackupsDirectory = !string.IsNullOrWhiteSpace(backupOverride)
                ? Path.GetFullPath(backupOverride)
                : IsWritableDirectory(ProgramBackupsDirectory) ? ProgramBackupsDirectory : persistentPaths.BackupsDirectory
        };
    }

    /// <summary>The "backups" folder next to the running program (the directory of the executable).</summary>
    public static string ProgramBackupsDirectory => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "backups"));

    internal static bool IsWritableDirectory(string directory)
    {
        try
        {
            Directory.CreateDirectory(directory);
            var probe = Path.Combine(directory, $".write-test-{Guid.NewGuid():N}");
            using (File.Create(probe, 1, FileOptions.DeleteOnClose))
            {
            }
            return true;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or NotSupportedException)
        {
            return false;
        }
    }

    public static QueueLoomPaths ForRoot(string rootDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        var root = Path.GetFullPath(rootDirectory);
        return new QueueLoomPaths(
            root,
            Path.Combine(root, "backups"),
            Path.Combine(root, "profiles.v1.json"),
            Path.Combine(root, "secrets.v1.json"),
            Path.Combine(root, "installation.id"),
            Path.Combine(root, "vault-key.dpapi"),
            Path.Combine(root, "settings.v1.json"),
            Path.Combine(root, ".storage.lock"));
    }

    public void EnsureCreated()
    {
        Directory.CreateDirectory(RootDirectory);
        AtomicFile.RestrictDirectoryToCurrentUser(RootDirectory);
    }
}
