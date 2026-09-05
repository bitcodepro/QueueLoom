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
        var localData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localData))
        {
            throw new InvalidOperationException("The operating system did not provide a local application-data directory.");
        }

        var rootOverride = Environment.GetEnvironmentVariable("QUEUELOOM_DATA_DIRECTORY");
        var persistentPaths = ForRoot(string.IsNullOrWhiteSpace(rootOverride) ? Path.Combine(localData, "QueueLoom") : rootOverride);
        var backupOverride = Environment.GetEnvironmentVariable("QUEUELOOM_BACKUP_DIRECTORY");
        var legacyBackups = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "backups"));
        return persistentPaths with
        {
            // Keep existing portable backups visible, without moving or deleting user data.
            BackupsDirectory = !string.IsNullOrWhiteSpace(backupOverride) ? Path.GetFullPath(backupOverride) :
                string.IsNullOrWhiteSpace(rootOverride) && Directory.Exists(legacyBackups)
                    ? legacyBackups : persistentPaths.BackupsDirectory
        };
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
