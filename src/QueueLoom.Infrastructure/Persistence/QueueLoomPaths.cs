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
            // A macOS bundle is replaced as a whole. Its backups must live beside the bundle, not inside it.
            // Portable installations keep backups beside the executable; unwritable defaults use the data folder.
            BackupsDirectory = !string.IsNullOrWhiteSpace(backupOverride)
                ? OutsideApplicationBundle(Path.GetFullPath(backupOverride), AppContext.BaseDirectory)
                : IsWritableDirectory(ProgramBackupsDirectory) ? ProgramBackupsDirectory : persistentPaths.BackupsDirectory
        };
    }

    /// <summary>The "backups" folder next to the executable, or beside a macOS application bundle.</summary>
    public static string ProgramBackupsDirectory => ProgramBackupsDirectoryFor(AppContext.BaseDirectory);

    internal static string ProgramBackupsDirectoryFor(string executableDirectory) =>
        OutsideApplicationBundle(Path.Combine(executableDirectory, "backups"), executableDirectory);

    public static string OutsideApplicationBundle(string directory, string executableDirectory)
    {
        var macOS = new DirectoryInfo(Path.GetFullPath(executableDirectory));
        var bundle = macOS.Parent?.Parent;
        var full = Path.GetFullPath(directory);
        if (macOS.Name != "MacOS" || macOS.Parent?.Name != "Contents" ||
            bundle is null || !bundle.Name.EndsWith(".app", StringComparison.OrdinalIgnoreCase)) return full;
        var relative = Path.GetRelativePath(bundle.FullName, full);
        if (relative == ".." || relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal) ||
            Path.IsPathRooted(relative)) return full;
        if (relative == ".") throw new InvalidOperationException("The application bundle itself cannot be used as a backup directory.");
        var root = Path.Combine(bundle.Parent!.FullName, "backups");
        return relative == Path.Combine("Contents", "MacOS", "backups")
            ? root : Path.Combine(root, "bundle-custom", relative);
    }

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
