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
    public string? BackupDirectoryWarning { get; init; }
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
        var launched = QueueLoom.Core.Updates.PayloadLaunch.Current;
        if (launched is not null && !string.IsNullOrWhiteSpace(backupOverride)) backupOverride = launched.ExternalBackupPath(backupOverride);
        return ForProgram(persistentPaths, launched?.Installation.DataAnchor ?? AppContext.BaseDirectory, backupOverride);
    }

    internal static QueueLoomPaths ForProgram(QueueLoomPaths persistentPaths, string executableDirectory, string? backupOverride)
    {
        var bundle = ApplicationBundleFor(executableDirectory);
        var invalidOverride = !string.IsNullOrWhiteSpace(backupOverride) && bundle is not null &&
            Path.GetRelativePath(bundle, Path.GetFullPath(backupOverride)) == ".";
        var programBackups = ProgramBackupsDirectoryFor(executableDirectory);
        var selected = !string.IsNullOrWhiteSpace(backupOverride) && !invalidOverride
            ? OutsideApplicationBundle(Path.GetFullPath(backupOverride), executableDirectory)
            : IsWritableDirectory(programBackups) ? programBackups : persistentPaths.BackupsDirectory;
        return persistentPaths with
        {
            BackupsDirectory = selected,
            BackupDirectoryWarning = invalidOverride
                ? $"The configured backup directory is the application bundle. Using {selected} instead." : null
        };
    }

    /// <summary>The "backups" folder next to the executable, or beside a macOS application bundle.</summary>
    public static string ProgramBackupsDirectory => ProgramBackupsDirectoryFor(
        QueueLoom.Core.Updates.PayloadLaunch.Current?.Installation.DataAnchor ?? AppContext.BaseDirectory);

    internal static string ProgramBackupsDirectoryFor(string executableDirectory) =>
        OutsideApplicationBundle(Path.Combine(executableDirectory, "backups"), executableDirectory);

    public static string OutsideApplicationBundle(string directory, string executableDirectory)
    {
        var bundle = ApplicationBundleFor(executableDirectory);
        var full = Path.GetFullPath(directory);
        if (bundle is null) return full;
        var relative = Path.GetRelativePath(bundle, full);
        if (relative == ".." || relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal) ||
            Path.IsPathRooted(relative)) return full;
        var root = Path.Combine(Path.GetDirectoryName(bundle)!, "backups");
        return relative == "." || relative == Path.Combine("Contents", "MacOS", "backups")
            ? root : Path.Combine(root, "bundle-custom", relative);
    }

    public static string? ApplicationBundleFor(string executableDirectory)
    {
        var macOS = new DirectoryInfo(Path.GetFullPath(executableDirectory));
        var bundle = macOS.Parent?.Parent;
        return macOS.Name == "MacOS" && macOS.Parent?.Name == "Contents" &&
            bundle?.Name.EndsWith(".app", StringComparison.OrdinalIgnoreCase) == true ? bundle.FullName : null;
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
