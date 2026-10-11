using System.Runtime.Versioning;
using QueueLoom.App.Services;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class MacBackupMigrationPermissionTests
{
    private const UnixFileMode PrivateDirectory = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;
    private const UnixFileMode PrivateFile = UnixFileMode.UserRead | UnixFileMode.UserWrite;
    private const UnixFileMode TraversableDirectory = PrivateDirectory | UnixFileMode.GroupRead | UnixFileMode.GroupExecute |
        UnixFileMode.OtherRead | UnixFileMode.OtherExecute;

    [Fact]
    public async Task ExistingConfiguredBackupDirectoriesKeepTheirModesAcrossRepeatedMigration()
    {
        if (OperatingSystem.IsWindows())
        {
            Assert.Skip("Existing Unix directory modes require Linux or macOS.");
            return;
        }
        await VerifyExistingConfiguredDirectories();
    }

    [UnsupportedOSPlatform("windows")]
    private static async Task VerifyExistingConfiguredDirectories()
    {
        using var directory = new TemporaryDirectory();
        File.SetUnixFileMode(directory.Path, TraversableDirectory);
        var executable = Directory.CreateDirectory(Path.Combine(directory.Path, "QueueLoom.app", "Contents", "MacOS")).FullName;
        var source = Directory.CreateDirectory(Path.Combine(executable, "backups", "session")).FullName;
        File.SetUnixFileMode(source, PrivateDirectory);
        File.WriteAllText(Path.Combine(source, "orders.json"), "synthetic message");
        File.SetUnixFileMode(Path.Combine(source, "orders.json"), PrivateFile);
        var paths = QueueLoomPaths.ForRoot(Path.Combine(directory.Path, "private-data")) with
        { BackupsDirectory = Path.Combine(directory.Path, "operator-selected", "shared-backups") };
        var selected = Directory.CreateDirectory(paths.BackupsDirectory).FullName;
        var existingSession = Directory.CreateDirectory(Path.Combine(selected, "session")).FullName;
        var selectedMode = TraversableDirectory | UnixFileMode.GroupWrite;
        var parent = Path.GetDirectoryName(selected)!;
        File.SetUnixFileMode(parent, TraversableDirectory);
        File.SetUnixFileMode(selected, selectedMode);
        File.SetUnixFileMode(existingSession, TraversableDirectory);
        var migration = new LegacyBackupMigration(paths, executable);

        Assert.Equal(1, await migration.RunAsync());
        Assert.Equal(selectedMode, File.GetUnixFileMode(selected));
        Assert.Equal(TraversableDirectory, File.GetUnixFileMode(existingSession));
        Assert.Equal(TraversableDirectory, File.GetUnixFileMode(parent));
        Assert.Equal(PrivateFile, File.GetUnixFileMode(Path.Combine(existingSession, "orders.json")));
        Assert.Equal(0, await migration.RunAsync());
        Assert.Equal(selectedMode, File.GetUnixFileMode(selected));
        Assert.Equal(TraversableDirectory, File.GetUnixFileMode(existingSession));
        Assert.Equal(TraversableDirectory, File.GetUnixFileMode(parent));
    }

    [Theory]
    [InlineData("startup", false)]
    [InlineData("startup", true)]
    [InlineData("preserve", false)]
    [InlineData("preserve", true)]
    [InlineData("custom-preserve", false)]
    [InlineData("custom-preserve", true)]
    public async Task MigratedBackupsRemainOwnerOnlyOutsideThePrivateDataRoot(string caller, bool conflict)
    {
        if (OperatingSystem.IsWindows())
        {
            Assert.Skip("Unix backup permissions require Linux or macOS; Windows cannot prove these modes.");
            return;
        }
        await VerifyUnixPermissions(caller, conflict);
    }

    [UnsupportedOSPlatform("windows")]
    private static async Task VerifyUnixPermissions(string caller, bool conflict)
    {
        using var directory = new TemporaryDirectory();
        // Model a traversable installation/shared directory without altering umask or any real backup folder.
        File.SetUnixFileMode(directory.Path, TraversableDirectory);
        var bundle = Path.Combine(directory.Path, "QueueLoom.app");
        var executableDirectory = Directory.CreateDirectory(Path.Combine(bundle, "Contents", "MacOS")).FullName;
        var custom = caller == "custom-preserve";
        var source = Directory.CreateDirectory(custom
            ? Path.Combine(bundle, "Contents", "saved-messages")
            : Path.Combine(executableDirectory, "backups")).FullName;
        var sourceSession = Directory.CreateDirectory(Path.Combine(source, "session")).FullName;
        File.SetUnixFileMode(source, PrivateDirectory);
        File.SetUnixFileMode(sourceSession, PrivateDirectory);
        const string payload = "{\"body\":\"synthetic confidential message\"}";
        var original = Path.Combine(sourceSession, "orders.json");
        File.WriteAllText(original, payload);
        File.SetUnixFileMode(original, PrivateFile);

        var paths = QueueLoomPaths.ForRoot(Path.Combine(directory.Path, "private-data")) with
        { BackupsDirectory = Path.Combine(directory.Path, "configured-backups") };
        var destination = caller == "startup" ? paths.BackupsDirectory
            : QueueLoomPaths.OutsideApplicationBundle(source, executableDirectory);
        var boundary = caller == "startup" ? destination : Path.Combine(directory.Path, "backups");
        var destinationSession = Directory.CreateDirectory(Path.Combine(destination, "session")).FullName;
        // Existing external folders can already be 0755, independently of the runner's umask.
        var current = destinationSession;
        while (true)
        {
            File.SetUnixFileMode(current, TraversableDirectory);
            if (current == boundary) break;
            current = Path.GetDirectoryName(current)!;
        }
        var requested = Path.Combine(destinationSession, "orders.json");
        if (conflict)
        {
            File.WriteAllText(requested, "different saved backup");
            File.SetUnixFileMode(requested, PrivateFile);
        }

        var observedTemporary = false;
        var previousObserver = MacBackupMigration.CopyStarting.Value;
        MacBackupMigration.CopyStarting.Value = temporary =>
        {
            observedTemporary = true;
            Assert.Equal(0, new FileInfo(temporary).Length);
            Assert.Equal(PrivateFile, File.GetUnixFileMode(temporary));
            AssertPrivateDirectories(destinationSession, boundary);
        };
        try
        {
            if (caller == "startup")
                Assert.Equal(1, await new LegacyBackupMigration(paths, executableDirectory).RunAsync());
            else
            {
                var target = new UpdateTarget("osx-arm64", directory.Path, Path.Combine(executableDirectory, "QueueLoom"), bundle);
                var receipt = new UpdateRestart.Receipt(Guid.NewGuid().ToString("N"), target, "", [])
                { BundleBackupDirectories = MacBackupMigration.CaptureDirectories(target, source) };
                MacBackupMigration.Preserve(receipt, bundle);
            }
        }
        finally { MacBackupMigration.CopyStarting.Value = previousObserver; }
        Assert.True(observedTemporary);

        var copied = conflict ? Assert.Single(Directory.GetFiles(destinationSession, "orders.recovered-*.json")) : requested;
        Assert.Equal(payload, File.ReadAllText(copied));
        Assert.Equal(payload, File.ReadAllText(original));
        if (conflict) Assert.Equal("different saved backup", File.ReadAllText(requested));
        Assert.Empty(Directory.GetFiles(boundary, "*.tmp", SearchOption.AllDirectories));
        Assert.Equal(PrivateFile, File.GetUnixFileMode(copied));
        AssertPrivateDirectories(destinationSession, boundary);
        Assert.Equal(TraversableDirectory, File.GetUnixFileMode(directory.Path));
        Assert.Equal(PrivateFile, File.GetUnixFileMode(original));
    }

    [UnsupportedOSPlatform("windows")]
    private static void AssertPrivateDirectories(string destinationSession, string boundary)
    {
        var current = destinationSession;
        while (true)
        {
            Assert.Equal(PrivateDirectory, File.GetUnixFileMode(current));
            if (current == boundary) break;
            current = Path.GetDirectoryName(current)!;
        }
    }
}
