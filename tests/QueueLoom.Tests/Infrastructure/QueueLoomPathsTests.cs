using System.Security.AccessControl;
using System.Security.Principal;
using System.Runtime.Versioning;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests.Infrastructure;

public sealed class QueueLoomPathsTests
{
    [Fact]
    public void CreateDefault_UsesPersistentStorageUnlessLegacyBackupsExist()
    {
        var paths = QueueLoomPaths.CreateDefault();

        var legacy = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "backups"));
        Assert.Equal(Directory.Exists(legacy) ? legacy : Path.Combine(paths.RootDirectory, "backups"), paths.BackupsDirectory);
        Assert.NotEqual(paths.RootDirectory, paths.BackupsDirectory);
    }

    [Fact]
    public async Task LocalStorage_UsesAProtectedWindowsAcl()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        using var temporaryDirectory = new TemporaryDirectory();
        var root = Path.Combine(temporaryDirectory.Path, "private");
        var paths = QueueLoomPaths.ForRoot(root);

        paths.EnsureCreated();
        await AtomicFile.WriteTextAsync(paths.SettingsFile, "{}", CancellationToken.None);

        var currentUser = WindowsIdentity.GetCurrent().User;
        Assert.NotNull(currentUser);

        var directorySecurity = new DirectoryInfo(root).GetAccessControl(AccessControlSections.Access);
        Assert.True(directorySecurity.AreAccessRulesProtected);
        AssertCurrentUserHasFullControl(directorySecurity, currentUser);

        var fileSecurity = new FileInfo(paths.SettingsFile).GetAccessControl(AccessControlSections.Access);
        Assert.True(fileSecurity.AreAccessRulesProtected);
        AssertCurrentUserHasFullControl(fileSecurity, currentUser);
    }

    [SupportedOSPlatform("windows")]
    private static void AssertCurrentUserHasFullControl(
        FileSystemSecurity security,
        SecurityIdentifier currentUser)
    {
        var rules = security.GetAccessRules(
                includeExplicit: true,
                includeInherited: false,
                typeof(SecurityIdentifier))
            .Cast<FileSystemAccessRule>();

        Assert.Contains(rules, rule =>
            rule.AccessControlType == AccessControlType.Allow &&
            currentUser.Equals(rule.IdentityReference) &&
            (rule.FileSystemRights & FileSystemRights.FullControl) == FileSystemRights.FullControl);
    }
}
