using System.Security.AccessControl;
using System.Security.Principal;
using System.Runtime.Versioning;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests.Infrastructure;

public sealed class QueueLoomPathsTests
{
    [Fact]
    public void CreateDefault_KeepsBackupsNextToTheProgram()
    {
        var paths = QueueLoomPaths.CreateDefault();

        Assert.Equal(Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "backups")), paths.BackupsDirectory);
        Assert.Equal(QueueLoomPaths.ProgramBackupsDirectory, paths.BackupsDirectory);
        Assert.True(Directory.Exists(paths.BackupsDirectory));
    }

    [Fact]
    public void A_folder_that_cannot_be_written_is_not_used_for_backups()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var file = Path.Combine(temporaryDirectory.Path, "not-a-folder");
        File.WriteAllText(file, "x");

        Assert.True(QueueLoomPaths.IsWritableDirectory(Path.Combine(temporaryDirectory.Path, "backups")));
        Assert.False(QueueLoomPaths.IsWritableDirectory(Path.Combine(file, "backups")));
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
