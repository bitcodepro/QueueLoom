using System.Collections;
using System.Runtime.Versioning;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

/// <summary>
/// Bug 4: exporting messages must not widen who can read them. Over a 0600 export, the new one stays 0600, and the
/// temporary file holding the bodies while they are serialized is owner-only too; a new export is private.
/// </summary>
public sealed class MessageExportAccessTests
{
    private static readonly ExportedMessage Message = new("isolated", new BrowsedMessage(
        ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.Active, 1, "secret body"u8.ToArray(),
        new EditableMessageProperties(MessageId: "id")));

    [Theory]
    [InlineData("json")]
    [InlineData("csv")]
    public async Task AnExportOverAPrivateFileStaysPrivateWhileAndAfterWriting(string extension)
    {
        if (OperatingSystem.IsWindows()) return;
        await OverPrivateFileOnUnix(extension);
    }

    [UnsupportedOSPlatform("windows")]
    private static async Task OverPrivateFileOnUnix(string extension)
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "messages." + extension);
        await File.WriteAllTextAsync(path, "previous");
        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        var rows = new InspectingRows(directory.Path, path);

        await MessageExport.WriteAsync(path, rows);

        Assert.Contains("secret body", await File.ReadAllTextAsync(path), StringComparison.Ordinal);
        Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(path));
        Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, rows.TemporaryModeWhileWriting);
    }

    [Fact]
    public async Task ANewExportIsPrivate()
    {
        if (OperatingSystem.IsWindows()) return;
        await NewExportOnUnix();
    }

    [UnsupportedOSPlatform("windows")]
    private static async Task NewExportOnUnix()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "messages.json");

        await MessageExport.WriteAsync(path, [Message]);

        Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(path));
    }

    // Windows: the folder lets everyone read, the export is restricted to the current user; it stays restricted.
    [Theory]
    [InlineData("json")]
    [InlineData("csv")]
    public async Task AnExportOverARestrictedWindowsFileStaysRestricted(string extension)
    {
        if (!OperatingSystem.IsWindows()) return;
        await OverRestrictedFileOnWindows(extension);
    }

    [SupportedOSPlatform("windows")]
    private static async Task OverRestrictedFileOnWindows(string extension)
    {
        using var directory = new TemporaryDirectory();
        var folder = new DirectoryInfo(directory.Path);
        var folderSecurity = folder.GetAccessControl();
        folderSecurity.AddAccessRule(new System.Security.AccessControl.FileSystemAccessRule(
            new System.Security.Principal.SecurityIdentifier(System.Security.Principal.WellKnownSidType.WorldSid, null),
            System.Security.AccessControl.FileSystemRights.Read,
            System.Security.AccessControl.InheritanceFlags.ObjectInherit | System.Security.AccessControl.InheritanceFlags.ContainerInherit,
            System.Security.AccessControl.PropagationFlags.None, System.Security.AccessControl.AccessControlType.Allow));
        folder.SetAccessControl(folderSecurity);
        var path = Path.Combine(directory.Path, "messages." + extension);
        await File.WriteAllTextAsync(path, "previous");
        var user = System.Security.Principal.WindowsIdentity.GetCurrent().User!;
        var restricted = new System.Security.AccessControl.FileSecurity();
        restricted.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        restricted.AddAccessRule(new System.Security.AccessControl.FileSystemAccessRule(user,
            System.Security.AccessControl.FileSystemRights.FullControl, System.Security.AccessControl.AccessControlType.Allow));
        new FileInfo(path).SetAccessControl(restricted);

        await MessageExport.WriteAsync(path, [Message]);

        var security = new FileInfo(path).GetAccessControl();
        Assert.True(security.AreAccessRulesProtected);
        Assert.All(security.GetAccessRules(true, true, typeof(System.Security.Principal.SecurityIdentifier))
                .Cast<System.Security.AccessControl.FileSystemAccessRule>(),
            rule => Assert.Equal(user, rule.IdentityReference));
    }

    /// <summary>Rows whose enumeration looks at the temporary file the export is writing into.</summary>
    private sealed class InspectingRows(string directory, string destination) : IReadOnlyList<ExportedMessage>
    {
        public UnixFileMode? TemporaryModeWhileWriting { get; private set; }
        public int Count => 1;
        public ExportedMessage this[int index] => Message;

        public IEnumerator<ExportedMessage> GetEnumerator()
        {
            if (!OperatingSystem.IsWindows())
            {
                var temporary = Directory.GetFiles(directory).Single(file => file != destination);
                TemporaryModeWhileWriting = File.GetUnixFileMode(temporary);
            }
            yield return Message;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
