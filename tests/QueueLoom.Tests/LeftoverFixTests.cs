using System.Reflection;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using QueueLoom.App.Services;
using QueueLoom.Core.IO;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Mcp;
using QueueLoom.Tests.Infrastructure;
using static QueueLoom.Tests.ViewModelStateTests;

namespace QueueLoom.Tests;

public sealed class LeftoverFixTests
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    // ---- Replacing a file the user picked ------------------------------------------------------------------------

    // A write that fails part-way leaves the existing file as it was, and no temporary file behind.
    [Fact]
    public async Task SafeFileWriter_FailedWriteKeepsThePreviousFile()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");

        await Assert.ThrowsAsync<IOException>(() => SafeFileWriter.WriteAsync(path, async (stream, token) =>
        {
            await stream.WriteAsync("{ \"partial\": "u8.ToArray(), token);
            throw new IOException("disk full");
        }, CancellationToken.None));

        Assert.Equal("previous export", await File.ReadAllTextAsync(path));
        Assert.Equal([path], Directory.GetFiles(directory.Path));
    }

    // A cancelled write leaves the previous file too.
    [Fact]
    public async Task SafeFileWriter_CancelledWriteKeepsThePreviousFile()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");
        using var cancellation = new CancellationTokenSource();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => SafeFileWriter.WriteAsync(path, async (stream, token) =>
        {
            await stream.WriteAsync("{ \"partial\": "u8.ToArray(), token);
            await cancellation.CancelAsync();
        }, cancellation.Token));

        Assert.Equal("previous export", await File.ReadAllTextAsync(path));
        Assert.Equal([path], Directory.GetFiles(directory.Path));
    }

    [Fact]
    public void SafeFileWriter_ReplacesTheFileWhenComplete()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "receipt.json");
        File.WriteAllText(path, "old receipt");

        SafeFileWriter.WriteText(path, "{\"Recovered\":true}");

        Assert.Equal("{\"Recovered\":true}", File.ReadAllText(path));
        Assert.Equal([path], Directory.GetFiles(directory.Path));
    }

    // A deliberately restricted file (0600) stays 0600 after being replaced, under the usual umask 022, with both
    // writers; the new content is written meanwhile into a file no more readable than that.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SafeFileWriter_KeepsTheExistingFileMode(bool synchronous)
    {
        if (OperatingSystem.IsWindows()) return; // Windows keeps the access rules instead; covered by the code path only
        await KeepsTheExistingFileModeOnUnix(synchronous);
    }

    [System.Runtime.Versioning.UnsupportedOSPlatform("windows")]
    private static async Task KeepsTheExistingFileModeOnUnix(bool synchronous)
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");
        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        UnixFileMode? seenWhileWriting = null;

        if (synchronous) SafeFileWriter.WriteText(path, "new export");
        else await SafeFileWriter.WriteAsync(path, async (stream, token) =>
        {
            seenWhileWriting = File.GetUnixFileMode(((FileStream)stream).Name);
            await stream.WriteAsync("new export"u8.ToArray(), token);
        }, CancellationToken.None);

        Assert.Equal("new export", await File.ReadAllTextAsync(path));
        Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(path));
        if (!synchronous) Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, seenWhileWriting);
    }

    // A file whose group has more access than everyone else (0640): the replacement is atomic, but that extra group
    // access is not carried over (a rename hands the file the process's group), and the caller is told; QueueLoom's own
    // files (narrowGroup: false) keep their mode as it is.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SafeFileWriter_NarrowsGroupAccessItCannotKeep(bool synchronous)
    {
        if (OperatingSystem.IsWindows()) return;
        await NarrowsGroupAccessOnUnix(synchronous);
    }

    [System.Runtime.Versioning.UnsupportedOSPlatform("windows")]
    private static async Task NarrowsGroupAccessOnUnix(bool synchronous)
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");
        var groupReadable = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead;
        File.SetUnixFileMode(path, groupReadable);

        var narrowed = synchronous
            ? SafeFileWriter.WriteText(path, "new export")
            : await SafeFileWriter.WriteTextAsync(path, "new export", CancellationToken.None);

        Assert.True(narrowed);
        Assert.Equal("new export", await File.ReadAllTextAsync(path));
        Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(path));
        Assert.Equal([path], Directory.GetFiles(directory.Path));

        File.SetUnixFileMode(path, groupReadable);
        Assert.False(SafeFileWriter.WriteText(path, "receipt", narrowGroup: false));
        Assert.Equal(groupReadable, File.GetUnixFileMode(path));
    }

    [Theory]
    [InlineData(0b110_100_000, 0b110_000_000, true)]  // 0640 -> 0600
    [InlineData(0b110_110_100, 0b110_100_100, true)]  // 0664 -> 0644
    [InlineData(0b110_010_100, 0b110_000_000, true)]  // 0624 -> 0600: the old group could not read, so "others" may not
    [InlineData(0b110_000_100, 0b110_000_000, true)]  // 0604 -> 0600: the old group was denied what others had
    [InlineData(0b110_100_100, 0b110_100_100, false)] // 0644 stays
    [InlineData(0b110_000_000, 0b110_000_000, false)] // 0600 stays
    [InlineData(0b111_101_101, 0b111_101_101, false)] // 0755 stays
    public void SafeFileWriter_ReplacementModeOnlyNarrows(int existing, int expected, bool narrowed)
    {
        Assert.Equal((UnixFileMode)expected, SafeFileWriter.ReplacementMode((UnixFileMode)existing, out var wasNarrowed));
        Assert.Equal(narrowed, wasNarrowed);
    }

    // Effective access, class by class (Linux: owner, else group, else others), for every mode and for readers in the
    // old group, the new group, both or neither: after the group changes, nobody may gain any permission.
    [Fact]
    public void SafeFileWriter_ReplacementModeNeverGrantsAnyoneMoreAfterTheGroupChanges()
    {
        for (var bits = 0; bits < 0b1_000_000_000; bits++)
        {
            var before = (UnixFileMode)bits;
            var after = SafeFileWriter.ReplacementMode(before, out _);
            foreach (var (inOld, inNew) in new[] { (false, false), (true, false), (false, true), (true, true) })
            {
                var had = inOld ? ((int)before >> 3) & 7 : (int)before & 7;
                var has = inNew ? ((int)after >> 3) & 7 : (int)after & 7;
                Assert.True((has & ~had) == 0, $"mode {bits:B9}, old group member {inOld}, new group member {inNew}");
            }
            Assert.Equal((int)before & 0b111_000_000, (int)after & 0b111_000_000);
        }
    }

    // An ordinary 0644 file (group access no wider than everyone's) is replaced atomically and keeps its mode.
    [Fact]
    public async Task SafeFileWriter_ReplacesAnOrdinarilyReadableFile()
    {
        if (OperatingSystem.IsWindows()) return;
        await ReplacesAnOrdinaryFileOnUnix();
    }

    [System.Runtime.Versioning.UnsupportedOSPlatform("windows")]
    private static async Task ReplacesAnOrdinaryFileOnUnix()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");
        var ordinary = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead | UnixFileMode.OtherRead;
        File.SetUnixFileMode(path, ordinary);

        await SafeFileWriter.WriteTextAsync(path, "new export", CancellationToken.None);

        Assert.Equal("new export", await File.ReadAllTextAsync(path));
        Assert.Equal(ordinary, File.GetUnixFileMode(path));
    }

    // The contents were staged completely, but putting them in place fails: the previous file is intact (both writers).
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SafeFileWriter_AFailedPublicationKeepsThePreviousFile(bool synchronous)
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");
        SafeFileWriter.BeforePublish.Value = (_, _) => throw new IOException("the disk went away");
        try
        {
            if (synchronous) Assert.Throws<IOException>(() => SafeFileWriter.WriteText(path, "new export"));
            else await Assert.ThrowsAsync<IOException>(() => SafeFileWriter.WriteTextAsync(path, "new export", CancellationToken.None));
        }
        finally
        {
            SafeFileWriter.BeforePublish.Value = null;
        }

        Assert.Equal("previous export", await File.ReadAllTextAsync(path));
        Assert.Equal([path], Directory.GetFiles(directory.Path));
    }

    // Windows ReplaceFile failing with ERROR_UNABLE_TO_MOVE_REPLACEMENT (1176): the original has been moved to the backup
    // name and the replacement is still at its temporary name. The original is put back.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SafeFileWriter_AHalfDoneWindowsReplacementPutsTheOriginalBack(bool synchronous)
    {
        if (!OperatingSystem.IsWindows()) return;
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");
        SafeFileWriter.ReplaceOverride.Value = (_, destination, backup) =>
        {
            File.Move(destination, backup);
            throw new IOException("Unable to move the replacement file to the file to be replaced.", unchecked((int)0x80070498));
        };
        try
        {
            if (synchronous) Assert.Throws<IOException>(() => SafeFileWriter.WriteText(path, "new export"));
            else await Assert.ThrowsAsync<IOException>(() => SafeFileWriter.WriteTextAsync(path, "new export", CancellationToken.None));
        }
        finally
        {
            SafeFileWriter.ReplaceOverride.Value = null;
        }

        Assert.Equal("previous export", await File.ReadAllTextAsync(path));
        Assert.Equal([path], Directory.GetFiles(directory.Path));
    }

    // If even putting the original back fails, the complete new version is kept (not deleted) and both places are named.
    [Fact]
    public async Task SafeFileWriter_KeepsTheNewVersionWhenTheOriginalCannotBePutBack()
    {
        if (!OperatingSystem.IsWindows()) return;
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");
        SafeFileWriter.ReplaceOverride.Value = (_, destination, backup) =>
        {
            File.Move(destination, backup);
            Directory.CreateDirectory(destination); // blocks moving the backup back
            throw new IOException("Unable to move the replacement file to the file to be replaced.", unchecked((int)0x80070498));
        };
        IOException error;
        try
        {
            error = await Assert.ThrowsAsync<IOException>(() => SafeFileWriter.WriteTextAsync(path, "new export", CancellationToken.None));
        }
        finally
        {
            SafeFileWriter.ReplaceOverride.Value = null;
        }

        var files = Directory.GetFiles(directory.Path).Select(File.ReadAllText).ToArray();
        Assert.Contains("previous export", files);
        Assert.Contains("new export", files);
        Assert.Contains("nor put back", error.Message, StringComparison.Ordinal);
    }

    // Windows: an export restricted to the current user (protected DACL) stays so after being replaced, although its
    // folder lets everyone read; the temporary file holding the new content is restricted before anything is written.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SafeFileWriter_KeepsARestrictedWindowsFileRestricted(bool synchronous)
    {
        if (!OperatingSystem.IsWindows()) return;
        await KeepsARestrictedFileOnWindows(synchronous);
    }

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static async Task KeepsARestrictedFileOnWindows(bool synchronous)
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
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");
        var user = System.Security.Principal.WindowsIdentity.GetCurrent().User!;
        var restricted = new System.Security.AccessControl.FileSecurity();
        restricted.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        restricted.AddAccessRule(new System.Security.AccessControl.FileSystemAccessRule(user,
            System.Security.AccessControl.FileSystemRights.FullControl, System.Security.AccessControl.AccessControlType.Allow));
        new FileInfo(path).SetAccessControl(restricted);
        System.Security.AccessControl.FileSecurity? seenWhileWriting = null;

        if (synchronous) SafeFileWriter.WriteText(path, "new export");
        else await SafeFileWriter.WriteAsync(path, async (stream, token) =>
        {
            seenWhileWriting = new FileInfo(((FileStream)stream).Name).GetAccessControl();
            await stream.WriteAsync("new export"u8.ToArray(), token);
        }, CancellationToken.None);

        Assert.Equal("new export", await File.ReadAllTextAsync(path));
        AssertOnlyCurrentUser(new FileInfo(path).GetAccessControl(), user);
        if (!synchronous) AssertOnlyCurrentUser(seenWhileWriting!, user);
    }

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static void AssertOnlyCurrentUser(System.Security.AccessControl.FileSecurity security, System.Security.Principal.SecurityIdentifier user)
    {
        Assert.True(security.AreAccessRulesProtected);
        var rules = security.GetAccessRules(true, true, typeof(System.Security.Principal.SecurityIdentifier))
            .Cast<System.Security.AccessControl.FileSystemAccessRule>().ToArray();
        Assert.NotEmpty(rules);
        Assert.All(rules, rule => Assert.Equal(user, rule.IdentityReference));
    }

    // ---- Pub/Sub: holding messages just pulled ---------------------------------------------------------------------

    // A transient failure to extend the hold is retried: the pulled batch is returned instead of dropped (dropping it
    // would bring the messages back only after the ack deadline, to be pulled and counted again).
    [Fact]
    public async Task PubSub_TransientHoldFailureIsRetried()
    {
        var subscriber = new HoldSubscriber { HoldFailures = 1 };
        var channel = PubSubChannel(subscriber, out _);

        var received = await channel.ReceiveAsync(10, CancellationToken.None);

        Assert.Equal(2, received.Count);
        Assert.Equal([GooglePubSubWorkspace.HoldSeconds, GooglePubSubWorkspace.HoldSeconds], subscriber.Deadlines);
    }

    // A hold that cannot be set releases the batch at once (deadline 0) and says so, rather than leaving the messages
    // invisible until the subscription's ack deadline passes.
    [Fact]
    public async Task PubSub_PersistentHoldFailureReleasesTheBatchAndExplains()
    {
        var subscriber = new HoldSubscriber { HoldFailures = int.MaxValue };
        var channel = PubSubChannel(subscriber, out _);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => channel.ReceiveAsync(10, CancellationToken.None));

        Assert.Contains("returned to the subscription", error.Message, StringComparison.Ordinal);
        Assert.Equal(0, subscriber.Deadlines[^1]);
        Assert.Equal(["a1", "a2"], subscriber.ReleasedAckIds);
    }

    // A definite refusal (e.g. permission denied) is not retried.
    [Fact]
    public async Task PubSub_DefiniteHoldRefusalIsNotRetried()
    {
        var subscriber = new HoldSubscriber { HoldFailures = int.MaxValue, HoldStatus = StatusCode.PermissionDenied };
        var channel = PubSubChannel(subscriber, out _);

        await Assert.ThrowsAsync<InvalidOperationException>(() => channel.ReceiveAsync(10, CancellationToken.None));

        Assert.Equal([GooglePubSubWorkspace.HoldSeconds, 0], subscriber.Deadlines);
    }

    // Cancelled while waiting to retry the hold (after the first or the second transient failure): the cancellation
    // propagates and both pulled messages are released exactly once.
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task PubSub_CancellationDuringHoldBackoffReleasesTheBatch(int failuresBeforeCancel)
    {
        using var cancellation = new CancellationTokenSource();
        var subscriber = new HoldSubscriber { HoldFailures = int.MaxValue };
        subscriber.OnHoldFailure = count => { if (count == failuresBeforeCancel) cancellation.CancelAfter(TimeSpan.FromMilliseconds(50)); }; // lands inside the 200 ms+ back-off
        var channel = PubSubChannel(subscriber, out _);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => channel.ReceiveAsync(10, cancellation.Token));

        Assert.Equal(["a1", "a2"], subscriber.ReleasedAckIds);
        Assert.Equal(1, subscriber.Deadlines.Count(deadline => deadline == 0));
        Assert.Equal(failuresBeforeCancel, subscriber.Deadlines.Count(deadline => deadline == GooglePubSubWorkspace.HoldSeconds));
    }

    private static ILeasedMessageChannel PubSubChannel(HoldSubscriber subscriber, out GooglePubSubWorkspace workspace)
    {
        workspace = new GooglePubSubWorkspace(new EmptyVault());
        typeof(GooglePubSubWorkspace).GetField("_subscriber", Any)!.SetValue(workspace, subscriber);
        var type = typeof(GooglePubSubWorkspace).GetNestedType("PubSubChannel", BindingFlags.NonPublic)!;
        return (ILeasedMessageChannel)Activator.CreateInstance(type, Any, null,
            [workspace, ServiceBusEntityReference.Subscription("t", "s"), ServiceBusSubQueue.Active, new SubscriptionName("p", "s"), null], null)!;
    }

    private sealed class HoldSubscriber : SubscriberServiceApiClient
    {
        public int HoldFailures { get; set; }
        public StatusCode HoldStatus { get; init; } = StatusCode.Unavailable;
        public List<int> Deadlines { get; } = [];
        public List<string> ReleasedAckIds { get; } = [];
        public Action<int>? OnHoldFailure { get; set; }
        private int _holdFailures;

        public override Task<PullResponse> PullAsync(PullRequest request, CallSettings? callSettings = null) =>
            Task.FromResult(new PullResponse
            {
                ReceivedMessages =
                {
                    new ReceivedMessage { AckId = "a1", Message = new PubsubMessage { MessageId = "m1" } },
                    new ReceivedMessage { AckId = "a2", Message = new PubsubMessage { MessageId = "m2" } }
                }
            });

        public override Task ModifyAckDeadlineAsync(ModifyAckDeadlineRequest request, CallSettings? callSettings = null)
        {
            Deadlines.Add(request.AckDeadlineSeconds);
            if (request.AckDeadlineSeconds == 0)
            {
                ReleasedAckIds.AddRange(request.AckIds);
                return Task.CompletedTask;
            }
            if (HoldFailures-- > 0)
            {
                OnHoldFailure?.Invoke(++_holdFailures);
                return Task.FromException(new RpcException(new Status(HoldStatus, "hold failed")));
            }
            return Task.CompletedTask;
        }
    }

    // ---- MCP: returning the connection to read-only -----------------------------------------------------------------

    // Both the revert to read-only and the fallback disconnect fail: the write's own result still reaches the client,
    // and the next tool call does not reuse the connection that may still carry write access.
    [Fact]
    public async Task Mcp_FailedRevertAndDisconnectKeepTheResultAndForceAReadOnlyReconnect()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        var repository = new FakeProfileRepository([profile], profile.Id);
        var workspace = new FakeWorkspace();
        using var session = new McpWorkspaceSession(repository, workspace, NullLogger<McpWorkspaceSession>.Instance);
        await session.ReadAsync(profile, (_, _) => Task.FromResult(0), default);
        workspace.FailReadOnlyRevert = true;
        workspace.FailDisconnect = true;

        var result = await session.WriteAsync(profile, (_, _) => Task.FromResult(42), default);

        Assert.Equal(42, result);
        Assert.Equal(ProfileAccessMode.ReadWrite, workspace.ConnectedAccessMode);
        workspace.FailReadOnlyRevert = false;
        workspace.FailDisconnect = false;
        var connects = workspace.ConnectCalls;
        await session.ReadAsync(profile, (_, _) => Task.FromResult(0), default);
        Assert.Equal(connects + 1, workspace.ConnectCalls);
        Assert.Equal(ProfileAccessMode.ReadOnly, workspace.ConnectedAccessMode);
    }

    // The write's own failure is what the client sees, not the cleanup's.
    [Fact]
    public async Task Mcp_FailedRevertDoesNotReplaceTheWriteError()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        var repository = new FakeProfileRepository([profile], profile.Id);
        var workspace = new FakeWorkspace { FailReadOnlyRevert = true, FailDisconnect = true };
        using var session = new McpWorkspaceSession(repository, workspace, NullLogger<McpWorkspaceSession>.Instance);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => session.WriteAsync<int>(profile,
            (_, _) => throw new InvalidOperationException("the broker refused the send"), default));

        Assert.Equal("the broker refused the send", error.Message);
    }

    private sealed class EmptyVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string value, CancellationToken token = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
    }
}
