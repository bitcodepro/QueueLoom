using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// The operation history is read off the window's thread, so a read of an item's files can overlap an operation that
// replaces them. On Windows the replacement (AtomicFile moves a new file over the old one) was refused while the read held
// the file open: a retry of a proven rejection then failed with "Access to the path is denied" before anything was sent.
// The replacement is now tried again for as long as such a read lasts, within a bound.
public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("detail")]
    [InlineData("state")]
    public async Task OperationFiles_ARetryReplacesItemFilesWhileTheHistoryReadsThem(string file)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows file sharing."); return; }
        using var directory = new TemporaryDirectory();
        var (store, workspace, plan) = await RejectedOperationAsync(directory);
        var path = Path.Combine(store.RootDirectory, plan.Id.ToString("N"), $"000000.{file}");

        // Held open as File.ReadAllText (the history read) opens it, and closed once the replacement was refused.
        var reader = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        var refused = 0;
        AtomicFile.ReplaceRetrying.Value = target =>
        {
            if (target == path && Interlocked.Increment(ref refused) == 1) reader.Dispose();
        };
        try
        {
            await store.RunItemsAsync(plan, [0], true, workspace, () => true, null, default);
        }
        finally
        {
            reader.Dispose();
        }

        Assert.True(refused >= 1);
        Assert.Equal(2, workspace.SentMessages.Count);
        Assert.Equal("Sent", store.ReadHistory(plan).Items[0].State);
    }

    // A file held open past the bound still fails the write, as before: nothing is sent for the retried item.
    [Fact]
    public async Task OperationFiles_AFileHeldPastTheBoundStillFailsTheRetryBeforeSending()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows file sharing."); return; }
        using var directory = new TemporaryDirectory();
        var (store, workspace, plan) = await RejectedOperationAsync(directory);
        var path = Path.Combine(store.RootDirectory, plan.Id.ToString("N"), "000000.detail");
        AtomicFile.ReplaceBudgetOverride.Value = TimeSpan.Zero;

        using (new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
                store.RunItemsAsync(plan, [0], true, workspace, () => true, null, default));
        }

        Assert.Single(workspace.SentMessages);
        Assert.Equal("Rejected", store.ReadHistory(plan).Items[0].State);
    }

    private async Task<(BatchReplayStore Store, FakeWorkspace Workspace, ReplayPlan Plan)> RejectedOperationAsync(TemporaryDirectory directory)
    {
        var store = new BatchReplayStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        var plan = await PrepareOperation(store, profile);
        workspace.OnSend = () => throw new DeliveryRejectedException("Provider explicitly rejected delivery");
        await store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
        workspace.OnSend = null;
        Assert.Equal("Rejected", store.ReadHistory(plan).Items[0].State);
        return (store, workspace, plan);
    }
}
