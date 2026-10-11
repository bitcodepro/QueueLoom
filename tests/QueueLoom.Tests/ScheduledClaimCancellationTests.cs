using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ScheduledResendCancellationAtClaimBoundaryPreservesUnsentRecovery(bool afterClaim)
    {
        using var directory = new TemporaryDirectory();
        using var cancellation = new CancellationTokenSource();
        var profile = CreateProfile("Synthetic scheduled recovery", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var schedules = new JsonScheduledResendStore(paths);
        var job = DueJob(profile);
        schedules.Save([job]);
        var historyRoot = Path.Combine(directory.Path, "operations");
        var history = new BatchReplayStore(historyRoot);
        var observed = new ClaimBoundaryCancellationStore(schedules, cancellation, afterClaim);
        var workspace = new FakeWorkspace();
        await using (var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace,
            replayStore: history, scheduledResends: observed))
        {
            await vm.InitializeAsync();
            await vm.ConnectCommand.ExecuteAsync();
            await vm.RunDueScheduledResendsAsync(cancellation.Token);
            await vm.OperationHistoryRefresh;
            Assert.Empty(workspace.SentMessages);
        }
        var reopened = new BatchReplayStore(historyRoot);
        var plan = Assert.Single(reopened.List());
        var item = Assert.Single(reopened.ReadHistory(plan).Items);
        Assert.Equal(job.Items[0].Message.Properties.MessageId, item.MessageId);
        if (!afterClaim)
        {
            Assert.Equal(job.Id, Assert.Single(schedules.Load()).Id);
            Assert.Equal("AwaitingScheduleClaim", item.State);
            Assert.False(item.CanContinue);
            return;
        }
        Assert.Empty(schedules.Load());
        Assert.Equal("Pending", item.State);
        Assert.True(item.CanContinue);
        // Reopening history never sends automatically. A deliberate continuation
        // uses the original saved destination and stable message ID exactly once.
        var recoveredWorkspace = new FakeWorkspace();
        await recoveredWorkspace.ConnectAsync(profile);
        var result = await reopened.RunItemsAsync(plan, [0], false, recoveredWorkspace, () => true, null, default);
        Assert.Equal(1, result.SentCount);
        var sent = Assert.Single(recoveredWorkspace.SentMessages);
        Assert.Equal(job.Items[0].Message.Properties.MessageId, sent.Message.Properties.MessageId);
        Assert.Equal(job.Items[0].Destination, sent.Destination);
        Assert.Equal("Sent", Assert.Single(reopened.ReadHistory(plan).Items).State);
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.RunItemsAsync(plan, [0], false,
            recoveredWorkspace, () => true, null, default));
        Assert.Single(recoveredWorkspace.SentMessages);
    }

    private sealed class ClaimBoundaryCancellationStore(JsonScheduledResendStore inner,
        CancellationTokenSource cancellation, bool afterClaim) : IScheduledResendStore
    {
        public IReadOnlyList<ScheduledResend> Load() => inner.Load();
        public void Save(IReadOnlyList<ScheduledResend> jobs) => inner.Save(jobs);
        public void Add(ScheduledResend job) => inner.Add(job);
        public bool TryRemove(ScheduledResend expected) => inner.TryRemove(expected);
        public string? TakeSetAsideFile() => inner.TakeSetAsideFile();
        public Task<IReadOnlyList<ScheduledResend>> LoadAsync(CancellationToken token = default) => inner.LoadAsync(token);
        public Task AddAsync(ScheduledResend job, CancellationToken token = default) => inner.AddAsync(job, token);
        public async Task<bool> TryRemoveAsync(ScheduledResend expected, CancellationToken token = default)
        {
            if (!afterClaim)
            {
                cancellation.Cancel();
                token.ThrowIfCancellationRequested();
            }
            var claimed = await inner.TryRemoveAsync(expected, token);
            if (claimed) cancellation.Cancel();
            return claimed;
        }
    }
}
