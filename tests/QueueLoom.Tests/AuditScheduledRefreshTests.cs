using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task Audit_AlreadyOpenWindowRunsDueJobAddedByAnotherStore()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = CreateProfile("External schedule", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace();
        await using var window = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id),
            new FakeSecretVault(), workspace, new FakeDialogService(), scheduledResends: new JsonScheduledResendStore(paths));
        await window.InitializeAsync();
        await window.ConnectCommand.ExecuteAsync();
        Assert.Empty(window.ScheduledResends);
        new JsonScheduledResendStore(paths).Add(DueJob(profile));

        await window.RunDueScheduledResendsAsync();
        await window.RunDueScheduledResendsAsync();

        Assert.Single(workspace.SentMessages);
        Assert.Empty(new JsonScheduledResendStore(paths).Load());
        Assert.Empty(window.ScheduledResends);
    }

    [Fact]
    public async Task Audit_ConcurrentWindowsReadingSameExternalJobStillSendOnce()
    {
        using var directory = new TemporaryDirectory();
        using var bothRead = new Barrier(2);
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = CreateProfile("Concurrent claims", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var a = new FakeWorkspace(); var b = new FakeWorkspace();
        var storeA = new ContendedScheduleStore(new JsonScheduledResendStore(paths), bothRead);
        var storeB = new ContendedScheduleStore(new JsonScheduledResendStore(paths), bothRead);
        await using var windowA = await Window(a, storeA);
        await using var windowB = await Window(b, storeB);
        new JsonScheduledResendStore(paths).Add(DueJob(profile));
        storeA.Contend = storeB.Contend = true;

        await Task.WhenAll(Task.Run(() => windowA.RunDueScheduledResendsAsync()),
            Task.Run(() => windowB.RunDueScheduledResendsAsync()));

        Assert.Equal(1, a.SentMessages.Count + b.SentMessages.Count);
        Assert.Empty(new JsonScheduledResendStore(paths).Load());
        Assert.Empty(windowA.ScheduledResends);
        Assert.Empty(windowB.ScheduledResends);
        async Task<MainWindowViewModel> Window(FakeWorkspace workspace, IScheduledResendStore store)
        {
            var vm = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(), workspace,
                new FakeDialogService(), scheduledResends: store);
            await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync(); return vm;
        }
    }

    private sealed class ContendedScheduleStore(JsonScheduledResendStore inner, Barrier bothRead) : IScheduledResendStore
    {
        public bool Contend { get; set; }
        public IReadOnlyList<ScheduledResend> Load()
        {
            var snapshot = inner.Load();
            if (Contend && !bothRead.SignalAndWait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Both windows must read the pending job before either claims it.");
            return snapshot;
        }
        public void Save(IReadOnlyList<ScheduledResend> jobs) => inner.Save(jobs);
        public void Add(ScheduledResend job) => inner.Add(job);
        public bool TryRemove(ScheduledResend expected) => inner.TryRemove(expected);
        public string? TakeSetAsideFile() => inner.TakeSetAsideFile();
    }

    [Fact]
    public async Task Audit_ConcurrentOpenWindowsClaimExternalDueJobOnce()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = CreateProfile("Two windows", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var a = new FakeWorkspace(); var b = new FakeWorkspace();
        await using var windowA = await Window(a);
        await using var windowB = await Window(b);
        var sending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        a.SendGate = async () => { sending.TrySetResult(); await release.Task; };
        new JsonScheduledResendStore(paths).Add(DueJob(profile));
        var first = windowA.RunDueScheduledResendsAsync();
        try
        {
            await sending.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await windowB.RunDueScheduledResendsAsync();
            Assert.Empty(b.SentMessages);
        }
        finally { release.TrySetResult(); await first; }
        await windowB.RunDueScheduledResendsAsync();
        Assert.Single(a.SentMessages);
        Assert.Empty(b.SentMessages);
        Assert.Empty(new JsonScheduledResendStore(paths).Load());

        async Task<MainWindowViewModel> Window(FakeWorkspace workspace)
        {
            var vm = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(), workspace,
                new FakeDialogService(), scheduledResends: new JsonScheduledResendStore(paths));
            await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync(); return vm;
        }
    }
}
