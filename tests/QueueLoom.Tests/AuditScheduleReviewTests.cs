using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("body")]
    [InlineData("key")]
    [InlineData("header")]
    [InlineData("tombstone")]
    public async Task Review_RefreshKeepsEqualWirePayloadButAcceptsExternalPayloadChanges(string change)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Wire payload", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        var seed = DueJob(profile) with { DueAt = DateTimeOffset.UtcNow.AddHours(1) };
        var draft = new MessageDraft(new EditableMessageBody("original", MessageBodyFormat.Text))
        {
            KafkaEnvelope = new KafkaEnvelope([1, 2], false, [new KafkaRawHeader("binary", [3, 4])],
                EditableMessageProperties.Empty, [])
        };
        var job = seed with { Items = [seed.Items[0] with { Message = draft }] };
        store.Add(job);
        await using var vm = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(),
            new FakeWorkspace(), new FakeDialogService(), scheduledResends: store);
        var original = Assert.Single(vm.ScheduledResends);
        await vm.RunDueScheduledResendsAsync();
        Assert.Same(original, Assert.Single(vm.ScheduledResends));

        var edited = change switch
        {
            "body" => new MessageDraft(new EditableMessageBody("edited", MessageBodyFormat.Text)) { KafkaEnvelope = draft.KafkaEnvelope },
            "key" => draft with { KafkaEnvelope = draft.KafkaEnvelope! with { Key = [] } },
            "header" => draft with { KafkaEnvelope = draft.KafkaEnvelope! with { Headers = [new KafkaRawHeader("binary", null)] } },
            _ => draft with { KafkaEnvelope = draft.KafkaEnvelope! with { IsTombstone = true } }
        };
        new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path)).Save([job with { Items = [job.Items[0] with { Message = edited }] }]);
        await vm.RunDueScheduledResendsAsync();

        var refreshed = Assert.Single(vm.ScheduledResends);
        Assert.NotSame(original, refreshed);
        Assert.Equal(edited.Body, refreshed.Resend.Items[0].Message.Body);
        Assert.Equal(edited.KafkaEnvelope!.Key, refreshed.Resend.Items[0].Message.KafkaEnvelope!.Key);
        Assert.Equal(edited.KafkaEnvelope.IsTombstone, refreshed.Resend.Items[0].Message.KafkaEnvelope!.IsTombstone);
        Assert.Equal(edited.KafkaEnvelope.Headers[0].Value, refreshed.Resend.Items[0].Message.KafkaEnvelope!.Headers[0].Value);
    }

    [Fact]
    public async Task Review_UnchangedScheduledRowCanStillBeCancelledAfterTick()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = CreateProfile("Row identity", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var store = new JsonScheduledResendStore(paths);
        store.Add(DueJob(profile) with { DueAt = DateTimeOffset.UtcNow.AddHours(1) });
        await using var vm = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(),
            new FakeWorkspace(), new FakeDialogService(), scheduledResends: store);
        var row = Assert.Single(vm.ScheduledResends);

        await vm.RunDueScheduledResendsAsync();
        vm.CancelScheduledResendCommand.Execute(row);

        Assert.Empty(vm.ScheduledResends);
        Assert.Empty(store.Load());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Review_RunNowRowIsRemovedWhenTickOverlapsClaimOrSend(bool duringSend)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Run now", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        store.Add(DueJob(profile) with { DueAt = DateTimeOffset.UtcNow.AddHours(1) });
        var repository = new FakeProfileRepository([profile], profile.Id);
        var workspace = new FakeWorkspace();
        await using var vm = new MainWindowViewModel(repository, new FakeSecretVault(), workspace,
            new FakeDialogService(), scheduledResends: store);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        var paused = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (duringSend) workspace.SendGate = async () => { paused.TrySetResult(); await release.Task; };
        else repository.GetGate = async (_, _) => { paused.TrySetResult(); await release.Task; return profile; };
        var run = vm.RunScheduledNowAsync(Assert.Single(vm.ScheduledResends));
        try
        {
            await paused.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await vm.RunDueScheduledResendsAsync();
        }
        finally { release.TrySetResult(); await run; }
        Assert.Empty(vm.ScheduledResends);
        Assert.Empty(store.Load());
        Assert.Single(workspace.SentMessages);
    }

    [Fact]
    public async Task Review_DamagedScheduleWarningIncludesNumberOfRemovedRows()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Damaged list", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        store.Save([DueJob(profile), DueJob(profile)]);
        await using var vm = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(),
            new FakeWorkspace(), new FakeDialogService(), scheduledResends: store);
        File.WriteAllText(store.FilePath, "{damaged schedule list");

        await vm.RunDueScheduledResendsAsync();

        var warning = Assert.Single(vm.Activity, a => a.Action == "Scheduled resends not loaded");
        Assert.Contains("2 removed from this list", warning.Details, StringComparison.Ordinal);
        Assert.Empty(vm.ScheduledResends);
        Assert.Single(Directory.GetFiles(directory.Path, "*.damaged-*"));
    }

    [Fact]
    public async Task Review_UnchangedScheduleReadFailureDoesNotReappearAfterDismissal()
    {
        var failing = new MutableScheduleReadFailure();
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        SetPrivate(vm, "_scheduledStore", failing);
        await vm.RunDueScheduledResendsAsync();
        Assert.Contains("first error", vm.ErrorText, StringComparison.Ordinal);
        typeof(MainWindowViewModel).GetProperty(nameof(vm.ErrorText))!.SetValue(vm, "");

        await vm.RunDueScheduledResendsAsync();

        Assert.False(vm.HasError);
        failing.Failure = "new error";
        await vm.RunDueScheduledResendsAsync();
        Assert.Contains("new error", vm.ErrorText, StringComparison.Ordinal);
    }

    private sealed class MutableScheduleReadFailure : IScheduledResendStore
    {
        public string Failure { get; set; } = "first error";
        public IReadOnlyList<ScheduledResend> Load() => throw new IOException(Failure);
        public void Save(IReadOnlyList<ScheduledResend> jobs) => throw new NotSupportedException();
        public void Add(ScheduledResend job) => throw new NotSupportedException();
        public bool TryRemove(ScheduledResend expected) => throw new NotSupportedException();
    }
}
