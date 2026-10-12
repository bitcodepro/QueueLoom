using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// A scheduled resend the operation store cannot hold is refused when it is scheduled, not when it is due.
public sealed partial class ViewModelStateTests
{
    private static readonly ServiceBusEntityReference SizeSource = ServiceBusEntityReference.Queue("orders");

    private static ResendItem SizedItem(int index, int bytes, int? sentBytes = null)
    {
        var original = new BrowsedMessage(SizeSource, ServiceBusSubQueue.DeadLetter, index + 1, new byte[bytes],
            new EditableMessageProperties(MessageId: $"m-{index}"));
        var draft = original.CreateDraft();
        // A text replacement can make the sent body larger than the original.
        if (sentBytes is { } size) draft = new MessageDraft(new EditableMessageBody(new string('x', size), MessageBodyFormat.Text), draft.Properties, draft.ApplicationProperties);
        return new ResendItem(original, SizeSource, draft);
    }

    private static ResendItem[] Totalling(long total, int count = 2)
    {
        var each = (int)(total / count);
        return Enumerable.Range(0, count).Select(index => SizedItem(index, index == count - 1 ? (int)(total - each * (count - 1)) : each)).ToArray();
    }

    private async Task<(MainWindowViewModel Vm, JsonScheduledResendStore Store, Func<ResendItem[], Task> Schedule)> SchedulerAsync(TemporaryDirectory directory)
    {
        var profile = CreateProfile("Sized", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        var vm = new MainWindowViewModel(new FakeProfileRepository([profile], profile.Id), new FakeSecretVault(), new FakeWorkspace(),
            new FakeDialogService(), scheduledResends: store);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        var schedule = typeof(MainWindowViewModel).GetMethod("ScheduleResendAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (vm, store, items => (Task)schedule.Invoke(vm, [profile, items,
            new ResendOptions(SizeSource, ResendMode.Copy, 10, SendAt: DateTimeOffset.UtcNow.AddHours(1)), DateTimeOffset.UtcNow.AddHours(1), CancellationToken.None])!);
    }

    // Exactly 32 MiB fits: it is scheduled, and the real operation store takes the same items when due.
    [Fact]
    public async Task ExactlyTheOperationLimitIsScheduledAndPrepared()
    {
        using var directory = new TemporaryDirectory();
        var (vm, store, schedule) = await SchedulerAsync(directory);
        await using var _ = vm;
        var items = Totalling(BatchReplayStore.MaximumOperationBytes);

        await schedule(items);

        Assert.Single(store.Load());
        var replay = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        await replay.CreateResendAsync(Guid.NewGuid(), items, ResendMode.Copy, 10, null, "identity", "Scheduled resend", CancellationToken.None);
    }

    // One byte over (or over only after a text replacement grew the bodies): refused, nothing saved, no success shown.
    [Theory]
    [InlineData("one byte over")]
    [InlineData("grown by replacement")]
    public async Task AScheduleOverTheOperationLimitIsRefusedAndNotSaved(string kind)
    {
        using var directory = new TemporaryDirectory();
        var (vm, store, schedule) = await SchedulerAsync(directory);
        await using var _ = vm;
        var half = (int)(BatchReplayStore.MaximumOperationBytes / 2);
        var items = kind == "one byte over"
            ? Totalling(BatchReplayStore.MaximumOperationBytes + 1)
            : [SizedItem(0, 1_000, half), SizedItem(1, 1_000, half + 1)];

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => schedule(items));

        Assert.Contains("32 MiB", error.Message, StringComparison.Ordinal);
        Assert.Empty(store.Load());
        Assert.Empty(vm.ScheduledResends);
        Assert.DoesNotContain(vm.Activity, item => item.Action == "Resend scheduled");
        var replay = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            replay.CreateResendAsync(Guid.NewGuid(), items, ResendMode.Copy, 10, null, "identity", "Scheduled resend", CancellationToken.None));
    }
}
