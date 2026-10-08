using QueueLoom.Core.Abstractions;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// Each ordinary Activity entry was a file created on the caller's thread, which is the window's: a monitor check
// changing many queues, or a slow or scanned disk, stalled the window. Ordinary entries are now written by one
// background writer; the record written before a destructive operation is still on disk before Append returns.
// Tests synchronize on the writer's own hook, never on elapsed time.
public sealed class ActivityBackgroundWriterTests
{
    private static readonly TimeSpan DeadlockGuard = TimeSpan.FromSeconds(30);

    private static ActivityRecord Record(string action, int second = 0) =>
        new(Guid.NewGuid(), new DateTimeOffset(2026, 10, 8, 12, 0, second, TimeSpan.Zero), "Info", action, "details", null, null, null);

    private static int FilesOn(string directory) =>
        Directory.Exists(directory) ? Directory.EnumerateFiles(directory, "*.json", SearchOption.AllDirectories).Count() : 0;

    [Fact]
    public async Task AnOrdinaryEntryIsWrittenByTheBackgroundWriterAndReadableMeanwhile()
    {
        using var directory = new TemporaryDirectory();
        await using var journal = new FileActivityJournal(directory.Path);
        var writerReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        journal.BeforeEntryWrite = async () =>
        {
            writerReached.TrySetResult();
            await release.Task;
        };

        journal.AppendEntry(Record("queued"));

        // AppendEntry returned while the writer is still held, and nothing is on disk yet: the caller did not write it.
        await writerReached.Task.WaitAsync(DeadlockGuard);
        Assert.Equal(0, FilesOn(directory.Path));
        // Reading meanwhile still shows it.
        Assert.Contains(journal.ReadRecent(), record => record.Action == "queued");

        release.TrySetResult();
        await journal.WaitForPendingEntriesAsync().WaitAsync(DeadlockGuard);
        Assert.Equal(1, FilesOn(directory.Path));
        Assert.Single(journal.ReadRecent(), record => record.Action == "queued");
    }

    [Fact]
    public async Task TheRecordBeforeADestructiveOperationIsOnDiskWhenAppendReturns()
    {
        using var directory = new TemporaryDirectory();
        await using var journal = new FileActivityJournal(directory.Path);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        journal.BeforeEntryWrite = () => release.Task;
        journal.AppendEntry(Record("ordinary"));

        journal.Append(Record("intent", 1));

        // Written and forced to disk although the ordinary entry before it is still queued.
        Assert.Equal(1, FilesOn(directory.Path));
        Assert.Contains(journal.ReadRecent(), record => record.Action == "intent");
        release.TrySetResult();
        await journal.WaitForPendingEntriesAsync().WaitAsync(DeadlockGuard);
        Assert.Equal(2, FilesOn(directory.Path));
    }

    [Fact]
    public async Task ClosingWritesEveryQueuedEntry()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        journal.BeforeEntryWrite = () => release.Task;
        for (var index = 0; index < 50; index++) journal.AppendEntry(Record($"entry {index}", index));

        var closing = journal.DisposeAsync().AsTask();
        release.TrySetResult();
        await closing.WaitAsync(DeadlockGuard);

        Assert.Equal(50, FilesOn(directory.Path));
    }

    [Fact]
    public async Task ClosingWaitsOnlyBoundedForAStuckWriter()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path) { CloseDeadline = TimeSpan.FromMilliseconds(200) };
        var never = new TaskCompletionSource();
        journal.BeforeEntryWrite = () => never.Task;
        journal.AppendEntry(Record("stuck"));

        // Returns although the writer never finishes: closing the window is not held by the disk.
        await journal.DisposeAsync().AsTask().WaitAsync(DeadlockGuard);
    }

    [Fact]
    public async Task AFailedEntryIsReportedAndLaterEntriesAreStillWritten()
    {
        using var directory = new TemporaryDirectory();
        await using var journal = new FileActivityJournal(directory.Path);
        var failures = new List<Exception>();
        journal.EntryWriteFailed += failures.Add;
        var first = true;
        journal.BeforeEntryWrite = () =>
        {
            if (!first) return Task.CompletedTask;
            first = false;
            throw new IOException("disk full");
        };

        journal.AppendEntry(Record("lost"));
        journal.AppendEntry(Record("kept", 1));
        await journal.WaitForPendingEntriesAsync().WaitAsync(DeadlockGuard);

        Assert.Equal("disk full", Assert.Single(failures).Message);
        Assert.Equal(["kept"], journal.ReadRecent().Select(record => record.Action));
    }

    [Fact]
    public async Task AnEntryAfterClosingIsWrittenDirectly()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        await journal.DisposeAsync();

        journal.AppendEntry(Record("late"));

        Assert.Equal(1, FilesOn(directory.Path));
    }
}

public sealed partial class ViewModelStateTests
{
    // An entry the background writer cannot save is reported in the window, as a failed synchronous write was.
    [Fact]
    public async Task AnActivityEntryThatCannotBeSavedInTheBackgroundIsReported()
    {
        using var directory = new TemporaryDirectory();
        await using var journal = new FileActivityJournal(directory.Path)
        {
            BeforeEntryWrite = () => throw new IOException("disk full")
        };
        var profile = CreateProfile("Journal", QueueLoom.Core.Profiles.EnvironmentKind.Development, QueueLoom.Core.Profiles.ProfileAccessMode.ReadWrite);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), activityJournal: journal);
        await vm.InitializeAsync();

        await vm.ConnectCommand.ExecuteAsync();
        await journal.WaitForPendingEntriesAsync().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Contains("Activity journal could not be saved", vm.ErrorText, StringComparison.Ordinal);
        Assert.Contains("disk full", vm.ErrorText, StringComparison.Ordinal);
    }
}

public sealed partial class ViewModelStateTests
{
    // A write failure reported while the window is closing reaches it only after it has closed: it is dropped there,
    // not shown on a closed window (as UI tests closing their window and deleting its folder would cause).
    [Fact]
    public async Task AnActivityWriteFailureArrivingAfterTheWindowClosedIsDropped()
    {
        using var directory = new TemporaryDirectory();
        var window = new QueuedContext();
        await using var journal = new FileActivityJournal(directory.Path) { BeforeEntryWrite = () => throw new IOException("folder gone") };
        var profile = CreateProfile("Closing", QueueLoom.Core.Profiles.EnvironmentKind.Development, QueueLoom.Core.Profiles.ProfileAccessMode.ReadWrite);
        var previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(window);
        QueueLoom.App.ViewModels.MainWindowViewModel vm;
        try { vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), activityJournal: journal); }
        finally { SynchronizationContext.SetSynchronizationContext(previous); }

        vm.ReportLocalDataProblem("Entry", "written in the background");
        await journal.WaitForPendingEntriesAsync().WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(1, window.Queued);
        await vm.DisposeAsync();
        var before = vm.ErrorText;

        window.RunQueued();

        Assert.Equal(before, vm.ErrorText);
    }

    private sealed class QueuedContext : SynchronizationContext
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<(SendOrPostCallback Callback, object? State)> _posts = new();
        public int Queued => _posts.Count;
        public override void Post(SendOrPostCallback d, object? state) => _posts.Enqueue((d, state));
        public void RunQueued() { while (_posts.TryDequeue(out var post)) post.Callback(post.State); }
    }
}
