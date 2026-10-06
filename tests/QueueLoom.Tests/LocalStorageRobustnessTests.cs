using System.Diagnostics;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class LocalStorageRobustnessTests
{
    // Restricting the lock file's permissions fails (an ACL-less file system, a file owned by someone else): the
    // acquire fails, and the stream it opened is released, so the next acquire gets the lock at once instead of
    // waiting out the 30 s deadline behind a lock this process itself still held.
    [Fact]
    public async Task AFailedLockAcquireDoesNotKeepTheLock()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "store.lock");
        CrossProcessFileLock.RestrictOverride.Value = _ => throw new UnauthorizedAccessException("permissions cannot be changed here");
        try
        {
            await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await CrossProcessFileLock.AcquireAsync(path, CancellationToken.None));
        }
        finally
        {
            CrossProcessFileLock.RestrictOverride.Value = null;
        }

        var watch = Stopwatch.StartNew();
        await using var second = await CrossProcessFileLock.AcquireAsync(path, CancellationToken.None);

        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5), $"The second acquire took {watch.Elapsed}.");
    }

    // A damaged or newer settings file falls back to defaults; the store now says why, so the window can tell.
    [Theory]
    [InlineData("{ not json", "could not be read")]
    [InlineData("""{ "SchemaVersion": 7 }""", "newer QueueLoom")]
    [InlineData("""{ "SchemaVersion": 1, "MonitorIntervalSeconds": 60 }""", null)]
    public async Task TheSettingsStoreSaysWhyItUsedDefaults(string content, string? expected)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        await File.WriteAllTextAsync(paths.SettingsFile, content);
        using var store = new JsonAppSettingsStore(paths);

        await store.LoadAsync();

        if (expected is null) Assert.Null(store.LoadProblem);
        else Assert.Contains(expected, store.LoadProblem, StringComparison.Ordinal);
    }
}

public sealed class LogLimitTests
{
    // Another process holds the log's write lock for a moment (longer than the 250 ms #84 allowed): the line waits for
    // it and is written, not dropped.
    [Fact]
    public async Task ALineWaitsForABrieflyHeldLockInsteadOfBeingDropped()
    {
        using var directory = new TemporaryDirectory();
        using var provider = new QueueLoom.Infrastructure.Logging.FileLoggerProvider(directory.Path);
        var logger = provider.CreateLogger("app");
        var held = new FileStream(Path.Combine(directory.Path, ".queueloom-log.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        var release = Task.Run(async () =>
        {
            await Task.Delay(700);
            held.Dispose();
        });

        Microsoft.Extensions.Logging.LoggerExtensions.LogWarning(logger, "written after the wait");
        await release;

        Assert.Contains("written after the wait", File.ReadAllText(provider.CurrentFilePath), StringComparison.Ordinal);
    }

    // The daily log is capped at 10 MB. Reaching it is now said once in the log, so the missing lines that follow are
    // explained instead of the log just going quiet.
    [Fact]
    public void AFullDailyLogSaysOnceWhyItStops()
    {
        using var directory = new TemporaryDirectory();
        var now = new DateTimeOffset(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);
        using var provider = new QueueLoom.Infrastructure.Logging.FileLoggerProvider(directory.Path, clock: () => now);
        File.WriteAllBytes(provider.CurrentFilePath, new byte[10 * 1024 * 1024 + 1]);
        var logger = provider.CreateLogger("app");

        Microsoft.Extensions.Logging.LoggerExtensions.LogWarning(logger, "first dropped line");
        Microsoft.Extensions.Logging.LoggerExtensions.LogWarning(logger, "second dropped line");

        var tail = System.Text.Encoding.UTF8.GetString(File.ReadAllBytes(provider.CurrentFilePath)[(10 * 1024 * 1024 + 1)..]);
        Assert.Equal(1, tail.Split("nothing more is written to it today").Length - 1);
        Assert.DoesNotContain("dropped line", tail, StringComparison.Ordinal);
    }

    // If the marker cannot be created, nothing is appended either: the capped log stays capped.
    [Fact]
    public void AFullLogStaysCappedWhenItsMarkerCannotBeCreated()
    {
        using var directory = new TemporaryDirectory();
        var now = new DateTimeOffset(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);
        using var provider = new QueueLoom.Infrastructure.Logging.FileLoggerProvider(directory.Path, clock: () => now);
        File.WriteAllBytes(provider.CurrentFilePath, new byte[10 * 1024 * 1024 + 1]);
        Directory.CreateDirectory(provider.CurrentFilePath + ".full");
        var logger = provider.CreateLogger("app");

        for (var i = 0; i < 5; i++)
        {
            Microsoft.Extensions.Logging.LoggerExtensions.LogWarning(logger, "dropped line");
        }

        Assert.Equal(10 * 1024 * 1024 + 1, new FileInfo(provider.CurrentFilePath).Length);
    }
}

public sealed partial class ViewModelStateTests
{
    // A damaged list of scheduled resends is set aside; the window says so (its jobs will not run) instead of
    // silently showing an empty list.
    [Fact]
    public async Task ADamagedScheduleListIsReportedInActivity()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var store = new JsonScheduledResendStore(paths);
        await File.WriteAllTextAsync(store.FilePath, "[ { broken");

        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace(), scheduledResends: store);

        var warning = Assert.Single(vm.Activity, item => item.Action == "Scheduled resends not loaded");
        Assert.Equal("Warning", warning.Level);
        Assert.Contains(".damaged-", warning.Details, StringComparison.Ordinal);
        Assert.Empty(vm.ScheduledResends);
    }

    // The list can also be found damaged later, when a job is cancelled: the jobs it held leave the window's list
    // too (they will not run) and the operator is told once.
    [Fact]
    public async Task AScheduleListDamagedLaterIsReportedWhenAJobIsCancelled()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var store = new JsonScheduledResendStore(paths);
        var due = DateTimeOffset.UtcNow.AddDays(1);
        store.Save([
            new ScheduledResend(Guid.NewGuid(), Guid.NewGuid(), "dev", DateTimeOffset.UtcNow, due, ResendMode.Copy, 0, "x", []),
            new ScheduledResend(Guid.NewGuid(), Guid.NewGuid(), "dev", DateTimeOffset.UtcNow, due, ResendMode.Copy, 0, "x", [])]);
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace(), scheduledResends: store);
        Assert.Equal(2, vm.ScheduledResends.Count);
        await File.WriteAllTextAsync(store.FilePath, "[ { broken");

        vm.CancelScheduledResendCommand.Execute(vm.ScheduledResends[0]);

        var warning = Assert.Single(vm.Activity, item => item.Action == "Scheduled resends not loaded");
        Assert.Contains(".damaged-", warning.Details, StringComparison.Ordinal);
        Assert.Empty(vm.ScheduledResends);
        Assert.Single(Directory.GetFiles(Path.GetDirectoryName(store.FilePath)!, "*.damaged-*"));
    }

    private static ScheduledResend LaterJob(Guid profileId) =>
        new(Guid.NewGuid(), profileId, "dev", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1), ResendMode.Copy, 0, "x", []);

    // Deleting environment A reads the damaged list directly: the warning is given, and B's cached job (it will not
    // run) leaves the list.
    [Fact]
    public async Task AScheduleListFoundDamagedWhileDeletingAnEnvironmentIsReported()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var a = CreateProfile("A", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var b = CreateProfile("B", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var store = new JsonScheduledResendStore(paths);
        store.Save([LaterJob(a.Id), LaterJob(b.Id)]);
        await using var vm = new MainWindowViewModel(new FakeProfileRepository([a, b], a.Id), new FakeSecretVault(), new FakeWorkspace(),
            new FakeDialogService { ConfirmResult = true }, scheduledResends: store);
        await vm.InitializeAsync();
        Assert.Equal(2, vm.ScheduledResends.Count);
        await File.WriteAllTextAsync(store.FilePath, "[ { broken");

        vm.SelectedProfile = vm.Profiles.Single(profile => profile.Name == "A");
        await vm.DeleteEnvironmentCommand.ExecuteAsync();

        Assert.Single(vm.Activity, item => item.Action == "Scheduled resends not loaded");
        Assert.Empty(vm.ScheduledResends);
    }

    // A background read (the history cleanup) finds the list damaged and no job changes afterwards: the next schedule
    // check reports it.
    [Fact]
    public async Task AScheduleListFoundDamagedInTheBackgroundIsReported()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var store = new JsonScheduledResendStore(paths);
        store.Save([LaterJob(Guid.NewGuid())]);
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace(), scheduledResends: store);
        await File.WriteAllTextAsync(store.FilePath, "[ { broken");
        store.Load();

        await vm.RunDueScheduledResendsAsync();

        Assert.Single(vm.Activity, item => item.Action == "Scheduled resends not loaded");
        Assert.Empty(vm.ScheduledResends);
    }

    // Adding a job finds the old list damaged, sets it aside, and then cannot save: the save error stands and the
    // set-aside list is still reported.
    [Fact]
    public async Task AScheduleListSetAsideByAFailedAddIsReported()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var profile = CreateProfile("Dev", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var inner = new JsonScheduledResendStore(paths);
        inner.Save([LaterJob(profile.Id)]);
        var store = new SaveFailingStore(inner);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), scheduledResends: store);
        await File.WriteAllTextAsync(inner.FilePath, "[ { broken");
        var schedule = typeof(MainWindowViewModel).GetMethod("ScheduleResend",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;

        var error = Assert.Throws<System.Reflection.TargetInvocationException>(() => schedule.Invoke(vm,
            [profile, Array.Empty<ResendItem>(), new ResendOptions(null, ResendMode.Copy, 0), DateTimeOffset.UtcNow.AddDays(1)]));

        Assert.IsType<IOException>(error.InnerException);
        Assert.Single(vm.Activity, item => item.Action == "Scheduled resends not loaded");
        Assert.Empty(vm.ScheduledResends);
    }

    private sealed class SaveFailingStore(JsonScheduledResendStore inner) : IScheduledResendStore
    {
        public IReadOnlyList<ScheduledResend> Load() => inner.Load();
        public void Save(IReadOnlyList<ScheduledResend> resends) => inner.Save(resends);
        public void Add(ScheduledResend resend)
        {
            inner.Load();
            throw new IOException("The disk is full.");
        }
        public bool TryRemove(ScheduledResend expected) => inner.TryRemove(expected);
        public string? TakeSetAsideFile() => inner.TakeSetAsideFile();
    }
}
