using System.Diagnostics;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// Scheduled resends are shared by every QueueLoom window through one file and its cross-process lock. While another
// window holds that lock (for up to 30 seconds), this window waited for it on its own thread: the background check every
// 20 seconds, cancelling, scheduling and running a job froze the window. Each now waits without holding the window's
// thread, and completes once the lock is free.
public sealed partial class ViewModelStateTests
{
    private static readonly TimeSpan WindowThreadBudget = TimeSpan.FromSeconds(2);

    [Theory]
    [InlineData("check")]
    [InlineData("cancel")]
    [InlineData("schedule")]
    [InlineData("run")]
    public async Task ScheduledResends_AnotherWindowHoldingTheListDoesNotFreezeThisWindow(string action)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Contended", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var schedules = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        if (action != "schedule") schedules.Add(DueJob(profile) with { DueAt = DateTimeOffset.UtcNow.AddHours(1) });
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService
        {
            ConfirmResult = true,
            ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddHours(1), PreserveMessageIds = false }
        };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs, scheduledResends: schedules);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        if (action == "schedule")
        {
            vm.Messages.Add(new MessageItemViewModel(OperationItem(7).Original, profile.Id) { IsMarked = true });
        }

        // Another window holds the shared list.
        var otherWindow = new FileStream(schedules.FilePath + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        Task? pending;
        var windowThread = Stopwatch.StartNew();
        try
        {
            pending = action switch
            {
                "check" => vm.RunDueScheduledResendsAsync(),
                // Cancel is a plain command: its work goes on after Execute returns, and is observed below.
                "cancel" => Execute(() => vm.CancelScheduledResendCommand.Execute(Assert.Single(vm.ScheduledResends))),
                "schedule" => vm.ResendMarkedMessagesCommand.ExecuteAsync(),
                _ => vm.RunScheduledNowAsync(Assert.Single(vm.ScheduledResends))
            };
            windowThread.Stop();
            Assert.True(windowThread.Elapsed < WindowThreadBudget,
                $"The window's thread waited {windowThread.Elapsed} for another window's lock.");
            if (pending is not null) Assert.False(pending.IsCompleted, "The action finished while the list was still held.");
        }
        finally
        {
            await otherWindow.DisposeAsync();
        }

        // Once the other window lets go, the action completes as before.
        if (pending is not null) await pending.WaitAsync(TimeSpan.FromSeconds(30));
        await WaitUntilListSettledAsync(() => action switch
        {
            "check" => schedules.Load().Count == 1 && vm.ScheduledResends.Count == 1,
            "schedule" => schedules.Load().Count == 1 && vm.ScheduledResends.Count == 1,
            "cancel" => schedules.Load().Count == 0 && vm.ScheduledResends.Count == 0,
            _ => schedules.Load().Count == 0 && workspace.SentMessages.Count == 1
        });
    }

    private static Task? Execute(Action execute)
    {
        execute();
        return null;
    }

    private static async Task WaitUntilListSettledAsync(Func<bool> condition)
    {
        var waited = Stopwatch.StartNew();
        while (!condition())
        {
            Assert.True(waited.Elapsed < TimeSpan.FromSeconds(30), "The action did not complete after the list was released.");
            await Task.Delay(20);
        }
    }
}
