using System.Reflection;
using QueueLoom.App.ViewModels;

namespace QueueLoom.UiTests;

public sealed class OutageStartupClosingUiTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Outage_RealWindowClosesDuringStartupAndDefersCleanup(bool honoursCancellation) => UiSession.RunAsync(async () =>
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = false;
        await using var fixture = await WindowFixture.OpenHeldAsync(async token =>
        {
            using var registration = token.Register(() => cancelled = true);
            started.TrySetResult();
            if (honoursCancellation) await release.Task.WaitAsync(token);
            else await release.Task;
        });
        await started.Task;
        fixture.ViewModel.KeepInTray = false;
        typeof(MainWindowViewModel).GetProperty("ShutdownDrainTimeout", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(fixture.ViewModel, TimeSpan.FromMilliseconds(100));
        var servicesReleased = 0;
        fixture.Window.ShutdownCompleted = () => { servicesReleased++; return ValueTask.CompletedTask; };
        var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Window.Closed += (_, _) => closed.TrySetResult();
        try
        {
            fixture.Window.Close();
            for (var i = 0; i < 30 && !closed.Task.IsCompleted; i++) await fixture.SettleAsync();
            Assert.True(closed.Task.IsCompleted, "Closing was held by startup.");
            Assert.True(cancelled, "Shutdown cancellation must reach startup before waiting for it.");
            if (!honoursCancellation) Assert.Equal(0, servicesReleased);
        }
        finally
        {
            release.TrySetResult();
            for (var i = 0; i < 40 && (!closed.Task.IsCompleted || servicesReleased == 0); i++) await fixture.SettleAsync();
        }
        Assert.Equal(1, servicesReleased);
    });
}
