using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.UiTests;

/// <summary>
/// Bug 8: closing a window does not write its own (possibly stale) monitor interval again; it only waits for its
/// saves still under way.
/// </summary>
public sealed class ClosingPreferencesUiTests
{
    // Windows A and B loaded 60 s; B saved 300 s. Closing A, which did not change the interval, keeps 300 s.
    [Fact]
    public Task Closing_keeps_the_interval_another_window_saved() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync(connect: false);
        var paths = QueueLoomPaths.ForRoot(fixture.DataDirectory);
        using (var otherWindow = new JsonAppSettingsStore(paths))
        {
            await otherWindow.SaveMonitorIntervalSecondsAsync(300);
        }

        await CloseAsync(fixture);

        using var settings = new JsonAppSettingsStore(paths);
        Assert.Equal(300, (await settings.LoadAsync()).MonitorIntervalSeconds);
    });

    // A change made in this window just before closing is still saved: closing waits for that save.
    [Fact]
    public Task Closing_finishes_this_windows_own_pending_save() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync(connect: false);
        fixture.ViewModel.MonitorIntervalSeconds = 120;

        await CloseAsync(fixture);

        using var settings = new JsonAppSettingsStore(QueueLoomPaths.ForRoot(fixture.DataDirectory));
        Assert.Equal(120, (await settings.LoadAsync()).MonitorIntervalSeconds);
    });

    // A change made while the window is still starting (environments not loaded yet) is saved too, although closing no
    // longer writes the interval itself.
    [Fact]
    public Task An_interval_change_during_startup_is_saved() => UiSession.RunAsync(async () =>
    {
        var startup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var fixture = await WindowFixture.OpenHeldAsync(startup.Task);
        Assert.Equal(60, fixture.ViewModel.MonitorIntervalSeconds); // preferences applied, environments still loading
        Assert.Empty(fixture.ViewModel.Profiles);

        fixture.ViewModel.MonitorIntervalSeconds = 120;
        startup.SetResult();
        await fixture.SettleAsync();
        await CloseAsync(fixture);

        using var settings = new JsonAppSettingsStore(QueueLoomPaths.ForRoot(fixture.DataDirectory));
        Assert.Equal(120, (await settings.LoadAsync()).MonitorIntervalSeconds);
    });

    private static async Task CloseAsync(WindowFixture fixture)
    {
        var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Window.Closed += (_, _) => closed.TrySetResult();
        fixture.Window.Close();
        for (var attempt = 0; attempt < 200 && !closed.Task.IsCompleted; attempt++)
        {
            await fixture.SettleAsync();
            await Task.Delay(25);
        }
        Assert.True(closed.Task.IsCompleted, "The window did not finish closing.");
    }
}
