using Avalonia;
using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.Input;
using Avalonia.VisualTree;
using QueueLoom.App.Controls;

namespace QueueLoom.UiTests;

public sealed class MonitorsHistoryUiTests
{
    [Fact]
    public Task History_DrawsTheDayWithThePeakAndTheLargestQueues() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.NavigateAsync("Monitors");

        var chart = fixture.Window.GetVisualDescendants().OfType<HistoryChart>().Single();
        chart.BringIntoView();
        await fixture.SettleAsync();

        Assert.True(fixture.ViewModel.HasHistory);
        Assert.InRange(chart.Points!.Count, 143, 145);
        Assert.Equal(DemoData.Development.Id, fixture.ViewModel.HistoryProfile?.Id);
        Assert.Equal("invoices-retry", fixture.ViewModel.HistorySources[0].Name);
        Assert.True(fixture.ViewModel.IsHistoryRising);

        // Hovering shows the nearest sample; the frame must still render.
        var middle = chart.TranslatePoint(new Point(chart.Bounds.Width / 2, chart.Bounds.Height / 2), fixture.Window)!.Value;
        fixture.Window.MouseMove(middle, RawInputModifiers.None);
        await fixture.SettleAsync();
        using (var frame = fixture.Window.CaptureRenderedFrame())
        {
            Assert.NotNull(frame);
            var directory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
                await using var file = File.Create(Path.Combine(directory, "dark-monitors-history.png"));
                frame!.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
            }
        }

        fixture.ViewModel.HistoryRange = "Last 6 hours";
        await fixture.SettleAsync();
        Assert.InRange(chart.Points!.Count, 36, 37);

        fixture.ViewModel.HistoryProfile = fixture.ViewModel.Profiles.Single(profile => profile.Id != DemoData.Development.Id);
        await fixture.SettleAsync();
        Assert.False(fixture.ViewModel.HasHistory);
        Assert.Contains("No checks of", fixture.ViewModel.HistoryEmptyText, StringComparison.Ordinal);
        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
    });
}
