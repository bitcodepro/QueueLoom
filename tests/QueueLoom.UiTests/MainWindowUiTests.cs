using Avalonia;
using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.Input;
using Avalonia.LogicalTree;
using Avalonia.Media;
using Avalonia.Styling;
using Avalonia.VisualTree;
using QueueLoom.App.Controls;
using QueueLoom.App.Models;
using NavigationPage = QueueLoom.App.Models.NavigationPage;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Settings;

namespace QueueLoom.UiTests;

public sealed class MainWindowUiTests
{
    private static readonly string[] Pages =
        ["Overview", "Explorer", "DeadLetters", "Backups", "Composer", "Monitors", "Environments", "Activity"];

    [Fact]
    public Task EveryPage_RendersWithoutBindingErrors() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.OpenDeadLettersAsync();

        foreach (var page in Pages)
        {
            await fixture.NavigateAsync(page);
            var host = fixture.Window.GetVisualDescendants().OfType<PageHost>().Single();
            Assert.Equal(Enum.Parse<NavigationPage>(page), host.Page);
            Assert.NotNull(host.Content);
            fixture.Window.CaptureRenderedFrame();
        }

        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
    });

    [Fact]
    public Task Pages_AreCreatedLazilyAndReused() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync(connect: false);
        var host = fixture.Window.GetVisualDescendants().OfType<PageHost>().Single();
        Assert.Equal([NavigationPage.Overview], host.CreatedPages.Keys);

        await fixture.NavigateAsync("Explorer");
        var explorer = host.Content;
        await fixture.NavigateAsync("Overview");
        await fixture.NavigateAsync("Explorer");

        Assert.Same(explorer, host.Content);
        Assert.Equal(2, host.CreatedPages.Count);
    });

    [Fact]
    public Task ExplorerContextMenu_CopiesEntityNameThroughViewModel() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.NavigateAsync("Explorer");

        var row = fixture.Window.GetVisualDescendants()
            .OfType<Grid>()
            .First(grid => grid.ContextMenu is not null && grid.DataContext is EntityItemViewModel { Name: "orders" });
        row.ContextMenu!.Open(row);
        await fixture.SettleAsync();
        var copy = row.ContextMenu.Items.OfType<MenuItem>().First();

        Assert.Equal("Copy queue name", copy.Header);
        Assert.NotNull(copy.Command);
        copy.Command!.Execute(copy.CommandParameter);
        await fixture.SettleAsync();
        row.ContextMenu.Close();

        Assert.Equal(["orders"], fixture.Clipboard.Copied);
        Assert.Contains(fixture.Notifications.Shown, toast => toast.Title == "Copied to clipboard");
    });

    [Fact]
    public Task ExplorerHeaders_SortByDeadLettersLargestFirst() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.NavigateAsync("Explorer");

        fixture.ViewModel.SortEntitiesCommand.Execute("DeadLetters");
        await fixture.SettleAsync();

        Assert.Equal("invoices-retry", fixture.ViewModel.Entities[0].Name);
        Assert.True(fixture.ViewModel.IsSortedByDeadLetters);

        fixture.ViewModel.SortEntitiesCommand.Execute("Hierarchy");
        await fixture.SettleAsync();
        Assert.Equal("orders", fixture.ViewModel.Entities[0].Name);
    });

    [Fact]
    public Task ControlF_FocusesTheCurrentPageSearchBox() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.NavigateAsync("Explorer");

        fixture.Window.KeyPressQwerty(PhysicalKey.F, RawInputModifiers.Control);
        fixture.Window.KeyReleaseQwerty(PhysicalKey.F, RawInputModifiers.Control);
        await fixture.SettleAsync();

        var focused = fixture.Window.FocusManager?.GetFocusedElement() as TextBox;
        Assert.NotNull(focused);
        Assert.Contains("search", focused!.Classes);
    });

    [Fact]
    public Task ControlDigit_NavigatesBetweenPages() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();

        fixture.Window.KeyPressQwerty(PhysicalKey.Digit7, RawInputModifiers.Control);
        fixture.Window.KeyReleaseQwerty(PhysicalKey.Digit7, RawInputModifiers.Control);
        await fixture.SettleAsync();

        Assert.Equal(NavigationPage.Environments, fixture.ViewModel.CurrentPage);
    });

    [Fact]
    public Task ThemeToggle_RecolorsToneBoundElements() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.NavigateAsync("Environments");
        fixture.ViewModel.ThemePreference = AppThemePreference.Dark;
        await fixture.SettleAsync();

        var connectedDot = fixture.Window.GetVisualDescendants()
            .OfType<Border>()
            .First(border => border.Classes.Contains("envDot") &&
                             border.DataContext is ProfileItemViewModel { IsConnected: true } &&
                             ToneAssist.GetBackground(border) == Tone.Success);
        var dark = ((ISolidColorBrush)connectedDot.Background!).Color;

        fixture.ViewModel.ThemePreference = AppThemePreference.Light;
        await fixture.SettleAsync();
        var light = ((ISolidColorBrush)connectedDot.Background!).Color;

        Assert.Equal(ThemeVariant.Light, fixture.Window.ActualThemeVariant);
        Assert.NotEqual(dark, light);
        fixture.ViewModel.ThemePreference = AppThemePreference.Dark;
    });

    [Fact]
    public Task FormatJsonError_PointsTheEditorAtTheFailingLine() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        fixture.ViewModel.NewMessageCommand.Execute(null);
        fixture.ViewModel.DraftBody = "{\n  \"a\": 1,\n  \"b\": oops\n}";
        fixture.ViewModel.FormatJsonCommand.Execute(null);
        await fixture.SettleAsync();

        Assert.Equal(3, fixture.ViewModel.DraftBodyErrorLine);
        var editor = fixture.Window.GetVisualDescendants().OfType<CodeEditor>()
            .First(item => item.ErrorLine == 3);
        Assert.Contains("oops", editor.Editor.SelectedText, StringComparison.Ordinal);
    });

    [Fact]
    public Task Screenshots_AreWrittenWhenRequested() => UiSession.RunAsync(async () =>
    {
        var directory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
        if (string.IsNullOrWhiteSpace(directory))
        {
            return;
        }

        Directory.CreateDirectory(directory);
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.OpenDeadLettersAsync();
        fixture.ViewModel.NewMessageCommand.Execute(null);
        foreach (var theme in new[] { AppThemePreference.Dark, AppThemePreference.Light })
        {
            fixture.ViewModel.ThemePreference = theme;
            foreach (var page in Pages)
            {
                await fixture.NavigateAsync(page);
                if (page == "Backups" && fixture.ViewModel.FilteredBackupMessages.Count > 0)
                {
                    fixture.ViewModel.SelectedBackup = fixture.ViewModel.FilteredBackupMessages[0];
                    await fixture.SettleAsync();
                }
                using var frame = fixture.Window.CaptureRenderedFrame();
                await using var file = File.Create(
                    Path.Combine(directory, $"{theme.ToString().ToLowerInvariant()}-{page.ToLowerInvariant()}.png"));
                frame?.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
            }
        }
        fixture.ViewModel.ThemePreference = AppThemePreference.Dark;
    });
}
