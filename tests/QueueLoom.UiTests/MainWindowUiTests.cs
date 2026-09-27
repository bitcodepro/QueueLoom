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
using QueueLoom.App.Views;
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
    public Task ToneForeground_WinsOverTheEyebrowClassColour() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        fixture.ViewModel.ThemePreference = AppThemePreference.Dark;
        await fixture.SettleAsync();

        var writeLabel = fixture.Window.GetVisualDescendants().OfType<TextBlock>()
            .Single(text => text.Text == "WRITE ENABLED" && text.Classes.Contains("eyebrow"));

        Assert.Equal(Color.Parse("#FFB45E"), ((ISolidColorBrush)writeLabel.Foreground!).Color);
    });

    [Fact]
    public Task MessageCheckboxes_DriveTheDeleteSelectedButton() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.OpenDeadLettersAsync();
        await fixture.NavigateAsync("DeadLetters");

        var rowBoxes = fixture.Window.GetVisualDescendants().OfType<CheckBox>()
            .Where(box => box.DataContext is MessageItemViewModel)
            .ToArray();
        Assert.Equal(fixture.ViewModel.Messages.Count, rowBoxes.Length);

        rowBoxes[0].IsChecked = true;
        await fixture.SettleAsync();

        var delete = fixture.Window.GetVisualDescendants().OfType<Button>()
            .Single(button => ReferenceEquals(button.Command, fixture.ViewModel.DeleteMarkedMessagesCommand));
        Assert.Equal("Delete 1 message…", delete.Content);
        Assert.True(delete.IsEffectivelyEnabled);
        Assert.True(fixture.ViewModel.Messages[0].IsMarked);

        var selectAll = fixture.Window.GetVisualDescendants().OfType<CheckBox>()
            .Single(box => box.DataContext is MainWindowViewModel);
        Assert.Null(selectAll.IsChecked);
        selectAll.IsChecked = true;
        await fixture.SettleAsync();
        Assert.All(fixture.ViewModel.Messages, message => Assert.True(message.IsMarked));
    });

    [Fact]
    public Task DesktopApproval_ProductionNeedsTheTypedNameAndDenyRejects() => UiSession.RunAsync(async () =>
    {
        var approver = new QueueLoom.App.Mcp.DesktopApprover();
        ConfirmDialogWindow? window = null;
        approver.WindowOpened += opened => window = opened;
        var request = new QueueLoom.Mcp.ApprovalRequest("Delete dead-letter messages", "Orders", IsProduction: true, "2 messages");

        var approval = approver.RequestAsync(request, null!, CancellationToken.None);
        await SettleAsync();
        Assert.NotNull(window);
        var approve = window!.GetVisualDescendants().OfType<Button>().Single(button => button.IsEffectivelyVisible && Equals(button.Content, "Approve"));
        Assert.False(approve.IsEffectivelyEnabled);
        ((ConfirmDialogViewModel)window.DataContext!).ConfirmationText = "Orders";
        await SettleAsync();
        Assert.True(approve.IsEffectivelyEnabled);
        approve.RaiseEvent(new Avalonia.Interactivity.RoutedEventArgs(Button.ClickEvent));
        Assert.True((await approval).Approved);

        window = null;
        var denial = approver.RequestAsync(request with { IsProduction = false }, null!, CancellationToken.None);
        await SettleAsync();
        window!.GetVisualDescendants().OfType<Button>().Single(button => Equals(button.Content, "Deny"))
            .RaiseEvent(new Avalonia.Interactivity.RoutedEventArgs(Button.ClickEvent));
        Assert.False((await denial).Approved);
    });

    [Fact]
    public Task DesktopApproval_TimesOutAsDenied() => UiSession.RunAsync(async () =>
    {
        var approver = new QueueLoom.App.Mcp.DesktopApprover(TimeSpan.FromMilliseconds(200));
        var decision = approver.RequestAsync(
            new QueueLoom.Mcp.ApprovalRequest("Send a message", "Dev", false, "1 message"), null!, CancellationToken.None);
        while (!decision.IsCompleted)
        {
            await SettleAsync();
        }

        Assert.False((await decision).Approved);
        Assert.Contains("Nobody approved", (await decision).Reason, StringComparison.Ordinal);
    });

    private static async Task SettleAsync()
    {
        for (var i = 0; i < 5; i++)
        {
            Avalonia.Threading.Dispatcher.UIThread.RunJobs();
            await Task.Delay(20);
        }
    }

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
    public Task Toasts_AreShownInTheWindow() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync(connect: false);
        var toasts = new QueueLoom.App.Services.WindowNotificationService(
            new QueueLoom.App.Services.TopLevelAccessor { Current = fixture.Window });

        toasts.Show("Copied to clipboard", "orders", QueueLoom.App.Services.NotificationTone.Success);
        await fixture.SettleAsync();
        fixture.Window.UpdateLayout();

        var card = fixture.Window.GetVisualDescendants().OfType<Avalonia.Controls.Notifications.NotificationCard>().Single();
        Assert.True(card.IsVisible);
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
        fixture.ViewModel.Messages[0].IsMarked = true;
        fixture.ViewModel.Messages[1].IsMarked = true;
        fixture.ViewModel.DeadLetterSearchQuery = "order";
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

        // The window an MCP client's change request opens.
        var approver = new QueueLoom.App.Mcp.DesktopApprover();
        ConfirmDialogWindow? approval = null;
        approver.WindowOpened += opened => approval = opened;
        var pending = approver.RequestAsync(new QueueLoom.Mcp.ApprovalRequest(
            "Delete dead-letter messages",
            "Local emulator",
            false,
            "Requested by: Claude Desktop\nEnvironment: Local emulator (Development)\nNamespace: localhost\n\n" +
            "Reason given: These two orders failed validation after the 18:00 deployment and were replayed manually.\n\n" +
            "2 dead-lettered message(s) will be backed up locally and then permanently deleted. Other messages stay in the queue.\n\n" +
            "• orders (dlq): 2\n\nFirst messages:\n  #101 order-1001\n  #102 order-1002"), null!, CancellationToken.None);
        await fixture.SettleAsync();
        approval!.Width = 640;
        await fixture.SettleAsync();
        using (var frame = approval.CaptureRenderedFrame())
        await using (var file = File.Create(Path.Combine(directory, "mcp-approval.png")))
        {
            frame?.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
        }
        approval.Close();
        await pending;
    });
}
