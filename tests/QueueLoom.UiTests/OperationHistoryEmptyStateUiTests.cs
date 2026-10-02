using Avalonia;
using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.Input;
using Avalonia.VisualTree;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

public sealed class OperationHistoryEmptyStateUiTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task FreshAndLegacyActivityExplainEmptyHistoryWithoutAnEmptyPicker(bool legacyActivity) => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync(connect: false, legacyActivity: legacyActivity);
        await fixture.NavigateAsync("Activity");
        Assert.Equal(legacyActivity ? 1 : 0, fixture.ViewModel.Activity.Count);
        Assert.Empty(fixture.OperationStore.List());
        AssertEmpty(fixture);
        var continuing = fixture.ViewModel.ContinueOperationCommand.Completion;
        var retrying = fixture.ViewModel.RetryRejectedOperationCommand.Completion;
        await Refresh(fixture);
        AssertEmpty(fixture);
        Assert.Same(continuing, fixture.ViewModel.ContinueOperationCommand.Completion);
        Assert.Same(retrying, fixture.ViewModel.RetryRejectedOperationCommand.Completion);
        Assert.Empty(fixture.Window.OwnedWindows);
        await Save(fixture, legacyActivity ? "history-empty-with-activity.png" : "history-empty-fresh.png");
        Assert.Empty(BindingErrors.Instance.Messages);
    });

    [Fact]
    public Task PointerRefreshDiscoversPlansAndReturnsToEmptyAfterLastPlanDisappears() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync(connect: false);
        await fixture.NavigateAsync("Activity");
        AssertEmpty(fixture);
        var continuing = fixture.ViewModel.ContinueOperationCommand.Completion;
        var retrying = fixture.ViewModel.RetryRejectedOperationCommand.Completion;
        // Another owner publishes snapshots in this fixture's isolated store.
        var first = await Create(fixture, "first");
        var second = await Create(fixture, "second");
        AssertEmpty(fixture);
        await Refresh(fixture);
        Assert.False(EmptyPanel(fixture).IsEffectivelyVisible);
        var chooser = Chooser(fixture);
        Assert.True(chooser.IsEffectivelyVisible);
        Assert.Equal(2, chooser.Items.Count);
        Assert.Equal(second.Id, fixture.ViewModel.SelectedOperation!.Plan.Id);
        Assert.True(Outcomes(fixture).IsEffectivelyVisible);
        Assert.True(Details(fixture).IsEffectivelyVisible);
        Assert.All(RecoveryButtons(fixture), button => Assert.True(button.IsEffectivelyVisible));
        Click(fixture.Window, chooser);
        await fixture.SettleAsync();
        Assert.True(chooser.IsDropDownOpen);
        var choice = Assert.IsType<ComboBoxItem>(chooser.ContainerFromIndex(1));
        Click(Assert.IsAssignableFrom<TopLevel>(TopLevel.GetTopLevel(choice)), choice);
        await fixture.SettleAsync();
        Assert.Equal(first.Id, fixture.ViewModel.SelectedOperation!.Plan.Id);
        var row = Assert.IsType<ListBoxItem>(Outcomes(fixture).ContainerFromIndex(0));
        Click(fixture.Window, row.GetVisualDescendants().OfType<TextBlock>()
            .Single(text => text.Text == fixture.ViewModel.OperationItems[0].Description));
        await fixture.SettleAsync();
        Assert.Contains(fixture.ViewModel.OperationItems[0].Description, Details(fixture).Text!, StringComparison.Ordinal);
        await Save(fixture, "history-empty-to-populated.png");

        File.Delete(Path.Combine(fixture.OperationStore.RootDirectory, first.Id.ToString("N"), "plan.json"));
        await Refresh(fixture);
        Assert.True(chooser.IsEffectivelyVisible);
        Assert.Single(chooser.Items);
        Assert.Equal(second.Id, fixture.ViewModel.SelectedOperation!.Plan.Id);
        File.Delete(Path.Combine(fixture.OperationStore.RootDirectory, second.Id.ToString("N"), "plan.json"));
        await Refresh(fixture);
        AssertEmpty(fixture);
        Assert.Null(fixture.ViewModel.SelectedOperation);
        Assert.Null(Outcomes(fixture).SelectedItem);
        Assert.Equal(string.Empty, Details(fixture).Text);
        Assert.Same(continuing, fixture.ViewModel.ContinueOperationCommand.Completion);
        Assert.Same(retrying, fixture.ViewModel.RetryRejectedOperationCommand.Completion);
        Assert.Empty(fixture.Window.OwnedWindows);
        await Save(fixture, "history-last-removed.png");
        Assert.Empty(BindingErrors.Instance.Messages);
    });

    private static void AssertEmpty(WindowFixture fixture)
    {
        Assert.Empty(fixture.ViewModel.OperationHistory);
        var panel = EmptyPanel(fixture);
        Assert.True(panel.IsEffectivelyVisible);
        Assert.Contains(panel.GetVisualDescendants().OfType<TextBlock>(), text => text.Text == "No operation history yet");
        var explanation = panel.GetVisualDescendants().OfType<TextBlock>().Single(text => text.Text!.StartsWith("Use Resend", StringComparison.Ordinal)).Text!;
        Assert.Contains("Replay loaded messages", explanation, StringComparison.Ordinal);
        Assert.Contains("Restore filtered backups", explanation, StringComparison.Ordinal);
        Assert.Contains("Scheduled resends appear when they start", explanation, StringComparison.Ordinal);
        Assert.False(Chooser(fixture).IsEffectivelyVisible);
        Assert.False(Outcomes(fixture).IsEffectivelyVisible);
        Assert.False(Details(fixture).IsEffectivelyVisible);
        Assert.All(RecoveryButtons(fixture), button => Assert.False(button.IsEffectivelyVisible));
        Assert.True(RefreshButton(fixture).IsEffectivelyVisible);
        Assert.True(RefreshButton(fixture).IsEffectivelyEnabled);
    }

    private static StackPanel EmptyPanel(WindowFixture fixture) => Assert.IsType<StackPanel>(fixture.Window.GetVisualDescendants()
        .OfType<StackPanel>().SingleOrDefault(panel => panel.Name == "OperationHistoryEmptyState"));
    private static ComboBox Chooser(WindowFixture fixture) => fixture.Window.GetVisualDescendants().OfType<ComboBox>()
        .Single(control => ReferenceEquals(control.ItemsSource, fixture.ViewModel.OperationHistory));
    private static ListBox Outcomes(WindowFixture fixture) => fixture.Window.GetVisualDescendants().OfType<ListBox>()
        .Single(control => ReferenceEquals(control.ItemsSource, fixture.ViewModel.OperationItems));
    private static TextBox Details(WindowFixture fixture) => fixture.Window.GetVisualDescendants().OfType<TextBox>()
        .Single(control => control.Name == "OperationItemDetails");
    private static IEnumerable<Button> RecoveryButtons(WindowFixture fixture) => fixture.Window.GetVisualDescendants().OfType<Button>()
        .Where(button => Equals(button.Content, "Continue unattempted") || Equals(button.Content, "Retry proven failures"));
    private static Button RefreshButton(WindowFixture fixture) => fixture.Window.GetVisualDescendants().OfType<Button>()
        .Single(button => Equals(button.Content, "Refresh operations"));

    private static async Task Refresh(WindowFixture fixture)
    {
        Click(fixture.Window, RefreshButton(fixture));
        await fixture.ViewModel.RefreshOperationHistoryCommand.Completion;
        await fixture.SettleAsync();
    }

    private static void Click(TopLevel root, Control target)
    {
        root.UpdateLayout();
        Assert.True(target.IsEffectivelyVisible);
        var point = target.TranslatePoint(new Point(target.Bounds.Width / 2, target.Bounds.Height / 2), root);
        Assert.NotNull(point);
        root.MouseMove(point.Value);
        root.MouseDown(point.Value, MouseButton.Left);
        root.MouseUp(point.Value, MouseButton.Left);
    }

    private static Task<ReplayPlan> Create(WindowFixture fixture, string label) => fixture.OperationStore.CreateAsync(
        DemoData.Development.Id, ServiceBusEntityReference.Queue("orders"),
        [(new MessageDraft(new EditableMessageBody("isolated test", MessageBodyFormat.Text)), label)], false, 50, default);

    private static async Task Save(WindowFixture fixture, string name)
    {
        var root = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
        if (string.IsNullOrWhiteSpace(root)) return;
        Directory.CreateDirectory(root);
        using var frame = fixture.Window.CaptureRenderedFrame();
        Assert.NotNull(frame);
        await using var file = File.Create(Path.Combine(root, name));
        frame.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
    }
}
