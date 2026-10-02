using Avalonia;
using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.Input;
using Avalonia.VisualTree;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.UiTests;

public sealed class OperationHistorySelectionUiTests
{
    [Theory]
    [InlineData("Pending", true)]
    [InlineData("Rejected", true)]
    [InlineData("Uncertain", false)]
    [InlineData("Sent", false)]
    [InlineData("Moved", false)]
    [InlineData("DeleteUncertain", false)]
    [InlineData("AwaitingScheduleClaim", false)]
    public Task PointerSelectsOperationAndRowsForInspectionWithoutRecovery(string state, bool canMark) => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        var first = await Create(fixture, "first", [state, state, state]);
        var second = await Create(fixture, "second", ["Pending"]);
        await fixture.ViewModel.RefreshOperationHistoryCommand.ExecuteAsync();
        await fixture.NavigateAsync("Activity");
        var continuing = fixture.ViewModel.ContinueOperationCommand.Completion;
        var retrying = fixture.ViewModel.RetryRejectedOperationCommand.Completion;

        // Open the real popup and choose a different operation using pointer input.
        await Choose(fixture, first);
        Assert.Equal(first.Id, fixture.ViewModel.SelectedOperation!.Plan.Id);
        Assert.Equal(3, fixture.ViewModel.OperationItems.Count);

        var list = Outcomes(fixture);
        for (var index = 0; index < 3; index++)
        {
            list.ScrollIntoView(index);
            await fixture.SettleAsync();
            var row = Assert.IsType<ListBoxItem>(list.ContainerFromIndex(index));
            Assert.True(row.IsEffectivelyEnabled);
            var text = row.GetVisualDescendants().OfType<TextBlock>()
                .Single(t => t.Text == fixture.ViewModel.OperationItems[index].Description);
            Click(fixture.Window, text);
            await fixture.SettleAsync();
            Assert.Same(fixture.ViewModel.OperationItems[index], list.SelectedItem);
            AssertDetails(fixture, fixture.ViewModel.OperationItems[index]);
            var check = row.GetVisualDescendants().OfType<CheckBox>().Single();
            Assert.Equal(canMark, check.IsEnabled);
            Assert.False(fixture.ViewModel.OperationItems[index].IsMarked);
        }
        // Keyboard selection must reach the same detail view and must not mark a retry.
        list.Focus();
        fixture.Window.KeyPressQwerty(PhysicalKey.ArrowUp, RawInputModifiers.None);
        fixture.Window.KeyReleaseQwerty(PhysicalKey.ArrowUp, RawInputModifiers.None);
        await fixture.SettleAsync();
        Assert.Same(fixture.ViewModel.OperationItems[1], list.SelectedItem);
        AssertDetails(fixture, fixture.ViewModel.OperationItems[1]);
        Assert.All(fixture.ViewModel.OperationItems, item => Assert.False(item.IsMarked));
        var checkbox = Assert.IsType<ListBoxItem>(list.ContainerFromIndex(1))
            .GetVisualDescendants().OfType<CheckBox>().Single();
        Click(fixture.Window, checkbox);
        await fixture.SettleAsync();
        Assert.Equal(canMark, fixture.ViewModel.OperationItems[1].IsMarked);
        Assert.Same(continuing, fixture.ViewModel.ContinueOperationCommand.Completion);
        Assert.Same(retrying, fixture.ViewModel.RetryRejectedOperationCommand.Completion);
        Assert.Empty(fixture.Window.OwnedWindows);
        Assert.Equal([state, state, state], fixture.OperationStore.ReadHistory(first).Items.Select(i => i.State));
        Assert.Equal("Pending", Assert.Single(fixture.OperationStore.ReadHistory(second).Items).State);
        await Save(fixture.Window, $"operation-inspection-{state}.png");
        await Choose(fixture, second);
        Assert.Null(list.SelectedItem);
        Assert.Equal(string.Empty, Details(fixture).Text);
        Assert.Empty(BindingErrors.Instance.Messages);
    });

    [Fact]
    public Task PointerInspectionWorksWhileRecoveryCommandsAreDisabled() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync(connect: false);
        var plan = await Create(fixture, "offline", ["Sent", "Uncertain", "Pending"]);
        await fixture.ViewModel.RefreshOperationHistoryCommand.ExecuteAsync();
        await fixture.NavigateAsync("Activity");
        await Choose(fixture, plan);
        Assert.False(fixture.ViewModel.CanWrite);
        foreach (var label in new[] { "Continue unattempted", "Retry proven failures" })
            Assert.False(fixture.Window.GetVisualDescendants().OfType<Button>().Single(b => Equals(b.Content, label)).IsEffectivelyEnabled);
        var list = Outcomes(fixture);
        var row = Assert.IsType<ListBoxItem>(list.ContainerFromIndex(0));
        Click(fixture.Window, row.GetVisualDescendants().OfType<TextBlock>()
            .Single(t => t.Text == fixture.ViewModel.OperationItems[0].Description));
        await fixture.SettleAsync();
        Assert.Same(fixture.ViewModel.OperationItems[0], list.SelectedItem);
        AssertDetails(fixture, fixture.ViewModel.OperationItems[0]);
        Assert.All(fixture.ViewModel.OperationItems, item => Assert.False(item.IsMarked));
        Assert.Empty(fixture.Window.OwnedWindows);
        Assert.Empty(BindingErrors.Instance.Messages);
    });

    [Fact]
    public Task PointerRefreshPreservesInspectionUpdatesOutcomeAndClearsStaleSelection() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        var plan = await Create(fixture, "refresh", ["Pending", "Pending", "Uncertain"]);
        var other = await Create(fixture, "other", ["Sent"]);
        await fixture.ViewModel.RefreshOperationHistoryCommand.ExecuteAsync();
        await fixture.NavigateAsync("Activity");
        await Choose(fixture, plan);
        var list = Outcomes(fixture);
        list.ScrollIntoView(1);
        await fixture.SettleAsync();
        var row = Assert.IsType<ListBoxItem>(list.ContainerFromIndex(1));
        Click(fixture.Window, row.GetVisualDescendants().OfType<TextBlock>()
            .Single(t => t.Text == fixture.ViewModel.OperationItems[1].Description));
        await fixture.SettleAsync();
        AssertDetails(fixture, fixture.ViewModel.OperationItems[1]);
        Click(fixture.Window, row.GetVisualDescendants().OfType<CheckBox>().Single());
        await fixture.SettleAsync();
        Assert.True(fixture.ViewModel.OperationItems[1].IsMarked);
        var previous = list.SelectedItem;
        var directory = Path.Combine(fixture.OperationStore.RootDirectory, plan.Id.ToString("N"));
        await File.WriteAllTextAsync(Path.Combine(directory, "000001.state"), "Sent");
        await File.WriteAllTextAsync(Path.Combine(directory, "000001.detail"), "Refreshed completed outcome");
        await Refresh(fixture);
        Assert.Equal(plan.Id, fixture.ViewModel.SelectedOperation!.Plan.Id);
        Assert.NotSame(previous, list.SelectedItem);
        Assert.Same(fixture.ViewModel.OperationItems[1], list.SelectedItem);
        AssertDetails(fixture, fixture.ViewModel.OperationItems[1]);
        Assert.Contains("Refreshed completed outcome", Details(fixture).Text!, StringComparison.Ordinal);
        Assert.All(fixture.ViewModel.OperationItems, item => Assert.False(item.IsMarked));

        // A retained operation whose selected item vanished must lose its detail selection.
        await File.WriteAllTextAsync(Path.Combine(directory, "plan.json"),
            System.Text.Json.JsonSerializer.Serialize(plan with { Count = 1 }));
        await Refresh(fixture);
        Assert.Equal(plan.Id, fixture.ViewModel.SelectedOperation!.Plan.Id);
        Assert.Single(fixture.ViewModel.OperationItems);
        Assert.Null(list.SelectedItem);
        Assert.Equal(string.Empty, Details(fixture).Text);

        File.Delete(Path.Combine(directory, "plan.json"));
        await Refresh(fixture);
        Assert.Equal(other.Id, fixture.ViewModel.SelectedOperation!.Plan.Id);
        Assert.Null(list.SelectedItem);
        Assert.Equal(string.Empty, Details(fixture).Text);
        File.Delete(Path.Combine(fixture.OperationStore.RootDirectory, other.Id.ToString("N"), "plan.json"));
        await Refresh(fixture);
        Assert.Null(fixture.ViewModel.SelectedOperation);
        Assert.Empty(fixture.ViewModel.OperationItems);
        Assert.Null(list.SelectedItem);
        Assert.Equal(string.Empty, Details(fixture).Text);
        Assert.Empty(fixture.Window.OwnedWindows);
        Assert.Empty(BindingErrors.Instance.Messages);
    });

    private static async Task Refresh(WindowFixture fixture)
    {
        Click(fixture.Window, fixture.Window.GetVisualDescendants().OfType<Button>()
            .Single(button => Equals(button.Content, "Refresh operations")));
        await fixture.ViewModel.RefreshOperationHistoryCommand.Completion;
        await fixture.SettleAsync();
    }

    private static async Task Choose(WindowFixture fixture, ReplayPlan plan)
    {
        var chooser = fixture.Window.GetVisualDescendants().OfType<ComboBox>()
            .Single(c => ReferenceEquals(c.ItemsSource, fixture.ViewModel.OperationHistory));
        Click(fixture.Window, chooser);
        await fixture.SettleAsync();
        Assert.True(chooser.IsDropDownOpen);
        var choice = Assert.IsType<ComboBoxItem>(chooser.ContainerFromIndex(
            fixture.ViewModel.OperationHistory.IndexOf(fixture.ViewModel.OperationHistory.Single(operation => operation.Plan.Id == plan.Id))));
        Click(Assert.IsAssignableFrom<TopLevel>(TopLevel.GetTopLevel(choice)), choice);
        await fixture.SettleAsync();
        Assert.False(chooser.IsDropDownOpen);
        Assert.Equal(plan.Id, fixture.ViewModel.SelectedOperation!.Plan.Id);
    }

    private static ListBox Outcomes(WindowFixture fixture) => fixture.Window.GetVisualDescendants().OfType<ListBox>()
        .Single(list => ReferenceEquals(list.ItemsSource, fixture.ViewModel.OperationItems));

    private static void AssertDetails(WindowFixture fixture, OperationItemViewModel item)
    {
        var details = Details(fixture);
        Assert.NotNull(details);
        Assert.True(details.IsEffectivelyVisible);
        Assert.True(details.IsReadOnly);
        Assert.Contains(item.Description, details.Text!, StringComparison.Ordinal);
        Assert.Contains(item.Outcome, details.Text!, StringComparison.Ordinal);
    }

    private static TextBox Details(WindowFixture fixture) => Assert.IsType<TextBox>(fixture.Window.GetVisualDescendants()
        .OfType<TextBox>().SingleOrDefault(box => box.Name == "OperationItemDetails"));

    private static void Click(TopLevel root, Control target)
    {
        root.UpdateLayout();
        Assert.True(target.IsEffectivelyVisible);
        Assert.True(target.Bounds.Width > 0 && target.Bounds.Height > 0);
        var point = target.TranslatePoint(new Point(target.Bounds.Width / 2, target.Bounds.Height / 2), root);
        Assert.NotNull(point);
        root.MouseMove(point.Value);
        root.MouseDown(point.Value, MouseButton.Left);
        root.MouseUp(point.Value, MouseButton.Left);
    }

    private static async Task<ReplayPlan> Create(WindowFixture fixture, string prefix, string[] states)
    {
        var profile = DemoData.Development;
        var plan = await fixture.OperationStore.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("orders"),
            states.Select((_, i) => (new MessageDraft(new EditableMessageBody("offline test", MessageBodyFormat.Text),
                new EditableMessageProperties(MessageId: $"{prefix}-{i}")), $"{prefix} source item {i}")),
            true, 50, default, profile.EndpointDisplay, ScheduledResend.IdentityFor(profile));
        var directory = Path.Combine(fixture.OperationStore.RootDirectory, plan.Id.ToString("N"));
        for (var index = 0; index < states.Length; index++)
        {
            await File.WriteAllTextAsync(Path.Combine(directory, $"{index:D6}.state"), states[index]);
            await File.WriteAllTextAsync(Path.Combine(directory, $"{index:D6}.detail"),
                $"Full outcome for {prefix}-{index}: " + new string('x', 300) + " END OF DETAIL");
        }
        return plan;
    }

    private static async Task Save(Window window, string name)
    {
        var directory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
        if (string.IsNullOrWhiteSpace(directory)) return;
        using var frame = window.CaptureRenderedFrame();
        Assert.NotNull(frame);
        Directory.CreateDirectory(directory);
        await using var output = File.Create(Path.Combine(directory, name));
        frame.Save(output, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
    }
}
