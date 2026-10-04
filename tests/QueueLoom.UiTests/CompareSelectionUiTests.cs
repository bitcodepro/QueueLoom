using System.Text;
using Avalonia.Automation;
using Avalonia.Controls;
using Avalonia.VisualTree;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.UiTests;

/// <summary>
/// "Tick two and press Compare 2" works for any listed messages; ticking an active message selects it for comparing
/// or exporting only and never enables Delete or Resend.
/// </summary>
public sealed class CompareSelectionUiTests
{
    [Fact]
    public Task TwoActiveMessagesAreTickedAndComparedWithoutOfferingDelete() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.NavigateAsync("DeadLetters");
        var viewModel = fixture.ViewModel;
        var profile = viewModel.Profiles.Single(item => item.Id == viewModel.ConnectedProfileId);
        viewModel.ReplaceMessages([Active(1, "{\"a\":1}", profile), Active(2, "{\"a\":2}", profile)]);
        await fixture.SettleAsync();

        var boxes = RowCheckBoxes(fixture);
        Assert.Equal(2, boxes.Length);
        Assert.All(boxes, box => Assert.True(box.IsVisible && box.IsEnabled));
        foreach (var box in boxes)
        {
            box.IsChecked = true;
        }
        await fixture.SettleAsync();

        Assert.Equal(2, viewModel.MarkedMessageCount);
        var compare = Button(fixture, "Compare 2");
        Assert.True(compare.IsVisible);
        Assert.True(compare.Command?.CanExecute(compare.CommandParameter));
        Assert.False(viewModel.DeleteMarkedMessagesCommand.CanExecute(null));
        Assert.False(viewModel.ResendMarkedMessagesCommand.CanExecute(null));
        Assert.False(viewModel.ShowDeleteMarkedMessages);
        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
    });

    [Fact]
    public Task TwoDeadLettersAreStillComparedAndDeletable() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.OpenDeadLettersAsync();
        var viewModel = fixture.ViewModel;
        await fixture.SettleAsync();

        var boxes = RowCheckBoxes(fixture);
        Assert.True(boxes.Length >= 2);
        boxes[0].IsChecked = true;
        boxes[1].IsChecked = true;
        await fixture.SettleAsync();

        Assert.True(Button(fixture, "Compare 2").IsVisible);
        Assert.Equal(viewModel.CanWrite, viewModel.DeleteMarkedMessagesCommand.CanExecute(null));
        Assert.True(viewModel.ShowDeleteMarkedMessages);
    });

    [Fact]
    public Task AnActiveTickNextToADeadLetterKeepsDeleteAndResendOff() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.OpenDeadLettersAsync();
        var viewModel = fixture.ViewModel;
        var profile = viewModel.Profiles.Single(item => item.Id == viewModel.ConnectedProfileId);
        var dead = viewModel.Messages.First();
        viewModel.ReplaceMessages([dead, Active(900, "{\"a\":3}", profile)]);
        await fixture.SettleAsync();

        foreach (var box in RowCheckBoxes(fixture))
        {
            box.IsChecked = true;
        }
        await fixture.SettleAsync();

        Assert.True(viewModel.CanCompareMarkedMessages);
        Assert.False(viewModel.DeleteMarkedMessagesCommand.CanExecute(null));
        Assert.False(viewModel.ResendMarkedMessagesCommand.CanExecute(null));

        // "Select all" ticks only what can be deleted; clearing it removes every tick.
        viewModel.AreAllMessagesMarked = false;
        viewModel.AreAllMessagesMarked = true;
        await fixture.SettleAsync();
        Assert.Equal(1, viewModel.MarkedMessageCount);
        Assert.True(dead.IsMarked);
        Assert.Equal(viewModel.CanWrite, viewModel.DeleteMarkedMessagesCommand.CanExecute(null));
    });

    private static MessageItemViewModel Active(long sequence, string body, ProfileItemViewModel profile) =>
        new(new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.Active, sequence,
                Encoding.UTF8.GetBytes(body), new EditableMessageProperties(MessageId: $"active-{sequence}"),
                state: ServiceBusMessageState.Active),
            profile.Id, profile.Name);

    private static CheckBox[] RowCheckBoxes(WindowFixture fixture) => fixture.Window.GetVisualDescendants().OfType<CheckBox>()
        .Where(box => AutomationProperties.GetName(box)?.StartsWith("Select message", StringComparison.Ordinal) == true && box.IsVisible)
        .ToArray();

    private static Button Button(WindowFixture fixture, string content) => fixture.Window.GetVisualDescendants().OfType<Button>()
        .Single(button => Equals(button.Content, content));
}
