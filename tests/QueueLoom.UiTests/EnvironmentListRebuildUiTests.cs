using System.Reflection;
using Avalonia.Controls;
using Avalonia.VisualTree;

namespace QueueLoom.UiTests;

/// <summary>
/// Adding, editing or selecting an environment rebuilds the bound environment lists. The real ComboBoxes write a
/// transient null selection while their collection is cleared; that must not clear the connected environment's
/// results, ticks or search scope.
/// </summary>
public sealed class EnvironmentListRebuildUiTests
{
    [Fact]
    public Task ReloadingTheEnvironmentListKeepsTheConnectedResultsAndTicks() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.OpenDeadLettersAsync();
        var viewModel = fixture.ViewModel;
        var count = viewModel.Messages.Count;
        Assert.True(count > 1);
        var scope = viewModel.SelectedDeadLetterEnvironmentFilter?.ProfileId;
        Assert.NotNull(scope);
        viewModel.Messages[0].IsMarked = true;
        await fixture.SettleAsync();
        Assert.NotEmpty(fixture.Window.GetVisualDescendants().OfType<ComboBox>());

        // What Add / Edit / Import environment do after saving: reload the list behind the bound ComboBoxes.
        var reload = typeof(QueueLoom.App.ViewModels.MainWindowViewModel)
            .GetMethod("ReloadProfilesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task)reload.Invoke(viewModel, [CancellationToken.None, null])!;
        await fixture.SettleAsync();

        Assert.Equal(count, viewModel.Messages.Count);
        Assert.Equal(1, viewModel.MarkedMessageCount);
        Assert.Equal(scope, viewModel.SelectedDeadLetterEnvironmentFilter?.ProfileId);
    });

    [Fact]
    public Task SelectingAnotherEnvironmentInTheListKeepsTheConnectedResultsAndTicks() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.OpenDeadLettersAsync();
        var viewModel = fixture.ViewModel;
        var count = viewModel.Messages.Count;
        var scope = viewModel.SelectedDeadLetterEnvironmentFilter?.ProfileId;
        viewModel.Messages[0].IsMarked = true;
        await fixture.SettleAsync();

        viewModel.SelectedProfile = viewModel.Profiles.First(profile => profile.Id != viewModel.ConnectedProfileId);
        await fixture.SettleAsync();

        Assert.Equal(count, viewModel.Messages.Count);
        Assert.Equal(1, viewModel.MarkedMessageCount);
        Assert.Equal(scope, viewModel.SelectedDeadLetterEnvironmentFilter?.ProfileId);
    });
}
