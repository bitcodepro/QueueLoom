using Avalonia.Automation;
using Avalonia.Controls;
using Avalonia.VisualTree;
using QueueLoom.Core.Settings;

namespace QueueLoom.UiTests;

public sealed class SavedSearchAuditUiTests
{
    [Fact]
    public Task MissingBookmarkPickerClearsExplorerResultsWithAnAlreadyEmptyScope() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.OpenDeadLettersAsync();
        var first = new SavedSearch("Deleted A", "missing-a", Guid.NewGuid());
        var second = new SavedSearch("Deleted B", "missing-b", Guid.NewGuid());
        fixture.ViewModel.SavedSearches.Add(first);
        fixture.ViewModel.SavedSearches.Add(second);
        await fixture.SettleAsync();
        var picker = fixture.Window.GetVisualDescendants().OfType<ComboBox>()
            .Single(box => AutomationProperties.GetName(box) == "Saved searches");
        picker.SelectedItem = first;
        await fixture.SettleAsync();
        Assert.Null(fixture.ViewModel.SelectedDeadLetterEnvironmentFilter);

        fixture.ViewModel.SelectedEntity = fixture.ViewModel.Entities.Single(entity => entity.IsQueue && entity.Name == "orders");
        Assert.True(fixture.ViewModel.BrowseSelectedDeadLettersCommand.CanExecute(null));
        await fixture.ViewModel.BrowseSelectedDeadLettersCommand.ExecuteAsync();
        await fixture.SettleAsync();
        Assert.NotEmpty(fixture.ViewModel.Messages);
        Assert.Null(fixture.ViewModel.SelectedDeadLetterEnvironmentFilter);
        picker.SelectedItem = second;
        await fixture.SettleAsync();

        Assert.Empty(fixture.ViewModel.Messages);
        Assert.Null(fixture.ViewModel.SelectedMessage);
        Assert.Contains("no longer available", fixture.ViewModel.ErrorText, StringComparison.OrdinalIgnoreCase);
        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
    });

    [Fact]
    public Task SavedSearchPickerRendersOneBookmarkWithoutBindingWarnings() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.NavigateAsync("DeadLetters");
        fixture.ViewModel.DeadLetterSearchQuery = "$.region == 'EU'";
        fixture.ViewModel.SaveSearchCommand.Execute(null);
        await fixture.SettleAsync();

        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
    });

    [Fact]
    public Task SaveButtonKeepsBothLongQueriesInTheSavedSearchPicker() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.NavigateAsync("DeadLetters");
        var save = fixture.Window.GetVisualDescendants().OfType<Button>().Single(button => Equals(button.Content, "Save search"));
        var picker = fixture.Window.GetVisualDescendants().OfType<ComboBox>()
            .Single(box => AutomationProperties.GetName(box) == "Saved searches");
        const string first = "$.order.customer.account.identifier == 'customer-1001'";
        const string second = "$.order.customer.account.identifier == 'customer-1002'";

        fixture.ViewModel.DeadLetterSearchQuery = first;
        await fixture.SettleAsync();
        Assert.True(save.IsEnabled);
        Assert.NotNull(save.Command);
        save.Command.Execute(save.CommandParameter);
        await fixture.SettleAsync();
        fixture.ViewModel.DeadLetterSearchQuery = second;
        await fixture.SettleAsync();
        save.Command.Execute(save.CommandParameter);
        await fixture.SettleAsync();

        Assert.Equal(2, picker.ItemCount);
        Assert.Equal([second, first], picker.Items.OfType<SavedSearch>().Select(search => search.Query));
        Assert.Equal(2, picker.Items.OfType<SavedSearch>().Select(search => search.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
    });

    [Fact]
    public Task SelectingADeletedEnvironmentBookmarkClearsTheScopeAndShowsTheError() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync();
        await fixture.OpenDeadLettersAsync();
        await fixture.NavigateAsync("DeadLetters");
        var missing = new SavedSearch("Deleted production", "correlation-42", Guid.NewGuid());
        fixture.ViewModel.SavedSearches.Add(missing);
        await fixture.SettleAsync();
        var picker = fixture.Window.GetVisualDescendants().OfType<ComboBox>()
            .Single(box => AutomationProperties.GetName(box) == "Saved searches");
        var scope = fixture.Window.GetVisualDescendants().OfType<ComboBox>()
            .Single(box => AutomationProperties.GetName(box) == "Filter dead-letter sources by environment");

        picker.SelectedItem = missing;
        await fixture.ViewModel.SearchDeadLettersCommand.Completion;
        await fixture.SettleAsync();

        Assert.Null(scope.SelectedItem);
        Assert.Empty(fixture.ViewModel.Messages);
        Assert.Contains("no longer available", fixture.ViewModel.ErrorText, StringComparison.OrdinalIgnoreCase);
        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
    });
}
