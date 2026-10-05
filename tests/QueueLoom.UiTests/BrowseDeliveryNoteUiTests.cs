using Avalonia.Automation;
using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.VisualTree;
using QueueLoom.App.Views.Pages;
using QueueLoom.Core.Profiles;

namespace QueueLoom.UiTests;

/// <summary>Reading SQS messages raises their receive count; Azure Service Bus peeks. The browse actions say which.</summary>
public sealed class BrowseDeliveryNoteUiTests
{
    [Fact]
    public Task Browse_actions_warn_that_reading_counts_on_SQS_but_not_on_Azure() => UiSession.RunAsync(async () =>
    {
        BindingErrors.Instance.Clear();
        await using var fixture = await WindowFixture.OpenAsync(allClouds: true);
        var viewModel = fixture.ViewModel;

        await ConnectAndScanAsync(fixture, DemoData.AwsStaging.Id);
        Assert.Equal(MessagingProvider.AmazonSqsSns, viewModel.ConnectedProvider);
        await fixture.NavigateAsync("Explorer");
        viewModel.SelectedEntity = viewModel.Entities.First(entity => entity.Name == "orders");
        await fixture.SettleAsync();
        var explorerNote = Note<ExplorerPage>(fixture, "Selected entity delivery note");
        Assert.True(explorerNote.IsEffectivelyVisible);
        Assert.StartsWith("Browsing on Amazon SQS counts as a receive", explorerNote.Text, StringComparison.Ordinal);

        await fixture.NavigateAsync("DeadLetters");
        SelectOrdersSource(fixture, DemoData.AwsStaging.Id);
        await fixture.SettleAsync();
        var messagesNote = Note<MessagesPage>(fixture, "Browse delivery note");
        Assert.True(messagesNote.IsEffectivelyVisible);
        Assert.StartsWith("Browsing on Amazon SQS counts as a receive", messagesNote.Text, StringComparison.Ordinal);
        Assert.True(messagesNote.Bounds.Width > 0 && messagesNote.Bounds.Height > 0);
        fixture.Window.CaptureRenderedFrame();

        // Azure Service Bus peeks without locks: no warning on either page.
        await ConnectAndScanAsync(fixture, DemoData.Development.Id);
        Assert.Equal(MessagingProvider.AzureServiceBus, viewModel.ConnectedProvider);
        SelectOrdersSource(fixture, DemoData.Development.Id);
        await fixture.SettleAsync();
        Assert.False(Note<MessagesPage>(fixture, "Browse delivery note").IsEffectivelyVisible);
        Assert.False(viewModel.HasBrowseDeliveryNote);

        await fixture.NavigateAsync("Explorer");
        viewModel.SelectedEntity = viewModel.Entities.First(entity => entity.Name == "orders");
        await fixture.SettleAsync();
        Assert.False(Note<ExplorerPage>(fixture, "Selected entity delivery note").IsEffectivelyVisible);
        fixture.Window.CaptureRenderedFrame();

        Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages.Distinct()));
    });

    private static async Task ConnectAndScanAsync(WindowFixture fixture, Guid profileId)
    {
        fixture.ViewModel.SelectedProfile = fixture.ViewModel.Profiles.Single(profile => profile.Id == profileId);
        await fixture.ViewModel.ConnectCommand.ExecuteAsync();
        await fixture.ViewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        await fixture.SettleAsync();
    }

    private static void SelectOrdersSource(WindowFixture fixture, Guid profileId)
    {
        fixture.ViewModel.SelectedDeadLetterEnvironmentFilter = fixture.ViewModel.DeadLetterEnvironmentFilters
            .Single(filter => filter.ProfileId == profileId);
        fixture.ViewModel.SelectedDlqSource = fixture.ViewModel.FilteredDeadLetterSources
            .First(source => source.ProfileId == profileId && source.EntityName == "orders");
    }

    private static TextBlock Note<TPage>(WindowFixture fixture, string name) where TPage : Control =>
        fixture.Window.GetVisualDescendants().OfType<TPage>().Single()
            .GetVisualDescendants().OfType<TextBlock>()
            .Single(text => AutomationProperties.GetName(text) == name);
}
