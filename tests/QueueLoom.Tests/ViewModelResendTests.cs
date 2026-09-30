using QueueLoom.App.Models;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task ResendMarked_CopiesByDefaultAndKeepsTheOriginals()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        viewModel.Messages.Single(message => message.SequenceNumber == 2).IsMarked = true;
        viewModel.Messages.Single(message => message.SequenceNumber == 4).IsMarked = true;
        Assert.Equal("Resend 2 messages…", viewModel.ResendMarkedMessagesLabel);

        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();

        var dialog = Assert.Single(dialogs.ResendDialogs);
        Assert.True(dialog.CanMove);
        Assert.Equal(2, workspace.SentMessages.Count);
        Assert.All(workspace.SentMessages, request => Assert.Equal(ServiceBusEntityReference.Queue("orders"), request.Destination));
        Assert.Empty(workspace.DeleteRequests);
        Assert.Equal(3, viewModel.Messages.Count);
        Assert.Contains("2 of 2 sent", viewModel.StatusText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResendMarked_MoveSendsThenRemovesTheOriginals()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { Mode = ResendMode.Move };
        viewModel.Messages.Single(message => message.SequenceNumber == 2).IsMarked = true;
        viewModel.Messages.Single(message => message.SequenceNumber == 4).IsMarked = true;

        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();

        Assert.Equal(2, workspace.SentMessages.Count);
        Assert.Equal([2L, 4L], Assert.Single(workspace.DeleteRequests).Messages.Select(key => key.SequenceNumber).Order());
        Assert.Equal([3L], viewModel.Messages.Select(message => message.SequenceNumber));
        Assert.Equal(1, Assert.Single(viewModel.DeadLetterSources).Count);
        Assert.Contains("2 originals removed", viewModel.StatusText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResendMarked_SendsNothingWhenTheDialogIsCancelled()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        dialogs.ResendChoice = _ => null;
        viewModel.AreAllMessagesMarked = true;

        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();

        Assert.Empty(workspace.SentMessages);
        Assert.Empty(workspace.DeleteRequests);
    }

    [Fact]
    public async Task ResendMarked_ProductionRequiresTheTypedEnvironmentName()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite, EnvironmentKind.Production);
        await using var _ = viewModel;
        viewModel.AreAllMessagesMarked = true;
        dialogs.ResendChoice = dialog => dialog.CanConfirm ? dialog.ToOptions() : null;

        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();

        Assert.True(Assert.Single(dialogs.ResendDialogs).RequiresTypedConfirmation);
        Assert.Empty(workspace.SentMessages);
    }

    [Fact]
    public async Task Composer_MoveSendsTheDraftAndRemovesTheOriginalOnce()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        dialogs.ConfirmResult = true;
        viewModel.SelectedMessage = viewModel.Messages.Single(message => message.SequenceNumber == 3);
        viewModel.OpenMessageAsDraftCommand.Execute(null);
        Assert.Equal(NavigationPage.Composer, viewModel.CurrentPage);
        Assert.True(viewModel.CanMoveDraftOriginal);
        Assert.False(viewModel.DraftMovesOriginal);

        viewModel.DraftMovesOriginal = true;
        Assert.Equal("Send and remove original", viewModel.SendDraftLabel);
        await viewModel.SendDraftCommand.ExecuteAsync();

        Assert.Single(workspace.SentMessages);
        Assert.Equal([3L], Assert.Single(workspace.DeleteRequests).Messages.Select(key => key.SequenceNumber));
        Assert.DoesNotContain(viewModel.Messages, message => message.SequenceNumber == 3);
        Assert.Contains("remove it from the DLQ", dialogs.Confirmations.Last().Message, StringComparison.Ordinal);
        Assert.False(viewModel.CanMoveDraftOriginal);

        await viewModel.SendDraftCommand.ExecuteAsync();
        Assert.Equal(2, workspace.SentMessages.Count);
        Assert.Single(workspace.DeleteRequests);
    }

    [Fact]
    public async Task Composer_CopyModeKeepsTheOriginal()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        dialogs.ConfirmResult = true;
        viewModel.SelectedMessage = viewModel.Messages.First();
        viewModel.OpenMessageAsDraftCommand.Execute(null);

        await viewModel.SendDraftCommand.ExecuteAsync();

        Assert.Single(workspace.SentMessages);
        Assert.Empty(workspace.DeleteRequests);
        Assert.Contains("original remains in DLQ", viewModel.StatusText, StringComparison.Ordinal);
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task Export_WritesTheTickedMessagesOrAllOfThem()
    {
        var (viewModel, _, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        dialogs.SaveFilePath = Path.Combine(directory.Path, "all.json");
        Assert.Equal("Export all…", viewModel.ExportMessagesLabel);

        await viewModel.ExportMessagesCommand.ExecuteAsync();
        using (var all = System.Text.Json.JsonDocument.Parse(await File.ReadAllTextAsync(dialogs.SaveFilePath)))
        {
            Assert.Equal(3, all.RootElement.GetArrayLength());
        }

        viewModel.Messages.First().IsMarked = true;
        Assert.Equal("Export 1…", viewModel.ExportMessagesLabel);
        dialogs.SaveFilePath = Path.Combine(directory.Path, "one.csv");
        await viewModel.ExportMessagesCommand.ExecuteAsync();
        Assert.Equal(2, (await File.ReadAllLinesAsync(dialogs.SaveFilePath)).Length);

        dialogs.SaveFilePath = null;
        await viewModel.ExportMessagesCommand.ExecuteAsync();
        Assert.Equal("Export cancelled", viewModel.StatusText);
    }
}
