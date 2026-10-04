using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;

namespace QueueLoom.Tests;

/// <summary>
/// The native SNS subject of an unwrapped notification follows intentional edits (Composer, resend find-and-replace),
/// while an unchanged resend publishes it as it was and never adds a Subject attribute the original lacked.
/// </summary>
public sealed partial class ViewModelStateTests
{
    private async Task<(MainWindowViewModel ViewModel, FakeWorkspace Workspace)> OpenSubjectDraftAsync(string? attribute, string native)
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        dialogs.ConfirmResult = true;
        var original = viewModel.Messages.Single(message => message.SequenceNumber == 2).Message;
        var notification = new BrowsedMessage(original.Source, original.SubQueue, 77, original.Body,
            original.Properties with { Subject = attribute, NativeSubject = native });
        viewModel.Messages.Add(new MessageItemViewModel(notification, viewModel.ConnectedProfileId));
        viewModel.SelectedMessage = viewModel.Messages.Last();
        viewModel.OpenMessageAsDraftCommand.Execute(null);
        return (viewModel, workspace);
    }

    [Fact]
    public async Task NativeSubject_FollowsAnEditedSubjectInTheComposer()
    {
        var (viewModel, workspace) = await OpenSubjectDraftAsync(attribute: "old", native: "old");
        await using var _ = viewModel;
        Assert.Equal("old", viewModel.DraftSubject);

        viewModel.DraftSubject = "new";
        await viewModel.SendDraftCommand.ExecuteAsync();

        var sent = Assert.Single(workspace.SentMessages).Message;
        Assert.Equal("new", sent.Properties.Subject);
        Assert.Equal("new", sent.Properties.NativeSubject);
        Assert.Equal("new", AwsMessageMapper.SnsSubject(sent));
    }

    [Fact]
    public async Task NativeOnlySubject_IsShownAndSentUnchangedWithoutAnAttribute()
    {
        var (viewModel, workspace) = await OpenSubjectDraftAsync(attribute: null, native: "Order 7 shipped");
        await using var _ = viewModel;
        Assert.Equal("Order 7 shipped", viewModel.DraftSubject);

        await viewModel.SendDraftCommand.ExecuteAsync();

        var sent = Assert.Single(workspace.SentMessages).Message;
        Assert.Null(sent.Properties.Subject);
        Assert.Equal("Order 7 shipped", sent.Properties.NativeSubject);
        Assert.False(AwsMessageMapper.ToSnsAttributes(sent).ContainsKey(MessageAttributeConventions.Subject));
    }

    [Fact]
    public async Task NativeOnlySubject_EditedInTheComposerStaysNativeOnly()
    {
        var (viewModel, workspace) = await OpenSubjectDraftAsync(attribute: null, native: "Order 7 shipped");
        await using var _ = viewModel;

        viewModel.DraftSubject = "Order 7 delivered";
        await viewModel.SendDraftCommand.ExecuteAsync();

        var sent = Assert.Single(workspace.SentMessages).Message;
        Assert.Null(sent.Properties.Subject);
        Assert.Equal("Order 7 delivered", AwsMessageMapper.SnsSubject(sent));
        Assert.False(AwsMessageMapper.ToSnsAttributes(sent).ContainsKey(MessageAttributeConventions.Subject));
    }
}

public sealed class NativeSubjectRewriteTests
{
    [Fact]
    public void ResendFindAndReplaceAlsoRewritesTheNativeSubject()
    {
        var draft = new MessageDraft(new EditableMessageBody("{}", MessageBodyFormat.Json),
            new EditableMessageProperties(Subject: "order old") { NativeSubject = "order old" });

        var rewritten = new MessageRewrite("old", "new", InBody: false, InProperties: true).Apply(draft);

        Assert.Equal("order new", rewritten.Properties.Subject);
        Assert.Equal("order new", rewritten.Properties.NativeSubject);
        Assert.Equal("order new", AwsMessageMapper.SnsSubject(rewritten));
    }
}
