using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

// The AMQP envelope (type, app ID, content encoding, priority) has no field of its own in the Composer: it was taken
// from the original message only. "Send and remove original" clears that reference, so sending the displayed draft
// again published the same gzip body without its encoding and priority. The envelope now outlives the move, and only
// opening or starting another draft replaces it.
public sealed partial class ViewModelStateTests
{
    private static readonly EditableMessageProperties Envelope = new(AmqpType: "order.shipped", AmqpAppId: "warehouse",
        AmqpContentEncoding: "gzip", AmqpPriority: 7);

    private async Task<(MainWindowViewModel ViewModel, FakeWorkspace Workspace)> OpenEnvelopeDraftAsync(EditableMessageProperties envelope)
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        dialogs.ConfirmResult = true;
        var original = viewModel.Messages.Single(message => message.SequenceNumber == 2).Message;
        var message = new BrowsedMessage(original.Source, original.SubQueue, 78, original.Body, original.Properties with
        {
            AmqpType = envelope.AmqpType, AmqpAppId = envelope.AmqpAppId,
            AmqpContentEncoding = envelope.AmqpContentEncoding, AmqpPriority = envelope.AmqpPriority
        });
        viewModel.Messages.Add(new MessageItemViewModel(message, viewModel.ConnectedProfileId));
        viewModel.SelectedMessage = viewModel.Messages.Last();
        viewModel.OpenMessageAsDraftCommand.Execute(null);
        return (viewModel, workspace);
    }

    [Fact]
    public async Task AmqpEnvelope_StaysWhenTheDraftIsSentAgainAfterAMove()
    {
        var (viewModel, workspace) = await OpenEnvelopeDraftAsync(Envelope);
        await using var _ = viewModel;
        viewModel.DraftMessageId = "moved-copy";
        viewModel.DraftMovesOriginal = true;

        await viewModel.SendDraftCommand.ExecuteAsync();
        viewModel.DraftMessageId = "second-copy";
        await viewModel.SendDraftCommand.ExecuteAsync();

        Assert.Single(workspace.DeleteRequests);
        Assert.Equal(2, workspace.SentMessages.Count);
        var first = workspace.SentMessages[0].Message;
        var again = workspace.SentMessages[1].Message;
        Assert.Equal(first.Body.GetBytes(), again.Body.GetBytes());
        foreach (var sent in new[] { first, again })
        {
            AssertEnvelope(sent.Properties, Envelope);
            var wire = RabbitMqMessageMapper.ToAmqp(sent);
            Assert.Equal("gzip", wire.ContentEncoding);
            Assert.Equal(7, wire.Priority);
            Assert.Equal("order.shipped", wire.Type);
            Assert.Equal("warehouse", wire.AppId);
        }
    }

    [Theory]
    [InlineData("new message")]
    [InlineData("another message")]
    public async Task AmqpEnvelope_DoesNotLeakIntoTheNextDraft(string next)
    {
        var (viewModel, workspace) = await OpenEnvelopeDraftAsync(Envelope);
        await using var _ = viewModel;
        viewModel.DraftMessageId = "moved-copy";
        viewModel.DraftMovesOriginal = true;
        await viewModel.SendDraftCommand.ExecuteAsync();

        if (next == "new message")
        {
            viewModel.NewMessageCommand.Execute(null);
            viewModel.SelectedDestination = viewModel.Destinations.First();
        }
        else
        {
            viewModel.SelectedMessage = viewModel.Messages.Single(message => message.SequenceNumber == 2);
            viewModel.OpenMessageAsDraftCommand.Execute(null);
        }
        viewModel.DraftMessageId = "next";
        viewModel.DraftMovesOriginal = false;
        await viewModel.SendDraftCommand.ExecuteAsync();

        AssertEnvelope(workspace.SentMessages.Last().Message.Properties, EditableMessageProperties.Empty);
    }

    private static void AssertEnvelope(EditableMessageProperties actual, EditableMessageProperties expected)
    {
        Assert.Equal(expected.AmqpType, actual.AmqpType);
        Assert.Equal(expected.AmqpAppId, actual.AmqpAppId);
        Assert.Equal(expected.AmqpContentEncoding, actual.AmqpContentEncoding);
        Assert.Equal(expected.AmqpPriority, actual.AmqpPriority);
    }
}
