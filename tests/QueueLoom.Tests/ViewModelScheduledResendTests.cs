using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task ResendMarked_AppliesFindAndReplaceToWhatIsSent()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        var marked = viewModel.Messages.Single(message => message.SequenceNumber == 2);
        marked.IsMarked = true;
        var original = marked.Message.CreateDraft().Body.Content;
        var find = original[..Math.Min(3, original.Length)];
        dialogs.ResendChoice = dialog =>
        {
            dialog.FindText = find;
            dialog.ReplaceText = "XYZ";
            Assert.StartsWith("Changes 1 of 1 messages.", dialog.RewritePreview, StringComparison.Ordinal);
            return dialog.ToOptions();
        };

        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();

        Assert.Equal(original.Replace(find, "XYZ", StringComparison.Ordinal), Assert.Single(workspace.SentMessages).Message.Body.Content);
    }

    [Fact]
    public async Task ScheduledResend_WaitsForItsTimeThenSendsOnce()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        var clock = new ManualClock(new DateTimeOffset(2026, 9, 30, 10, 0, 0, TimeSpan.Zero));
        viewModel.Clock = clock;
        viewModel.Messages.Single(message => message.SequenceNumber == 2).IsMarked = true;
        viewModel.Messages.Single(message => message.SequenceNumber == 4).IsMarked = true;
        dialogs.ResendChoice = dialog =>
        {
            dialog.SendLater = true;
            dialog.SendAtText = "30m";
            Assert.True(dialog.CanConfirm);
            Assert.Equal("Schedule copies", dialog.ConfirmLabel);
            return dialog.ToOptions() with { Mode = ResendMode.Move };
        };

        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();

        Assert.Empty(workspace.SentMessages);
        var scheduled = Assert.Single(viewModel.ScheduledResends);
        Assert.Equal(clock.GetUtcNow().AddMinutes(30), scheduled.Resend.DueAt);
        Assert.StartsWith("Waits 30 min", scheduled.Status, StringComparison.Ordinal);

        await viewModel.RunDueScheduledResendsAsync();
        Assert.Empty(workspace.SentMessages);

        clock.Now = clock.Now.AddMinutes(31);
        await viewModel.RunDueScheduledResendsAsync();
        Assert.Equal(2, workspace.SentMessages.Count);
        Assert.Equal([2L, 4L], Assert.Single(workspace.DeleteRequests).Messages.Select(key => key.SequenceNumber).Order());
        Assert.Empty(viewModel.ScheduledResends);

        await viewModel.RunDueScheduledResendsAsync();
        Assert.Equal(2, workspace.SentMessages.Count);
    }

    [Fact]
    public async Task ScheduledResend_CanBeCancelled()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        viewModel.AreAllMessagesMarked = true;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();

        viewModel.CancelScheduledResendCommand.Execute(Assert.Single(viewModel.ScheduledResends));
        await viewModel.RunDueScheduledResendsAsync();

        Assert.Empty(viewModel.ScheduledResends);
        Assert.Empty(workspace.SentMessages);
    }

    [Theory]
    [InlineData("30m", 30)]
    [InlineData("2h", 120)]
    [InlineData("11:30", 90)]
    [InlineData("09:00", 23 * 60)]
    public void ResendDialog_UnderstandsDelaysAndClockTimes(string text, int minutes)
    {
        var now = new DateTimeOffset(2026, 9, 30, 10, 0, 0, TimeZoneInfo.Local.GetUtcOffset(new DateTime(2026, 9, 30, 10, 0, 0)));

        Assert.Equal(now.AddMinutes(minutes), ResendDialogViewModel.ParseWhen(text, now));
    }

    [Fact]
    public void ResendDialog_RejectsTimesInThePastOrTooFarAhead()
    {
        var now = DateTimeOffset.Now;
        Assert.Null(ResendDialogViewModel.ParseWhen("0m", now));
        Assert.Null(ResendDialogViewModel.ParseWhen("40d", now));
        Assert.Null(ResendDialogViewModel.ParseWhen("2020-01-01 10:00", now));
        Assert.Null(ResendDialogViewModel.ParseWhen("soon", now));
    }

    [Fact]
    public void ScheduledResendStore_KeepsJobsAcrossRestarts()
    {
        using var directory = new TemporaryTestDirectory();
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        var draft = new MessageDraft(new EditableMessageBody("{\"id\":1}", MessageBodyFormat.Json),
            new EditableMessageProperties(MessageId: "m-1", Subject: "order"),
            [new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "eu")]);
        var resend = new ScheduledResend(Guid.NewGuid(), Guid.NewGuid(), "Staging", DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddHours(1),
            ResendMode.Move, 10, "orders",
            [new ScheduledResendItem(ServiceBusEntityReference.Subscription("events", "billing"), ServiceBusSubQueue.DeadLetter, 7, "m-1",
                ServiceBusEntityReference.Topic("events"), draft)]);

        store.Save([resend]);
        var loaded = Assert.Single(new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path)).Load());

        Assert.Equal(resend with { Items = [] }, loaded with { Items = [] });
        var item = Assert.Single(loaded.Items);
        Assert.Equal(resend.Items[0] with { Message = draft }, item with { Message = draft });
        Assert.Equal(draft.Body, item.Message.Body);
        Assert.Equal(draft.Properties, item.Message.Properties);
        Assert.Equal(draft.ApplicationProperties, item.Message.ApplicationProperties);
        var key = item.ToResendItem().Key;
        Assert.Equal((ServiceBusSubQueue.DeadLetter, 7L, "m-1"), (key.SubQueue, key.SequenceNumber, key.MessageId));
        store.Save([]);
        Assert.Empty(store.Load());
    }

    [Fact]
    public void MessageRewrite_ChangesTextBodiesAndTextPropertiesOnly()
    {
        var draft = new MessageDraft(new EditableMessageBody("{\"env\":\"STAGING\"}", MessageBodyFormat.Json),
            new EditableMessageProperties(Subject: "staging order", CorrelationId: "c-staging"),
            [new MessageApplicationProperty("env", ApplicationPropertyType.String, "staging"), new MessageApplicationProperty("n", ApplicationPropertyType.Int32, "1")]);

        var both = new MessageRewrite("staging", "prod", InProperties: true, MatchCase: false).Apply(draft, out var changed);
        Assert.True(changed);
        Assert.Equal("{\"env\":\"prod\"}", both.Body.Content);
        Assert.Equal(("prod order", "c-prod"), (both.Properties.Subject, both.Properties.CorrelationId));
        Assert.Equal("prod", both.ApplicationProperties[0].Value);

        var bodyOnly = new MessageRewrite("staging", "prod").Apply(draft, out changed);
        Assert.False(changed);
        Assert.Same(draft, bodyOnly);

        var binary = new MessageDraft(new EditableMessageBody(Convert.ToBase64String([0xFF, 0x01]), MessageBodyFormat.Base64));
        Assert.Same(binary, new MessageRewrite("/w", "x").Apply(binary));

        var broken = new MessageRewrite("\"}", string.Empty, MatchCase: false).Apply(draft);
        Assert.True(MessageRewrite.BreaksJson(draft, broken));
    }

    private sealed class ManualClock(DateTimeOffset now) : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = now;

        public override DateTimeOffset GetUtcNow() => Now;
    }

    private sealed class TemporaryTestDirectory : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));

        public void Dispose()
        {
            if (Directory.Exists(Path))
            {
                Directory.Delete(Path, recursive: true);
            }
        }
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task CompareMarked_IsOfferedForExactlyTwoMessages()
    {
        var (viewModel, _, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        viewModel.Messages[0].IsMarked = true;
        Assert.False(viewModel.CompareMarkedMessagesCommand.CanExecute(null));
        viewModel.Messages[1].IsMarked = true;
        Assert.True(viewModel.CanCompareMarkedMessages);

        await viewModel.CompareMarkedMessagesCommand.ExecuteAsync();

        var comparison = Assert.Single(dialogs.Comparisons);
        Assert.Contains(viewModel.Messages[0].MessageId, comparison.LeftTitle, StringComparison.Ordinal);
        viewModel.Messages[2].IsMarked = true;
        Assert.False(viewModel.CompareMarkedMessagesCommand.CanExecute(null));
    }
}
