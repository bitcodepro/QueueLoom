using Confluent.Kafka;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("null", true)]
    [InlineData("null", false)]
    [InlineData("empty", true)]
    [InlineData("empty", false)]
    [InlineData("space", true)]
    [InlineData("space", false)]
    [InlineData("tab", true)]
    [InlineData("tab", false)]
    [InlineData("binary", true)]
    [InlineData("binary", false)]
    public async Task KafkaComposer_UneditedDraftPreservesRawKeySubjectAndNullValue(string kind, bool tombstone)
    {
        var original = KafkaComposerFixture(kind, tombstone);
        var replay = await ReplayKafkaThroughComposer(original, clear: false);
        Assert.Equal(original.Key, replay.Key);
        Assert.Equal(original.Value, replay.Value);
        var subject = Assert.Single(replay.Headers, header => header.Key == "Subject");
        Assert.Equal(original.Headers[0].GetValueBytes(), subject.GetValueBytes());
    }

    [Theory]
    [InlineData("space", true)]
    [InlineData("space", false)]
    [InlineData("tab", true)]
    [InlineData("tab", false)]
    [InlineData("binary", true)]
    [InlineData("binary", false)]
    public async Task KafkaComposer_ExplicitClearRemovesRawKeyAndSubject(string kind, bool tombstone)
    {
        var original = KafkaComposerFixture(kind, tombstone);
        var replay = await ReplayKafkaThroughComposer(original, clear: true);
        Assert.Null(replay.Key);
        Assert.DoesNotContain(replay.Headers, header => header.Key == "Subject");
        Assert.Equal(original.Value, replay.Value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task KafkaComposer_UneditedWhitespaceSubjectSurvivesWithNullKey(bool tombstone)
    {
        var original = KafkaComposerFixture("space", tombstone);
        original.Key = null;
        var replay = await ReplayKafkaThroughComposer(original, clear: false);
        Assert.Null(replay.Key);
        Assert.Equal(original.Headers[0].GetValueBytes(), Assert.Single(replay.Headers, header => header.Key == "Subject").GetValueBytes());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task KafkaComposer_ExplicitWhitespaceEditsReplaceOriginalMetadata(bool tombstone)
    {
        var original = KafkaComposerFixture("space", tombstone);
        var replay = await ReplayKafkaThroughComposer(original, clear: false, editWhitespace: true);
        Assert.Equal(new byte[] { 9 }, replay.Key);
        Assert.Equal(new byte[] { 9 }, Assert.Single(replay.Headers, header => header.Key == "Subject").GetValueBytes());
        Assert.Equal(original.Value, replay.Value);
    }
    [Theory]
    [InlineData("space", true)]
    [InlineData("space", false)]
    [InlineData("tab", true)]
    [InlineData("tab", false)]
    public async Task KafkaComposer_UneditedWhitespaceMessageIdRemainsRaw(string kind, bool tombstone)
    {
        var original = KafkaComposerMessageIdFixture(kind, tombstone);
        var replay = await ReplayKafkaThroughComposer(original, clear: false);
        Assert.Equal(original.Headers[0].GetValueBytes(), Assert.Single(replay.Headers, header => header.Key == "MessageId").GetValueBytes());
        Assert.Equal(original.Value, replay.Value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task KafkaComposer_ExplicitWhitespaceMessageIdEditRemainsLiteral(bool tombstone)
    {
        var replay = await ReplayKafkaThroughComposer(KafkaComposerMessageIdFixture("space", tombstone),
            clear: false, messageIdEdit: "\t");
        Assert.Equal(new byte[] { 9 }, Assert.Single(replay.Headers, header => header.Key == "MessageId").GetValueBytes());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task KafkaComposer_ExplicitMessageIdClearCreatesNewId(bool tombstone)
    {
        var replay = await ReplayKafkaThroughComposer(KafkaComposerMessageIdFixture("space", tombstone),
            clear: false, messageIdEdit: string.Empty);
        var id = System.Text.Encoding.UTF8.GetString(Assert.Single(replay.Headers, header => header.Key == "MessageId").GetValueBytes());
        Assert.True(Guid.TryParseExact(id, "N", out _));
    }

    private static Message<byte[]?, byte[]?> KafkaComposerMessageIdFixture(string kind, bool tombstone)
    {
        var original = KafkaComposerFixture(kind, tombstone);
        var id = original.Key;
        original.Key = null;
        original.Headers = new Headers();
        original.Headers.Add("MessageId", id);
        return original;
    }
    private static Message<byte[]?, byte[]?> KafkaComposerFixture(string kind, bool tombstone)
    {
        byte[]? key = kind switch { "null" => null, "empty" => [], "space" => [0x20], "tab" => [9], _ => [0xff] };
        var headers = new Headers();
        headers.Add("Subject", key?.ToArray());
        return new() { Key = key, Value = tombstone ? null : [], Headers = headers };
    }

    private static async Task<Message<byte[]?, byte[]?>> ReplayKafkaThroughComposer(Message<byte[]?, byte[]?> original, bool clear, bool editWhitespace = false, string? messageIdEdit = null)
    {
        var profile = ServiceBusProfile.CreateNew("Isolated Kafka", EnvironmentKind.Development,
            new(AuthenticationKind.KafkaNone), accessMode: ProfileAccessMode.ReadWrite) with
        { Provider = MessagingProvider.Kafka, Kafka = new KafkaSettings("broker.invalid:9092") };
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        var source = ServiceBusEntityReference.Queue("isolated");
        viewModel.Destinations.Add(new DestinationItemViewModel(source));
        var message = KafkaMessageMapper.FromKafka(new ConsumeResult<byte[]?, byte[]?>
            { Topic = "isolated", Partition = new Partition(0), Offset = new Offset(1), Message = original },
            source, ServiceBusSubQueue.Active);
        viewModel.SelectedMessage = new MessageItemViewModel(message, profile.Id, profile.Name);
        viewModel.OpenMessageAsDraftCommand.Execute(null);
        if (clear) { viewModel.DraftPartitionKey = string.Empty; viewModel.DraftSubject = string.Empty; }
        if (editWhitespace) { viewModel.DraftPartitionKey = "\t"; viewModel.DraftSubject = "\t"; }
        if (messageIdEdit is not null) viewModel.DraftMessageId = messageIdEdit;
        await viewModel.SendDraftCommand.ExecuteAsync();
        return KafkaMessageMapper.ToKafka(Assert.Single(workspace.SentMessages).Message);
    }
}