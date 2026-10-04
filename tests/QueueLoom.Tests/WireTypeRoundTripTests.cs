using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

/// <summary>
/// An SNS String.Array (and other labels QueueLoom has no type for) keeps its type through the application's own
/// paths: open as draft and send unchanged, and a dead-letter backup restored later.
/// </summary>
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task WireType_SurvivesOpenAsDraftAndSendUnchanged()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        dialogs.ConfirmResult = true;
        var original = viewModel.Messages.Single(message => message.SequenceNumber == 2).Message;
        var tagged = new BrowsedMessage(original.Source, original.SubQueue, 99, original.Body, original.Properties,
            [new MessageApplicationProperty("tags", ApplicationPropertyType.String, """["blue","green"]""") { WireType = "String.Array" }]);
        viewModel.Messages.Add(new MessageItemViewModel(tagged, viewModel.ConnectedProfileId));
        viewModel.SelectedMessage = viewModel.Messages.Last();

        viewModel.OpenMessageAsDraftCommand.Execute(null);
        await viewModel.SendDraftCommand.ExecuteAsync();

        var sent = Assert.Single(workspace.SentMessages).Message;
        var tags = Assert.Single(sent.ApplicationProperties, property => property.Name == "tags");
        Assert.Equal("String.Array", tags.WireType);
        Assert.Equal("String.Array", AwsMessageMapper.ToSnsAttributes(sent)["tags"].DataType);
    }

    [Fact]
    public void WireType_IsDroppedWhenTheTypeIsChangedInTheEditor()
    {
        var edited = new MessageApplicationProperty("tags", ApplicationPropertyType.Int32, "7") { WireType = "String.Array" };

        Assert.Equal("Number.Int32", AwsMessageMapper.ToAttribute(edited).DataType);
    }
}

public sealed class WireTypeBackupAndIdentityTests
{
    [Fact]
    public async Task WireType_SurvivesABackupAndRestore()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = ServiceBusProfile.CreateNew("aws", EnvironmentKind.Development, new(AuthenticationKind.AwsDefaultCredentials))
            with { Provider = MessagingProvider.AmazonSqsSns };
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 7, "{}"u8.ToArray(),
            new EditableMessageProperties(MessageId: "m-7"),
            [
                new MessageApplicationProperty("tags", ApplicationPropertyType.String, """["blue"]""") { WireType = "String.Array" },
                new MessageApplicationProperty("plain", ApplicationPropertyType.String, "x")
            ]);

        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(profile, DateTimeOffset.UtcNow, CancellationToken.None);
        await session.BackupAsync(message, CancellationToken.None);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));

        Assert.Equal("String.Array", restored.ApplicationProperties.Single(property => property.Name == "tags").WireType);
        Assert.Null(restored.ApplicationProperties.Single(property => property.Name == "plain").WireType);
    }

    // ScheduledResend.IdentityFor hashes the serialized profile. A Kafka profile with the partitioner left off must
    // keep the identity it had before the setting existed, so due schedules and durable operations still match;
    // turning the setting on deliberately changes it.
    [Fact]
    public void KafkaProfileIdentityIsUnchangedUntilTheJavaPartitionerIsTurnedOn()
    {
        var profile = ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development, new(AuthenticationKind.KafkaNone))
            with { Provider = MessagingProvider.Kafka, Kafka = new KafkaSettings("broker:9092") };

        var json = System.Text.Json.JsonSerializer.Serialize(profile);
        Assert.DoesNotContain("JavaCompatiblePartitioner", json, StringComparison.Ordinal);
        var withoutTheSetting = System.Text.Json.Nodes.JsonNode.Parse(json)!.ToJsonString();
        Assert.Equal(withoutTheSetting, json);

        var javaCompatible = profile with { Kafka = profile.Kafka! with { JavaCompatiblePartitioner = true } };
        Assert.NotEqual(QueueLoom.Core.ServiceBus.ScheduledResend.IdentityFor(profile), QueueLoom.Core.ServiceBus.ScheduledResend.IdentityFor(javaCompatible));
        Assert.Contains("\"JavaCompatiblePartitioner\":true", System.Text.Json.JsonSerializer.Serialize(javaCompatible), StringComparison.Ordinal);
    }
}
