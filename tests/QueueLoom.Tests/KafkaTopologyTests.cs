using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

public sealed class KafkaTopologyTests
{
    [Fact]
    public void DeadLetterTopics_ArePairedByNameAndInternalTopicsAreHidden()
    {
        var index = new KafkaTopologyIndex(
            [
                new KafkaTopicInfo("orders", [0, 1, 2], 120),
                new KafkaTopicInfo("orders.DLT", [0], 4),
                new KafkaTopicInfo("payments", [0], 7),
                new KafkaTopicInfo("payments-dlq", [0], 1),
                new KafkaTopicInfo("audit.DLT", [0], 3)
            ],
            KafkaSettings.DefaultDeadLetterSuffixes);

        Assert.Equal("orders.DLT", index.DeadLetterTopicOf("orders"));
        Assert.Equal("payments-dlq", index.DeadLetterTopicOf("payments"));
        Assert.Null(index.DeadLetterTopicOf("orders.DLT"));
        Assert.Null(index.SourceOf("audit.DLT"));
        Assert.True(KafkaTopologyIndex.IsInternal("__consumer_offsets"));
        Assert.True(KafkaTopologyIndex.IsInternal("_schemas"));

        var topology = index.ToTopology(DateTimeOffset.UtcNow);
        var orders = topology.Queues.Single(queue => queue.Name == "orders");
        Assert.Equal((120L, 4L), (orders.Runtime.MessageCounts.Active, orders.Runtime.MessageCounts.DeadLetter));
        Assert.Equal("3 partitions", orders.Note);
        Assert.Equal("Dead-letter topic of orders · 1 partition", topology.Queues.Single(queue => queue.Name == "orders.DLT").Note);
        Assert.False(topology.Queues.Single(queue => queue.Name == "audit.DLT").HasDeadLetterQueue);
        Assert.False(topology.CanDeleteSelectedMessages);
    }

    [Fact]
    public void Profiles_NeedHostAndPortServersAndCompleteSasl()
    {
        ServiceBusProfile Kafka(KafkaSettings settings, AuthenticationKind kind = AuthenticationKind.KafkaNone) =>
            ServiceBusProfile.CreateNew("Events", EnvironmentKind.Development, new AuthenticationSettings(kind)) with
            { Provider = MessagingProvider.Kafka, Kafka = settings };

        Assert.True(ProfileValidator.Validate(Kafka(new KafkaSettings("broker-1:9092,broker-2:9092"))).IsValid);
        Assert.Contains(ProfileValidator.Validate(Kafka(new KafkaSettings("broker-1"))).Errors,
            error => error.Code == "profile.kafka.servers.invalid");
        Assert.Contains(ProfileValidator.Validate(Kafka(new KafkaSettings("broker:9092"), AuthenticationKind.KafkaSaslPassword)).Errors,
            error => error.Code == "profile.kafka.sasl.incomplete");
    }

    [Fact]
    public void Editor_BuildsASaslKafkaEnvironmentWithCustomDeadLetterEndings()
    {
        var editor = new ProfileEditorViewModel(null) { Name = "Events" };
        editor.SelectedProvider = editor.ProviderOptions.Single(option => option.Provider == MessagingProvider.Kafka);
        editor.AuthenticationKind = AuthenticationKind.KafkaSaslPassword;
        editor.KafkaBootstrapServers = " broker-1:9093 , broker-2:9093 ";
        editor.KafkaUseTls = true;
        editor.KafkaUserName = "queueloom";
        editor.BrokerPassword = "s3cret";
        editor.KafkaDeadLetterSuffixes = ".DLT, .errors";

        Assert.True(editor.TryBuild(out var result), editor.Error);
        var settings = result!.Profile.Kafka!;
        Assert.Equal("broker-1:9093,broker-2:9093", settings.BootstrapServers);
        Assert.Equal(KafkaSaslMechanism.ScramSha512, settings.SaslMechanism);
        Assert.Equal([".DLT", ".errors"], settings.DeadLetterSuffixes);
        Assert.Equal("s3cret", result.ConnectionString);
        Assert.Equal("SASL ScramSha512 · queueloom", result.Profile.AuthenticationDisplayName);
    }

    [Fact]
    public void TopicsWithoutAPartitionLeader_AreStillListedWithANote()
    {
        var index = new KafkaTopologyIndex(
            [new KafkaTopicInfo("orders", [0], 0) { CountError = "counts unavailable: a partition has no leader right now" }],
            KafkaSettings.DefaultDeadLetterSuffixes);

        var orders = Assert.Single(index.ToTopology(DateTimeOffset.UtcNow).Queues);
        Assert.Equal("1 partition · counts unavailable: a partition has no leader right now", orders.Note);
    }

}
