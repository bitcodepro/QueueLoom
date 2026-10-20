using System.Diagnostics;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.UiTests;

/// <summary>
/// The app lists Kafka consumer groups in a child of itself (librdkafka 2.15 has aborted a process by freeing that
/// result twice). These tests run the app's assembly, built next to them, as that child, as the app does.
/// </summary>
public sealed class KafkaGroupListingChild
{
    [ModuleInitializer]
    internal static void ListInAChild() => KafkaGroupListing.HelperStart = AppChild;

    private static ProcessStartInfo AppChild()
    {
        var directory = AppContext.BaseDirectory;
        var start = new ProcessStartInfo("dotnet");
        foreach (var argument in new[] { "exec", "--runtimeconfig", Path.Combine(directory, "QueueLoom.UiTests.runtimeconfig.json"),
                     "--depsfile", Path.Combine(directory, "QueueLoom.UiTests.deps.json"), Path.Combine(directory, "QueueLoom.dll"),
                     KafkaGroupListing.HelperArgument })
        {
            start.ArgumentList.Add(argument);
        }
        return start;
    }

    // The child sends the same ListConsumerGroups request as before, so a group keeps its type and state, both for a
    // classic group and for a KIP-848 one (group.protocol=consumer); the classic ListGroups request reports a KIP-848
    // group as Dead. CI's broker (Kafka 4) has the consumer protocol on.
    [EmulatorFact(Emulators.Kafka)]
    public Task The_child_lists_a_classic_group_with_its_type_and_state() => ListsAJoinedGroup("classic", nameof(ConsumerGroupType.Classic));

    [EmulatorFact(Emulators.Kafka)]
    public Task The_child_lists_a_kip848_group_with_its_type_and_state() => ListsAJoinedGroup("consumer", nameof(ConsumerGroupType.Consumer));

    private static async Task ListsAJoinedGroup(string protocol, string type)
    {
        var servers = Environment.GetEnvironmentVariable(Emulators.Kafka)!;
        var run = Guid.NewGuid().ToString("N")[..6];
        var topic = $"listing-{run}";
        var group = $"billing-{protocol}-{run}";
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = servers }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
        try
        {
            var config = new ConsumerConfig { BootstrapServers = servers, GroupId = group, AutoOffsetReset = AutoOffsetReset.Earliest };
            config.Set("group.protocol", protocol);
            using var consumer = new ConsumerBuilder<Ignore, Ignore>(config).Build();
            consumer.Subscribe(topic);
            // A member that holds its partition: the group is Stable.
            var joined = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (consumer.Assignment.Count == 0 && DateTime.UtcNow < joined)
            {
                consumer.Consume(TimeSpan.FromMilliseconds(200));
            }
            Assert.NotEmpty(consumer.Assignment);

            var groups = await KafkaGroupListing.ListAsync(null, new ClientConfig { BootstrapServers = servers },
                TimeSpan.FromSeconds(15), CancellationToken.None);

            var listed = Assert.Single(groups, candidate => candidate.GroupId == group);
            Assert.Equal(type, listed.Type);
            Assert.Equal(nameof(ConsumerGroupState.Stable), listed.State);
            consumer.Close();
        }
        finally
        {
            await admin.DeleteGroupsAsync([group]).ContinueWith(_ => { });
            await admin.DeleteTopicsAsync([topic]);
        }
    }
}
