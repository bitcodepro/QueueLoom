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

    // The child sends the same ListConsumerGroups request as before, so a group keeps its type and state; the classic
    // ListGroups request would report a KIP-848 group as Dead.
    [EmulatorFact(Emulators.Kafka)]
    public async Task The_child_lists_a_group_with_its_type_and_state()
    {
        var servers = Environment.GetEnvironmentVariable(Emulators.Kafka)!;
        var run = Guid.NewGuid().ToString("N")[..6];
        var topic = $"listing-{run}";
        var group = $"billing-{run}";
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = servers }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
        try
        {
            using (var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig { BootstrapServers = servers, GroupId = group }).Build())
            {
                consumer.Commit([new TopicPartitionOffset(topic, 0, 0)]);
            }

            var groups = await KafkaGroupListing.ListAsync(null, new ClientConfig { BootstrapServers = servers },
                TimeSpan.FromSeconds(15), CancellationToken.None);

            var listed = Assert.Single(groups, candidate => candidate.GroupId == group);
            Assert.Equal(nameof(ConsumerGroupState.Empty), listed.State);
            Assert.Equal(nameof(ConsumerGroupType.Classic), listed.Type);
        }
        finally
        {
            await admin.DeleteGroupsAsync([group]).ContinueWith(_ => { });
            await admin.DeleteTopicsAsync([topic]);
        }
    }
}
