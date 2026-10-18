using System.Reflection;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// When consumer groups could not be listed (for example missing ACLs), or some groups' offsets could not be read, the
// failure was swallowed and the topics showed no groups and no lag: the same as a cluster without consumers. The
// topics still show; their note now says the groups or their lag could not be read, and why.
public sealed class KafkaConsumerGroupNoteTests
{
    [Fact]
    public async Task AClusterThatRefusesToListGroupsStillShowsItsTopicsAndSaysWhy()
    {
        var topology = await LagAsync(listGroups: () => throw new KafkaException(new Error(ErrorCode.GroupAuthorizationFailed,
            "Not authorized to describe groups\nfor this principal")));

        var orders = Assert.Single(topology.Queues);
        Assert.Null(orders.Consumers);
        Assert.Contains("Consumer groups could not be read (Not authorized to describe groups for this principal)", orders.Note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GroupsWhoseOffsetsCannotBeReadAreCountedInTheNote()
    {
        var topology = await LagAsync(
            listGroups: () => [Group("billing"), Group("audit"), Group("reports")],
            offsets: group => group == "billing"
                ? [new TopicPartitionOffsetError("orders", 0, 4, new Error(ErrorCode.NoError))]
                : throw new KafkaException(new Error(ErrorCode.GroupAuthorizationFailed, "denied")));

        var orders = Assert.Single(topology.Queues);
        Assert.Equal("billing", Assert.Single(orders.Consumers!.Groups).Group);
        Assert.Contains("The offsets of 2 consumer group(s) could not be read (denied)", orders.Note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadableGroupsAddNoNote()
    {
        var topology = await LagAsync(listGroups: () => [Group("billing")],
            offsets: _ => [new TopicPartitionOffsetError("orders", 0, 4, new Error(ErrorCode.NoError))]);

        Assert.DoesNotContain("could not be read", Assert.Single(topology.Queues).Note, StringComparison.Ordinal);
    }

    private static ConsumerGroupListing Group(string id) => new() { GroupId = id, State = ConsumerGroupState.Stable };

    private static async Task<QueueLoom.Core.ServiceBus.ServiceBusTopology> LagAsync(Func<List<ConsumerGroupListing>> listGroups,
        Func<string, List<TopicPartitionOffsetError>>? offsets = null)
    {
        await using var workspace = new KafkaWorkspace(new DeepAuditCloudTests.EmptyVault());
        var admin = DispatchProxy.Create<IAdminClient, FakeAdmin>();
        ((FakeAdmin)(object)admin).ListGroups = listGroups;
        ((FakeAdmin)(object)admin).Offsets = offsets ?? (_ => []);
        typeof(KafkaWorkspace).GetField("_admin", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, admin);
        var index = new KafkaTopologyIndex([new KafkaTopicInfo("orders", [0], 10) { Ends = new Dictionary<int, long> { [0] = 10 } }],
            QueueLoom.Core.Profiles.KafkaSettings.DefaultDeadLetterSuffixes);
        var lag = (Task<KafkaTopologyIndex>)typeof(KafkaWorkspace).GetMethod("WithConsumerLagAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(workspace, [index, CancellationToken.None])!;
        return (await lag).ToTopology(DateTimeOffset.UnixEpoch);
    }

    public class FakeAdmin : DispatchProxy
    {
        public Func<List<ConsumerGroupListing>> ListGroups { get; set; } = () => [];
        public Func<string, List<TopicPartitionOffsetError>> Offsets { get; set; } = _ => [];

        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            switch (method!.Name)
            {
                case nameof(IAdminClient.ListConsumerGroupsAsync):
                    try
                    {
                        return Task.FromResult(new ListConsumerGroupsResult { Valid = ListGroups() });
                    }
                    catch (Exception exception)
                    {
                        return Task.FromException<ListConsumerGroupsResult>(exception);
                    }
                case nameof(IAdminClient.ListConsumerGroupOffsetsAsync):
                    var group = ((IEnumerable<ConsumerGroupTopicPartitions>)args![0]!).Single().Group;
                    try
                    {
                        return Task.FromResult(new List<ListConsumerGroupOffsetsResult>
                        {
                            new() { Group = group, Partitions = Offsets(group) }
                        });
                    }
                    catch (Exception exception)
                    {
                        return Task.FromException<List<ListConsumerGroupOffsetsResult>>(exception);
                    }
                case nameof(IDisposable.Dispose):
                    return null;
                default:
                    throw new NotSupportedException(method.Name);
            }
        }
    }
}
