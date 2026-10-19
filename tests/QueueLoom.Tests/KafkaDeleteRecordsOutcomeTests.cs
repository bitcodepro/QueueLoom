using System.Reflection;
using Confluent.Kafka;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

// Deleting Kafka records cuts a partition at an offset. When DeleteRecords failed as a whole (a timeout, a lost
// connection) rather than per partition, every record was reported as not deleted, although the broker may have cut
// the partition before the answer was lost: the purge report understated what was gone. The partition's low watermark
// now decides: records before it are gone. When even that cannot be read, the original error is reported.
public sealed class KafkaDeleteRecordsOutcomeTests
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    [Theory]
    [InlineData(3, 0)]  // the cut happened before the answer was lost: all three are gone
    [InlineData(0, 3)]  // nothing was cut: all three remain
    [InlineData(2, 1)]  // the cut reached only the first two
    public async Task AFailedDeleteIsJudgedByTheLowWatermark(long lowAfterDelete, int notDeleted)
    {
        await using var owner = new KafkaWorkspace(new DeepAuditCloudTests.EmptyVault());
        var fixture = new PubSubKafkaStabilityTests.Fixture();
        owner.ConsumerFactory = fixture.Create;
        typeof(KafkaWorkspace).GetField("_admin", Any)!.SetValue(owner, FailingAdmin(() => fixture.Low = lowAfterDelete));
        var channel = Channel(owner);
        try
        {
            var page = await channel.ReceiveAsync(10, CancellationToken.None);

            var remaining = await channel.SettleAsync(page, CancellationToken.None);

            Assert.Equal(notDeleted, remaining.Count);
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    [Fact]
    public async Task WhenTheWatermarkCannotBeReadTheOriginalErrorIsReported()
    {
        await using var owner = new KafkaWorkspace(new DeepAuditCloudTests.EmptyVault());
        var fixture = new PubSubKafkaStabilityTests.Fixture();
        owner.ConsumerFactory = fixture.Create;
        typeof(KafkaWorkspace).GetField("_admin", Any)!.SetValue(owner, FailingAdmin(() => fixture.FailWatermarks = true));
        var channel = Channel(owner);
        try
        {
            var page = await channel.ReceiveAsync(10, CancellationToken.None);

            var error = await Assert.ThrowsAsync<KafkaException>(() => channel.SettleAsync(page, CancellationToken.None));

            Assert.Equal(ErrorCode.RequestTimedOut, error.Error.Code);
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    private static ILeasedMessageChannel Channel(KafkaWorkspace owner)
    {
        var type = typeof(KafkaWorkspace).GetNestedType("KafkaChannel", BindingFlags.NonPublic)!;
        return (ILeasedMessageChannel)Activator.CreateInstance(type, Any, null,
            [owner, new KafkaTopicInfo("isolated", [0], 3), ServiceBusEntityReference.Queue("isolated"), ServiceBusSubQueue.Active, null], null)!;
    }

    private static IAdminClient FailingAdmin(Action beforeFailing)
    {
        var admin = DispatchProxy.Create<IAdminClient, AdminProxy>();
        ((AdminProxy)(object)admin).BeforeFailing = beforeFailing;
        return admin;
    }

    public class AdminProxy : DispatchProxy
    {
        internal Action BeforeFailing { get; set; } = null!;

        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == nameof(IAdminClient.DeleteRecordsAsync))
            {
                BeforeFailing();
                return Task.FromException<List<Confluent.Kafka.Admin.DeleteRecordsResult>>(new KafkaException(ErrorCode.RequestTimedOut));
            }
            if (method.Name == nameof(IDisposable.Dispose)) return null;
            throw new NotSupportedException(method.Name);
        }
    }
}
