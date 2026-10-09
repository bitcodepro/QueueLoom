using System.Reflection;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;
using Grpc.Core;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Messaging;
using Status = Grpc.Core.Status;

namespace QueueLoom.Tests;

// On an exactly-once subscription a failed Acknowledge names, in its ErrorInfo, the ack IDs that failed; the others in the
// request were acknowledged. The whole chunk was reported as not settled, so a purge undercounted what it deleted and said
// those messages "could not be deleted", then tried to return messages that were already gone. Only the named IDs are
// unsettled now; an error without such details still counts the whole chunk, as before.
public sealed class PubSubExactlyOnceAckTests
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    [Fact]
    public async Task OnlyTheAckIdsAnExactlyOnceFailureNamesAreUnsettled()
    {
        var subscriber = new FailingAckSubscriber(Failure(new Dictionary<string, string>
        {
            ["a1"] = "PERMANENT_FAILURE_INVALID_ACK_ID",
            ["a2"] = "TRANSIENT_FAILURE_UNORDERED_ACK_ID"
        }));

        var failed = await Settle(subscriber, 4);

        Assert.Equal(["a1", "a2"], failed.Select(message => message.LeaseHandle).Order());
    }

    [Fact]
    public async Task AFailureWithoutPerIdDetailsStillLeavesTheWholeChunkUnsettled()
    {
        var subscriber = new FailingAckSubscriber(new RpcException(new Status(StatusCode.Unavailable, "down")));

        var failed = await Settle(subscriber, 4);

        Assert.Equal(4, failed.Count);
    }

    private static async Task<IReadOnlyCollection<LeasedMessage>> Settle(SubscriberServiceApiClient subscriber, int count)
    {
        await using var workspace = new GooglePubSubWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(GooglePubSubWorkspace).GetField("_subscriber", Any)!.SetValue(workspace, subscriber);
        var type = typeof(GooglePubSubWorkspace).GetNestedType("PubSubChannel", BindingFlags.NonPublic)!;
        var source = ServiceBusEntityReference.Subscription("t", "s");
        var channel = (ILeasedMessageChannel)Activator.CreateInstance(type, Any, null,
            [workspace, source, ServiceBusSubQueue.Active, new SubscriptionName("p", "s"), null], null)!;
        var messages = Enumerable.Range(0, count).Select(i => new LeasedMessage(
            GooglePubSubWorkspace.ToBrowsedMessage(new ReceivedMessage { AckId = $"a{i}", Message = new PubsubMessage { MessageId = $"m{i}" } },
                source, ServiceBusSubQueue.Active), $"a{i}", true)).ToList();
        return await channel.SettleAsync(messages, CancellationToken.None);
    }

    /// <summary>The RpcException an exactly-once Acknowledge returns: per-ID results in an ErrorInfo status detail.</summary>
    private static RpcException Failure(IDictionary<string, string> metadata)
    {
        var info = new ErrorInfo { Reason = "EXACTLY_ONCE_ACKID_FAILURE" };
        info.Metadata.Add(metadata);
        var status = new Google.Rpc.Status { Code = (int)StatusCode.InvalidArgument, Message = "Some acknowledgement ids in the request were invalid." };
        status.Details.Add(Google.Protobuf.WellKnownTypes.Any.Pack(info));
        var trailers = new Metadata { { "grpc-status-details-bin", status.ToByteArray() } };
        var exception = new RpcException(new Status(StatusCode.InvalidArgument, status.Message), trailers);
        Assert.NotNull(exception.GetErrorInfo());
        return exception;
    }

    private sealed class FailingAckSubscriber(RpcException failure) : SubscriberServiceApiClient
    {
        public override Task AcknowledgeAsync(AcknowledgeRequest request, Google.Api.Gax.Grpc.CallSettings? callSettings = null) =>
            Task.FromException(failure);
    }
}
