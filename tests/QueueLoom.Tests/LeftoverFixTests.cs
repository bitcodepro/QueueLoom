using System.Reflection;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using QueueLoom.App.Services;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Mcp;
using QueueLoom.Tests.Infrastructure;
using static QueueLoom.Tests.ViewModelStateTests;

namespace QueueLoom.Tests;

public sealed class LeftoverFixTests
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    // ---- Replacing a file the user picked ------------------------------------------------------------------------

    // A write that fails part-way leaves the existing file as it was, and no temporary file behind.
    [Fact]
    public async Task SafeFileWriter_FailedWriteKeepsThePreviousFile()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");

        await Assert.ThrowsAsync<IOException>(() => SafeFileWriter.WriteAsync(path, async (stream, token) =>
        {
            await stream.WriteAsync("{ \"partial\": "u8.ToArray(), token);
            throw new IOException("disk full");
        }, CancellationToken.None));

        Assert.Equal("previous export", await File.ReadAllTextAsync(path));
        Assert.Equal([path], Directory.GetFiles(directory.Path));
    }

    // A cancelled write leaves the previous file too.
    [Fact]
    public async Task SafeFileWriter_CancelledWriteKeepsThePreviousFile()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(path, "previous export");
        using var cancellation = new CancellationTokenSource();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => SafeFileWriter.WriteAsync(path, async (stream, token) =>
        {
            await stream.WriteAsync("{ \"partial\": "u8.ToArray(), token);
            await cancellation.CancelAsync();
        }, cancellation.Token));

        Assert.Equal("previous export", await File.ReadAllTextAsync(path));
        Assert.Equal([path], Directory.GetFiles(directory.Path));
    }

    [Fact]
    public void SafeFileWriter_ReplacesTheFileWhenComplete()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "receipt.json");
        File.WriteAllText(path, "old receipt");

        SafeFileWriter.WriteText(path, "{\"Recovered\":true}");

        Assert.Equal("{\"Recovered\":true}", File.ReadAllText(path));
        Assert.Equal([path], Directory.GetFiles(directory.Path));
    }

    // ---- Pub/Sub: holding messages just pulled ---------------------------------------------------------------------

    // A transient failure to extend the hold is retried: the pulled batch is returned instead of dropped (dropping it
    // would bring the messages back only after the ack deadline, to be pulled and counted again).
    [Fact]
    public async Task PubSub_TransientHoldFailureIsRetried()
    {
        var subscriber = new HoldSubscriber { HoldFailures = 1 };
        var channel = PubSubChannel(subscriber, out _);

        var received = await channel.ReceiveAsync(10, CancellationToken.None);

        Assert.Equal(2, received.Count);
        Assert.Equal([GooglePubSubWorkspace.HoldSeconds, GooglePubSubWorkspace.HoldSeconds], subscriber.Deadlines);
    }

    // A hold that cannot be set releases the batch at once (deadline 0) and says so, rather than leaving the messages
    // invisible until the subscription's ack deadline passes.
    [Fact]
    public async Task PubSub_PersistentHoldFailureReleasesTheBatchAndExplains()
    {
        var subscriber = new HoldSubscriber { HoldFailures = int.MaxValue };
        var channel = PubSubChannel(subscriber, out _);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => channel.ReceiveAsync(10, CancellationToken.None));

        Assert.Contains("returned to the subscription", error.Message, StringComparison.Ordinal);
        Assert.Equal(0, subscriber.Deadlines[^1]);
        Assert.Equal(["a1", "a2"], subscriber.ReleasedAckIds);
    }

    // A definite refusal (e.g. permission denied) is not retried.
    [Fact]
    public async Task PubSub_DefiniteHoldRefusalIsNotRetried()
    {
        var subscriber = new HoldSubscriber { HoldFailures = int.MaxValue, HoldStatus = StatusCode.PermissionDenied };
        var channel = PubSubChannel(subscriber, out _);

        await Assert.ThrowsAsync<InvalidOperationException>(() => channel.ReceiveAsync(10, CancellationToken.None));

        Assert.Equal([GooglePubSubWorkspace.HoldSeconds, 0], subscriber.Deadlines);
    }

    private static ILeasedMessageChannel PubSubChannel(HoldSubscriber subscriber, out GooglePubSubWorkspace workspace)
    {
        workspace = new GooglePubSubWorkspace(new EmptyVault());
        typeof(GooglePubSubWorkspace).GetField("_subscriber", Any)!.SetValue(workspace, subscriber);
        var type = typeof(GooglePubSubWorkspace).GetNestedType("PubSubChannel", BindingFlags.NonPublic)!;
        return (ILeasedMessageChannel)Activator.CreateInstance(type, Any, null,
            [workspace, ServiceBusEntityReference.Subscription("t", "s"), ServiceBusSubQueue.Active, new SubscriptionName("p", "s"), null], null)!;
    }

    private sealed class HoldSubscriber : SubscriberServiceApiClient
    {
        public int HoldFailures { get; set; }
        public StatusCode HoldStatus { get; init; } = StatusCode.Unavailable;
        public List<int> Deadlines { get; } = [];
        public List<string> ReleasedAckIds { get; } = [];

        public override Task<PullResponse> PullAsync(PullRequest request, CallSettings? callSettings = null) =>
            Task.FromResult(new PullResponse
            {
                ReceivedMessages =
                {
                    new ReceivedMessage { AckId = "a1", Message = new PubsubMessage { MessageId = "m1" } },
                    new ReceivedMessage { AckId = "a2", Message = new PubsubMessage { MessageId = "m2" } }
                }
            });

        public override Task ModifyAckDeadlineAsync(ModifyAckDeadlineRequest request, CallSettings? callSettings = null)
        {
            Deadlines.Add(request.AckDeadlineSeconds);
            if (request.AckDeadlineSeconds == 0)
            {
                ReleasedAckIds.AddRange(request.AckIds);
                return Task.CompletedTask;
            }
            if (HoldFailures-- > 0)
            {
                return Task.FromException(new RpcException(new Status(HoldStatus, "hold failed")));
            }
            return Task.CompletedTask;
        }
    }

    // ---- MCP: returning the connection to read-only -----------------------------------------------------------------

    // Both the revert to read-only and the fallback disconnect fail: the write's own result still reaches the client,
    // and the next tool call does not reuse the connection that may still carry write access.
    [Fact]
    public async Task Mcp_FailedRevertAndDisconnectKeepTheResultAndForceAReadOnlyReconnect()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        var repository = new FakeProfileRepository([profile], profile.Id);
        var workspace = new FakeWorkspace();
        using var session = new McpWorkspaceSession(repository, workspace, NullLogger<McpWorkspaceSession>.Instance);
        await session.ReadAsync(profile, (_, _) => Task.FromResult(0), default);
        workspace.FailReadOnlyRevert = true;
        workspace.FailDisconnect = true;

        var result = await session.WriteAsync(profile, (_, _) => Task.FromResult(42), default);

        Assert.Equal(42, result);
        Assert.Equal(ProfileAccessMode.ReadWrite, workspace.ConnectedAccessMode);
        workspace.FailReadOnlyRevert = false;
        workspace.FailDisconnect = false;
        var connects = workspace.ConnectCalls;
        await session.ReadAsync(profile, (_, _) => Task.FromResult(0), default);
        Assert.Equal(connects + 1, workspace.ConnectCalls);
        Assert.Equal(ProfileAccessMode.ReadOnly, workspace.ConnectedAccessMode);
    }

    // The write's own failure is what the client sees, not the cleanup's.
    [Fact]
    public async Task Mcp_FailedRevertDoesNotReplaceTheWriteError()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        var repository = new FakeProfileRepository([profile], profile.Id);
        var workspace = new FakeWorkspace { FailReadOnlyRevert = true, FailDisconnect = true };
        using var session = new McpWorkspaceSession(repository, workspace, NullLogger<McpWorkspaceSession>.Instance);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => session.WriteAsync<int>(profile,
            (_, _) => throw new InvalidOperationException("the broker refused the send"), default));

        Assert.Equal("the broker refused the send", error.Message);
    }

    private sealed class EmptyVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string value, CancellationToken token = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
    }
}
