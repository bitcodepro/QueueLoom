using Azure.Core;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class AzureServiceBusWorkspaceTests
{
    [Fact]
    public void ClientOptions_UseBoundedInteractiveRetries()
    {
        var messaging = AzureServiceBusWorkspace.CreateMessagingClientOptions();
        var administration = AzureServiceBusWorkspace.CreateAdministrationClientOptions();

        Assert.Equal(ServiceBusTransportType.AmqpTcp, messaging.TransportType);
        Assert.Equal(ServiceBusRetryMode.Exponential, messaging.RetryOptions.Mode);
        Assert.Equal(2, messaging.RetryOptions.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(15), messaging.RetryOptions.TryTimeout);
        Assert.Equal(TimeSpan.FromSeconds(3), messaging.RetryOptions.MaxDelay);

        Assert.Equal(RetryMode.Exponential, administration.Retry.Mode);
        Assert.Equal(2, administration.Retry.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(15), administration.Retry.NetworkTimeout);
        Assert.Equal(TimeSpan.FromSeconds(3), administration.Retry.MaxDelay);
    }

    [Fact]
    public void SearchSafetyLimit_DoesNotUseEventuallyConsistentKnownCount()
    {
        var target = new DeadLetterSearchTarget(
            ServiceBusEntityReference.Queue("orders"),
            ServiceBusSubQueue.DeadLetter,
            KnownMessageCount: 0);
        var request = new DeadLetterSearchRequest(
            "correlation-42",
            [target],
            maximumMessagesPerTarget: 37);

        Assert.Equal(37, AzureServiceBusWorkspace.GetSearchSafetyLimit(request, target));
    }

    [Fact]
    public async Task SearchTargetPages_PreservesScannedMatchesWhenLaterPageIsCancelled()
    {
        var source = ServiceBusEntityReference.Queue("orders");
        var target = new DeadLetterSearchTarget(source, ServiceBusSubQueue.DeadLetter, KnownMessageCount: 0);
        var request = new DeadLetterSearchRequest(
            "match",
            [target],
            batchSize: 2,
            maximumMessagesPerTarget: 10);
        using var cancellation = new CancellationTokenSource();

        var result = await AzureServiceBusWorkspace.SearchTargetPagesAsync(
            target,
            request,
            (count, from, token) =>
            {
                if (from is null)
                {
                    return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(
                        [Message(1, "match"), Message(2, "other")]);
                }

                cancellation.Cancel();
                return Task.FromCanceled<IReadOnlyList<ServiceBusReceivedMessage>>(token);
            },
            message => AzureMessageMapper.FromAzure(message, source, ServiceBusSubQueue.DeadLetter),
            shouldStop: () => false,
            cancellation.Token);

        Assert.Equal(2, result.ScannedMessageCount);
        var match = Assert.Single(result.Matches);
        Assert.Equal(1, match.SequenceNumber);
        Assert.False(result.IsSuccessful);
        Assert.Contains("cancelled or timed out", result.Error, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SearchTargetPages_ReportsPartialProgressWhenLaterPageFails()
    {
        var source = ServiceBusEntityReference.Queue("orders");
        var target = new DeadLetterSearchTarget(source, ServiceBusSubQueue.DeadLetter, KnownMessageCount: 1);
        var request = new DeadLetterSearchRequest(
            "match",
            [target],
            batchSize: 2,
            maximumMessagesPerTarget: 10);

        var result = await AzureServiceBusWorkspace.SearchTargetPagesAsync(
            target,
            request,
            (count, from, _) => from is null
                ? Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(
                    [Message(1, "match"), Message(2, "other")])
                : Task.FromException<IReadOnlyList<ServiceBusReceivedMessage>>(
                    new InvalidOperationException("second page failed")),
            message => AzureMessageMapper.FromAzure(message, source, ServiceBusSubQueue.DeadLetter),
            shouldStop: () => false,
            CancellationToken.None);

        Assert.Equal(2, result.ScannedMessageCount);
        Assert.Single(result.Matches);
        Assert.Equal("second page failed", result.Error);
    }

    [Fact]
    public async Task SearchPageWalker_ContinuesUntilAnEmptyPage()
    {
        var visited = new List<long>();
        var requestedFrom = new List<long?>();

        var result = await AzureServiceBusWorkspace.WalkSearchPagesAsync(
            batchSize: 2,
            safetyLimit: 10,
            (count, from, _) =>
            {
                requestedFrom.Add(from);
                return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(from switch
                {
                    null => [Message(1), Message(2)],
                    3 => [Message(3), Message(4)],
                    5 => [],
                    _ => throw new InvalidOperationException($"Unexpected page start: {from}")
                });
            },
            message => visited.Add(message.SequenceNumber),
            shouldStop: () => false,
            CancellationToken.None);

        Assert.Equal(4, result.ScannedMessageCount);
        Assert.False(result.SafetyLimitReached);
        Assert.Equal([null, 3, 5], requestedFrom);
        Assert.Equal([1, 2, 3, 4], visited);
    }

    [Fact]
    public async Task SearchPageWalker_ProbesAfterSafetyLimit()
    {
        var visited = new List<long>();

        var result = await AzureServiceBusWorkspace.WalkSearchPagesAsync(
            batchSize: 2,
            safetyLimit: 3,
            (count, from, _) => Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(from switch
            {
                null => [Message(1), Message(2)],
                3 => [Message(3)],
                4 => [Message(4)],
                _ => []
            }),
            message => visited.Add(message.SequenceNumber),
            shouldStop: () => false,
            CancellationToken.None);

        Assert.Equal(3, result.ScannedMessageCount);
        Assert.True(result.SafetyLimitReached);
        Assert.Equal([1, 2, 3], visited);
    }

    [Fact]
    public async Task SearchPageWalker_ExactSafetyLimitIsCompleteWhenProbeIsEmpty()
    {
        var result = await AzureServiceBusWorkspace.WalkSearchPagesAsync(
            batchSize: 2,
            safetyLimit: 2,
            (count, from, _) => Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(from switch
            {
                null => [Message(1), Message(2)],
                3 => [],
                _ => throw new InvalidOperationException($"Unexpected page start: {from}")
            }),
            visitMessage: _ => { },
            shouldStop: () => false,
            CancellationToken.None);

        Assert.Equal(2, result.ScannedMessageCount);
        Assert.False(result.SafetyLimitReached);
    }

    [Fact]
    public void PurgeRequiresTwoConsecutiveEmptyReceives()
    {
        Assert.False(AzureServiceBusWorkspace.HasConfirmedEmptyPurge(1));
        Assert.True(AzureServiceBusWorkspace.HasConfirmedEmptyPurge(2));
    }

    [Fact]
    public void SessionRequirement_IsResolvedFromCachedTopology()
    {
        var sessionQueue = new ServiceBusQueue(
            "session-queue",
            ServiceBusEntityRuntime.Empty,
            RequiresSession: true);
        var regularQueue = new ServiceBusQueue(
            "regular-queue",
            ServiceBusEntityRuntime.Empty);
        var subscription = new ServiceBusSubscription(
            "events",
            "session-subscription",
            ServiceBusEntityRuntime.Empty,
            RequiresSession: true);
        var topology = new ServiceBusTopology(
            DateTimeOffset.UtcNow,
            [sessionQueue, regularQueue],
            [new ServiceBusTopic("events", ServiceBusEntityRuntime.Empty, [subscription])]);

        Assert.True(AzureServiceBusWorkspace.TryGetRequiresSession(topology, sessionQueue.Reference));
        Assert.False(AzureServiceBusWorkspace.TryGetRequiresSession(topology, regularQueue.Reference));
        Assert.True(AzureServiceBusWorkspace.TryGetRequiresSession(topology, subscription.Reference));
        Assert.Null(AzureServiceBusWorkspace.TryGetRequiresSession(
            topology,
            ServiceBusEntityReference.Queue("missing")));
    }

    private static ServiceBusReceivedMessage Message(long sequenceNumber, string? body = null) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString(body ?? $"message-{sequenceNumber}"),
            messageId: $"message-{sequenceNumber}",
            sequenceNumber: sequenceNumber);
}
