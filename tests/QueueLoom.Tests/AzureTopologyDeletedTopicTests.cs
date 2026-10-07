using System.Reflection;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class AzureTopologyDeletedTopicTests
{
    private static Exception NotFound() => new ServiceBusException("gone", ServiceBusFailureReason.MessagingEntityNotFound);
    private static Exception Busy() => new ServiceBusException("busy", ServiceBusFailureReason.ServiceBusy);
    private static Exception Cancelled() => new OperationCanceledException("cancelled");

    // A topic is deleted between the topic list and the reads of its subscriptions (another tool, a colleague, a
    // test): it is left out and the surviving topic is listed, instead of the whole refresh failing. Either read, or
    // both, may be the one that notices.
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task ATopicDeletedDuringTheRefreshIsLeftOut(bool propertiesNotFound, bool runtimeNotFound)
    {
        var topology = await RefreshAsync(new Administration(
            propertiesNotFound ? NotFound : null, runtimeNotFound ? NotFound : null, propertiesFirst: true));

        Assert.Equal(["kept"], topology.Topics.Select(topic => topic.Name));
    }

    // Only "not found" is absorbed: an unauthorized or busy namespace still fails the refresh.
    [Fact]
    public async Task OtherFailuresStillFailTheRefresh()
    {
        var busy = await Assert.ThrowsAsync<ServiceBusException>(() => RefreshAsync(new Administration(Busy, null, propertiesFirst: true)));
        Assert.Equal(ServiceBusFailureReason.ServiceBusy, busy.Reason);
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            RefreshAsync(new Administration(null, () => new UnauthorizedAccessException("no Manage claim"), propertiesFirst: true)));
    }

    // One read reports "not found" and the other fails for another reason, in either order: the other failure is
    // what the refresh reports. Absorbing it would cache a topology missing a topic that may well still exist.
    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task NotFoundDoesNotHideAnotherFailure(bool notFoundOnProperties, bool propertiesFirst)
    {
        var administration = notFoundOnProperties
            ? new Administration(NotFound, Busy, propertiesFirst)
            : new Administration(Busy, NotFound, propertiesFirst);

        var failure = await Assert.ThrowsAsync<ServiceBusException>(() => RefreshAsync(administration));

        Assert.Equal(ServiceBusFailureReason.ServiceBusy, failure.Reason);
    }

    // The same with a cancelled read: the refresh is cancelled; it does not succeed without the topic.
    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task NotFoundDoesNotHideACancellation(bool notFoundOnProperties, bool propertiesFirst)
    {
        var administration = notFoundOnProperties
            ? new Administration(NotFound, Cancelled, propertiesFirst)
            : new Administration(Cancelled, NotFound, propertiesFirst);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => RefreshAsync(administration));
    }

    private static async Task<QueueLoom.Core.ServiceBus.ServiceBusTopology> RefreshAsync(ServiceBusAdministrationClient administration)
    {
        await using var workspace = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(AzureServiceBusWorkspace).GetField("_administration", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, administration);
        return await workspace.GetTopologyAsync(forceRefresh: true);
    }

    /// <summary>One page of items, or a failure. It starts once <paramref name="after"/> completes and then signals <paramref name="done"/>.</summary>
    private sealed class Pages<T>(IReadOnlyList<T> items, Exception? failure = null, Task? after = null, TaskCompletionSource? done = null)
        : AsyncPageable<T> where T : notnull
    {
        public override async IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = null, int? pageSizeHint = null)
        {
            await Task.Yield();
            if (after is not null) await after;
            try
            {
                if (failure is not null) throw failure;
            }
            finally
            {
                done?.TrySetResult();
            }
            yield return Page<T>.FromValues(items, null, new NoResponse());
        }
    }

    private sealed class NoResponse : Response
    {
        public override int Status => 200;
        public override string ReasonPhrase => "OK";
        public override Stream? ContentStream { get; set; }
        public override string ClientRequestId { get; set; } = string.Empty;
        public override void Dispose() { }
        protected override bool ContainsHeader(string name) => false;
        protected override IEnumerable<Azure.Core.HttpHeader> EnumerateHeaders() => [];
        protected override bool TryGetHeader(string name, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out string? value) { value = null; return false; }
        protected override bool TryGetHeaderValues(string name, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out IEnumerable<string>? values) { values = null; return false; }
    }

    /// <summary>
    /// Topics "gone" and "kept". For "gone", the subscription properties and runtime reads end with the given
    /// failures (null: they succeed), one strictly after the other.
    /// </summary>
    private sealed class Administration(Func<Exception>? properties, Func<Exception>? runtime, bool propertiesFirst) : ServiceBusAdministrationClient
    {
        private readonly TaskCompletionSource _propertiesDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _runtimeDone = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override AsyncPageable<QueueProperties> GetQueuesAsync(CancellationToken cancellationToken = default) => new Pages<QueueProperties>([]);
        public override AsyncPageable<QueueRuntimeProperties> GetQueuesRuntimePropertiesAsync(CancellationToken cancellationToken = default) =>
            new Pages<QueueRuntimeProperties>([]);
        public override AsyncPageable<TopicProperties> GetTopicsAsync(CancellationToken cancellationToken = default) =>
            new Pages<TopicProperties>([Topic("gone"), Topic("kept")]);
        public override AsyncPageable<TopicRuntimeProperties> GetTopicsRuntimePropertiesAsync(CancellationToken cancellationToken = default) =>
            new Pages<TopicRuntimeProperties>([]);
        public override AsyncPageable<SubscriptionProperties> GetSubscriptionsAsync(string topicName, CancellationToken cancellationToken = default) =>
            topicName == "gone"
                ? new Pages<SubscriptionProperties>([], properties?.Invoke(), propertiesFirst ? null : _runtimeDone.Task, _propertiesDone)
                : new Pages<SubscriptionProperties>([]);
        public override AsyncPageable<SubscriptionRuntimeProperties> GetSubscriptionsRuntimePropertiesAsync(string topicName, CancellationToken cancellationToken = default) =>
            topicName == "gone"
                ? new Pages<SubscriptionRuntimeProperties>([], runtime?.Invoke(), propertiesFirst ? _propertiesDone.Task : null, _runtimeDone)
                : new Pages<SubscriptionRuntimeProperties>([]);

        private static TopicProperties Topic(string name) => ServiceBusModelFactory.TopicProperties(
            name, 1024, false, TimeSpan.FromDays(1), TimeSpan.FromDays(1), TimeSpan.FromMinutes(1), false, EntityStatus.Active, false);
    }
}
