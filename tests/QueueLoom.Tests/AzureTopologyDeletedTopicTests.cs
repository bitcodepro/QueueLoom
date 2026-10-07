using System.Reflection;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class AzureTopologyDeletedTopicTests
{
    // A topic is deleted between the topic list and the read of its subscriptions (another tool, a colleague, a
    // test): it is left out and the surviving topic is listed, instead of the whole refresh failing.
    [Fact]
    public async Task ATopicDeletedDuringTheRefreshIsLeftOut()
    {
        var topology = await RefreshAsync(new Administration(gone => new ServiceBusException("gone", ServiceBusFailureReason.MessagingEntityNotFound)));

        Assert.Equal(["kept"], topology.Topics.Select(topic => topic.Name));
    }

    // Only "not found" is absorbed: an unauthorized or busy namespace still fails the refresh.
    [Fact]
    public async Task OtherFailuresStillFailTheRefresh()
    {
        await Assert.ThrowsAnyAsync<Exception>(() =>
            RefreshAsync(new Administration(_ => new ServiceBusException("busy", ServiceBusFailureReason.ServiceBusy))));
        await Assert.ThrowsAnyAsync<Exception>(() =>
            RefreshAsync(new Administration(_ => new UnauthorizedAccessException("no Manage claim"))));
    }

    private static async Task<QueueLoom.Core.ServiceBus.ServiceBusTopology> RefreshAsync(ServiceBusAdministrationClient administration)
    {
        await using var workspace = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(AzureServiceBusWorkspace).GetField("_administration", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, administration);
        return await workspace.GetTopologyAsync(forceRefresh: true);
    }

    private sealed class Pages<T>(IReadOnlyList<T> items, Exception? failure = null) : AsyncPageable<T> where T : notnull
    {
        public override async IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = null, int? pageSizeHint = null)
        {
            await Task.Yield();
            if (failure is not null) throw failure;
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

    private sealed class Administration(Func<string, Exception> goneFailure) : ServiceBusAdministrationClient
    {
        public override AsyncPageable<QueueProperties> GetQueuesAsync(CancellationToken cancellationToken = default) => new Pages<QueueProperties>([]);
        public override AsyncPageable<QueueRuntimeProperties> GetQueuesRuntimePropertiesAsync(CancellationToken cancellationToken = default) =>
            new Pages<QueueRuntimeProperties>([]);
        public override AsyncPageable<TopicProperties> GetTopicsAsync(CancellationToken cancellationToken = default) =>
            new Pages<TopicProperties>([ServiceBusModelFactory.TopicProperties("gone", 1024, false, TimeSpan.FromDays(1), TimeSpan.FromDays(1), TimeSpan.FromMinutes(1), false, EntityStatus.Active, false),
                ServiceBusModelFactory.TopicProperties("kept", 1024, false, TimeSpan.FromDays(1), TimeSpan.FromDays(1), TimeSpan.FromMinutes(1), false, EntityStatus.Active, false)]);
        public override AsyncPageable<TopicRuntimeProperties> GetTopicsRuntimePropertiesAsync(CancellationToken cancellationToken = default) =>
            new Pages<TopicRuntimeProperties>([]);
        public override AsyncPageable<SubscriptionProperties> GetSubscriptionsAsync(string topicName, CancellationToken cancellationToken = default) =>
            topicName == "gone" ? new Pages<SubscriptionProperties>([], goneFailure(topicName)) : new Pages<SubscriptionProperties>([]);
        public override AsyncPageable<SubscriptionRuntimeProperties> GetSubscriptionsRuntimePropertiesAsync(string topicName, CancellationToken cancellationToken = default) =>
            new Pages<SubscriptionRuntimeProperties>([]);
    }
}
