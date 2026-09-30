using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Azure;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Azure;

/// <summary>Reading queues, topics and subscriptions and mapping them to QueueLoom's model.</summary>
public sealed partial class AzureServiceBusWorkspace
{
    private async Task<bool> RequiresSessionAsync(
        ServiceBusEntityReference source,
        CancellationToken cancellationToken)
    {
        var requiresSession = TryGetRequiresSession(_cachedTopology, source);
        if (!requiresSession.HasValue)
        {
            var administration = GetAdministrationClient();
            requiresSession = source.Kind switch
            {
                ServiceBusEntityKind.Queue =>
                    (await administration.GetQueueAsync(source.Name, cancellationToken).ConfigureAwait(false))
                    .Value.RequiresSession,
                ServiceBusEntityKind.Subscription =>
                    (await administration.GetSubscriptionAsync(
                            source.TopicName!,
                            source.Name,
                            cancellationToken)
                        .ConfigureAwait(false))
                    .Value.RequiresSession,
                _ => throw new ArgumentException(
                    "Only queues and subscriptions can be used as message sources.",
                    nameof(source))
            };
        }

        return requiresSession.Value;
    }

    internal static bool? TryGetRequiresSession(
        ServiceBusTopology? topology,
        ServiceBusEntityReference source)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (topology is null)
        {
            return null;
        }

        if (source.Kind == ServiceBusEntityKind.Queue)
        {
            return topology.Queues.FirstOrDefault(queue =>
                string.Equals(queue.Name, source.Name, StringComparison.OrdinalIgnoreCase))?.RequiresSession;
        }

        if (source.Kind == ServiceBusEntityKind.Subscription)
        {
            var topic = topology.Topics.FirstOrDefault(item =>
                string.Equals(item.Name, source.TopicName, StringComparison.OrdinalIgnoreCase));
            return topic?.Subscriptions.FirstOrDefault(subscription =>
                string.Equals(subscription.Name, source.Name, StringComparison.OrdinalIgnoreCase))?.RequiresSession;
        }

        return null;
    }

    private static async Task<ServiceBusTopic> MapTopicAsync(
        ServiceBusAdministrationClient administration,
        TopicProperties properties,
        TopicRuntimeProperties? runtime,
        CancellationToken cancellationToken)
    {
        var subscriptionPropertiesTask = ReadAllAsync(
            administration.GetSubscriptionsAsync(properties.Name, cancellationToken),
            cancellationToken);
        var subscriptionRuntimeTask = ReadAllAsync(
            administration.GetSubscriptionsRuntimePropertiesAsync(properties.Name, cancellationToken),
            cancellationToken);
        await Task.WhenAll(subscriptionPropertiesTask, subscriptionRuntimeTask).ConfigureAwait(false);

        var runtimeByName = subscriptionRuntimeTask.Result
            .ToDictionary(item => item.SubscriptionName, StringComparer.Ordinal);
        var subscriptions = subscriptionPropertiesTask.Result
            .Select(subscription => MapSubscription(
                subscription,
                runtimeByName.GetValueOrDefault(subscription.SubscriptionName)))
            .OrderBy(subscription => subscription.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var topicRuntime = runtime is null
            ? ServiceBusEntityRuntime.Empty
            : new ServiceBusEntityRuntime(
                new ServiceBusMessageCounts(scheduled: runtime.ScheduledMessageCount),
                runtime.SizeInBytes,
                runtime.CreatedAt,
                runtime.UpdatedAt,
                runtime.AccessedAt);

        return new ServiceBusTopic(
            properties.Name,
            topicRuntime,
            subscriptions,
            MapStatus(properties.Status.ToString()));
    }

    private static ServiceBusQueue MapQueue(QueueProperties properties, QueueRuntimeProperties? runtime) =>
        new(
            properties.Name,
            runtime is null ? ServiceBusEntityRuntime.Empty : MapRuntime(runtime),
            MapStatus(properties.Status.ToString()),
            properties.RequiresSession);

    private static ServiceBusSubscription MapSubscription(
        SubscriptionProperties properties,
        SubscriptionRuntimeProperties? runtime) =>
        new(
            properties.TopicName,
            properties.SubscriptionName,
            runtime is null ? ServiceBusEntityRuntime.Empty : MapRuntime(runtime),
            MapStatus(properties.Status.ToString()),
            properties.RequiresSession);

    private static ServiceBusEntityRuntime MapRuntime(QueueRuntimeProperties runtime) =>
        new(
            new ServiceBusMessageCounts(
                runtime.ActiveMessageCount,
                runtime.DeadLetterMessageCount,
                runtime.ScheduledMessageCount,
                runtime.TransferMessageCount,
                runtime.TransferDeadLetterMessageCount),
            runtime.SizeInBytes,
            runtime.CreatedAt,
            runtime.UpdatedAt,
            runtime.AccessedAt);

    private static ServiceBusEntityRuntime MapRuntime(SubscriptionRuntimeProperties runtime) =>
        new(
            new ServiceBusMessageCounts(
                runtime.ActiveMessageCount,
                runtime.DeadLetterMessageCount,
                scheduled: 0,
                runtime.TransferMessageCount,
                runtime.TransferDeadLetterMessageCount),
            sizeInBytes: 0,
            runtime.CreatedAt,
            runtime.UpdatedAt,
            runtime.AccessedAt);

    private static ServiceBusEntityStatus MapStatus(string status) => status switch
    {
        "Active" => ServiceBusEntityStatus.Active,
        "Disabled" => ServiceBusEntityStatus.Disabled,
        "SendDisabled" => ServiceBusEntityStatus.SendDisabled,
        "ReceiveDisabled" => ServiceBusEntityStatus.ReceiveDisabled,
        "Creating" => ServiceBusEntityStatus.Creating,
        "Deleting" => ServiceBusEntityStatus.Deleting,
        "Renaming" => ServiceBusEntityStatus.Renaming,
        "Restoring" => ServiceBusEntityStatus.Restoring,
        _ => ServiceBusEntityStatus.Unknown
    };

    private static async Task<List<T>> ReadAllAsync<T>(
        AsyncPageable<T> pageable,
        CancellationToken cancellationToken)
        where T : notnull
    {
        var items = new List<T>();
        await foreach (var item in pageable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            items.Add(item);
        }
        return items;
    }
}
