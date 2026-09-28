using Avalonia;
using QueueLoom.App.Models;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public enum BackupGroupKind
{
    All,
    Environment,
    Queue,
    Topic,
    Subscription
}

/// <summary>
/// One row of the backup tree on the Backups page: all backups, an environment, a queue, a topic (with all of its
/// subscriptions) or one subscription. Selecting it filters the list; "Delete" removes every backup it covers.
/// </summary>
public sealed class BackupGroupItemViewModel(
    string key,
    BackupGroupKind kind,
    string title,
    int count,
    int indent,
    Tone tone,
    Func<BackupMessageItemViewModel, bool> contains)
{
    public string Key { get; } = key;

    public BackupGroupKind Kind { get; } = kind;

    public string Title { get; } = title;

    public int Count { get; } = count;

    public Tone Tone { get; } = tone;

    public Thickness IndentMargin { get; } = new(indent * 14, 0, 0, 0);

    public string KindLabel => Kind switch
    {
        BackupGroupKind.All => "ALL",
        BackupGroupKind.Environment => "ENVIRONMENT",
        BackupGroupKind.Queue => "QUEUE",
        BackupGroupKind.Topic => "TOPIC",
        _ => "SUBSCRIPTION"
    };

    public bool Contains(BackupMessageItemViewModel item) => contains(item);

    /// <summary>
    /// All backups, then per environment its topics (each followed by its subscriptions) and its queues.
    /// Topics and queues are kept apart per environment: the same name in two environments is two groups.
    /// </summary>
    public static IReadOnlyList<BackupGroupItemViewModel> Build(IReadOnlyCollection<BackupMessageItemViewModel> items)
    {
        var groups = new List<BackupGroupItemViewModel>
        {
            new("all", BackupGroupKind.All, "All backups", items.Count, 0, Tone.Neutral, _ => true)
        };

        foreach (var environment in items
                     .GroupBy(item => item.Summary.ProfileId)
                     .OrderBy(group => group.First().ProfileName, StringComparer.OrdinalIgnoreCase))
        {
            var profileId = environment.Key;
            var first = environment.First();
            groups.Add(new BackupGroupItemViewModel(
                $"env:{profileId}", BackupGroupKind.Environment, $"{first.ProfileName} · {first.EnvironmentLabel}",
                environment.Count(), 0, first.EnvironmentTone,
                item => item.Summary.ProfileId == profileId));

            foreach (var topic in environment
                         .Where(item => item.Summary.Source.Kind == ServiceBusEntityKind.Subscription)
                         .GroupBy(item => item.Summary.Source.TopicName!, StringComparer.Ordinal)
                         .OrderBy(group => group.Key, StringComparer.OrdinalIgnoreCase))
            {
                var topicName = topic.Key;
                groups.Add(new BackupGroupItemViewModel(
                    $"topic:{profileId}:{topicName}", BackupGroupKind.Topic, topicName, topic.Count(), 1, Tone.Accent,
                    item => item.Summary.ProfileId == profileId &&
                            item.Summary.Source.Kind == ServiceBusEntityKind.Subscription &&
                            item.Summary.Source.TopicName == topicName));

                foreach (var subscription in topic
                             .GroupBy(item => item.Summary.Source.Name, StringComparer.Ordinal)
                             .OrderBy(group => group.Key, StringComparer.OrdinalIgnoreCase))
                {
                    var subscriptionName = subscription.Key;
                    groups.Add(new BackupGroupItemViewModel(
                        $"sub:{profileId}:{topicName}:{subscriptionName}", BackupGroupKind.Subscription, subscriptionName,
                        subscription.Count(), 2, Tone.Neutral,
                        item => item.Summary.ProfileId == profileId &&
                                item.Summary.Source.Kind == ServiceBusEntityKind.Subscription &&
                                item.Summary.Source.TopicName == topicName &&
                                item.Summary.Source.Name == subscriptionName));
                }
            }

            foreach (var queue in environment
                         .Where(item => item.Summary.Source.Kind == ServiceBusEntityKind.Queue)
                         .GroupBy(item => item.Summary.Source.Name, StringComparer.Ordinal)
                         .OrderBy(group => group.Key, StringComparer.OrdinalIgnoreCase))
            {
                var queueName = queue.Key;
                groups.Add(new BackupGroupItemViewModel(
                    $"queue:{profileId}:{queueName}", BackupGroupKind.Queue, queueName, queue.Count(), 1, Tone.Accent,
                    item => item.Summary.ProfileId == profileId &&
                            item.Summary.Source.Kind == ServiceBusEntityKind.Queue &&
                            item.Summary.Source.Name == queueName));
            }
        }

        return groups;
    }
}
