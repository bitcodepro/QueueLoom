using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Namespace topology, entity filtering and runtime counters.</summary>
public sealed partial class MainWindowViewModel
{
    public EntityItemViewModel? SelectedEntity
    {
        get => _selectedEntity;
        set
        {
            if (SetProperty(ref _selectedEntity, value))
            {
                OnPropertyChanged(nameof(HasSelectedEntity));
                NotifyBrowseDeliveryNotes();
                OnPropertyChanged(nameof(MonitorTargetPreview));
                NotifyCommandStates();
            }
        }
    }

    public bool HasSelectedEntity => SelectedEntity is not null;

    public string SearchText
    {
        get => _searchText;
        set
        {
            if (SetProperty(ref _searchText, value))
            {
                ApplyEntityFilter();
            }
        }
    }

    public string LastUpdatedText => _lastUpdated.HasValue
        ? $"Updated {_lastUpdated.Value.ToLocalTime():HH:mm:ss}"
        : "Not updated yet";

    public int QueueCount => _topology?.Queues.Count ?? 0;

    public int TopicCount => _topology?.Topics.Count ?? 0;

    public int SubscriptionCount => _topology?.Topics.Sum(topic => topic.Subscriptions.Count) ?? 0;

    public long ActiveMessageCount => _topology?.AggregateMessageCounts.Active ?? 0;

    public long DeadLetterCount => (_topology?.AggregateMessageCounts.DeadLetter ?? 0)
                                   + (_topology?.AggregateMessageCounts.TransferDeadLetter ?? 0);
    public long GlobalDlqSourceCount => DeadLetterSources.Sum(source => source.Count);

    /// <summary>The listed total is no more certain than its least certain row.</summary>
    public QueueLoom.Core.Monitoring.DeadLetterCountQuality GlobalDlqCountQuality =>
        QueueLoom.Core.Monitoring.DeadLetterCountQualities.Combine(DeadLetterSources.Select(source => source.CountQuality));

    private async Task RefreshTopologyAsync(CancellationToken cancellationToken)
    {
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true, cancellationToken)
            .ConfigureAwait(true);
        ApplyTopology(topology);
        StatusText = "Topology and runtime counters refreshed";
        AddActivity("Info", "Topology refreshed", $"{QueueCount} queues · {TopicCount} topics");
    }

    private void ApplyTopology(ServiceBusTopology topology, bool preserveDestination = true)
    {
        var previousDestination = preserveDestination ? SelectedDestination?.Reference : null;
        var previousReplayDestination = preserveDestination ? ReplayDestination?.Reference : null;
        _topology = topology;
        NotifyBrowseDeliveryNotes();
        OnPropertyChanged(nameof(CanDeleteSelectedMessages));
        OnPropertyChanged(nameof(ShowDeleteMarkedMessages));
        DeleteMarkedMessagesCommand?.NotifyCanExecuteChanged();
        _lastUpdated = topology.FetchedAt;
        _allEntities.Clear();

        foreach (var queue in topology.Queues)
        {
            _allEntities.Add(new EntityItemViewModel(
                queue.Reference,
                queue.Runtime,
                queue.Status,
                queue.RequiresSession,
                indent: 0,
                queue.Note,
                queueKindName: topology.QueueKindName,
                consumers: queue.Consumers));
        }

        foreach (var topic in topology.Topics)
        {
            _allEntities.Add(new EntityItemViewModel(
                topic.Reference,
                topic.Runtime,
                topic.Status,
                requiresSession: false,
                indent: 0,
                topic.Note,
                topology.TopicKindName));
            foreach (var subscription in topic.Subscriptions)
            {
                _allEntities.Add(new EntityItemViewModel(
                    subscription.Reference,
                    subscription.Runtime,
                    subscription.Status,
                    subscription.RequiresSession,
                    indent: 1,
                    subscription.Note));
            }
        }

        Destinations.Clear();
        foreach (var destination in topology.SendDestinations.OrderBy(item => item.DisplayName, StringComparer.OrdinalIgnoreCase))
        {
            Destinations.Add(new DestinationItemViewModel(destination));
        }
        SelectedDestination = previousDestination is null
            ? null
            : Destinations.FirstOrDefault(item => item.Reference == previousDestination);
        ReplayDestination = previousReplayDestination is null
            ? null
            : Destinations.FirstOrDefault(item => item.Reference == previousReplayDestination);
        ApplyEntityFilter();
        RefreshConsumerRows();
        NotifyStatistics();
    }

    private void ApplyEntityFilter()
    {
        var query = SearchText.Trim();
        var selection = SelectedEntity?.Reference;
        Entities.Clear();
        foreach (var item in OrderEntities(_allEntities.Where(item =>
                     query.Length == 0 ||
                     item.Reference.DisplayName.Contains(query, StringComparison.OrdinalIgnoreCase) ||
                     item.KindLabel.Contains(query, StringComparison.OrdinalIgnoreCase))))
        {
            Entities.Add(item);
        }
        SelectedEntity = selection is null
            ? Entities.FirstOrDefault()
            : Entities.FirstOrDefault(item => item.Reference == selection) ?? Entities.FirstOrDefault();
    }

    private Task BrowseSelectedEntityAsync(ServiceBusSubQueue subQueue, CancellationToken cancellationToken)
    {
        var entity = SelectedEntity ?? throw new InvalidOperationException("Select a queue or subscription first.");
        var profile = GetConnectedProfileItem();
        return BrowseAsync(profile, entity.Reference, subQueue, cancellationToken);
    }

    private void NotifyStatistics()
    {
        OnPropertyChanged(nameof(GlobalDlqDisplay));
        OnPropertyChanged(nameof(UsesSampledCounts));
        OnPropertyChanged(nameof(SupportsTransferDeadLetter));
        OnPropertyChanged(nameof(QueueCount));
        OnPropertyChanged(nameof(TopicCount));
        OnPropertyChanged(nameof(SubscriptionCount));
        OnPropertyChanged(nameof(ActiveMessageCount));
        OnPropertyChanged(nameof(DeadLetterCount));
        OnPropertyChanged(nameof(GlobalDlqSourceCount));
        OnPropertyChanged(nameof(VisibleDlqSourceCount));
        OnPropertyChanged(nameof(VisibleDlqSourceRowCount));
        OnPropertyChanged(nameof(LastUpdatedText));
    }
}
