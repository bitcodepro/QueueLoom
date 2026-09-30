using System.Collections.ObjectModel;

namespace QueueLoom.App.ViewModels;

/// <summary>Consumers and consumer lag of the connected environment, from the latest topology refresh.</summary>
public sealed partial class MainWindowViewModel
{
    public ObservableCollection<EntityItemViewModel> ConsumerRows { get; } = [];

    public bool HasConsumerRows => ConsumerRows.Count > 0;

    public string ConsumerRowsCaption => _connectedProfile is null
        ? string.Empty
        : $"{_connectedProfile.Name} · refreshed with Explorer ({ConsumerRows.Count(row => row.HasConsumerLag):N0} need attention)";

    /// <summary>Groups that are behind and queues nobody reads come first.</summary>
    private void RefreshConsumerRows()
    {
        ConsumerRows.Clear();
        foreach (var row in _allEntities
                     .Where(item => item.HasConsumers)
                     .OrderByDescending(item => item.HasConsumerLag)
                     .ThenByDescending(item => item.Consumers!.MaximumLag ?? item.Active)
                     .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
                     .Take(50))
        {
            ConsumerRows.Add(row);
        }
        OnPropertyChanged(nameof(HasConsumerRows));
        OnPropertyChanged(nameof(ConsumerRowsCaption));
    }
}
