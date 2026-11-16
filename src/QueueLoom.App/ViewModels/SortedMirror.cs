using System.Collections.ObjectModel;
using System.Collections.Specialized;

namespace QueueLoom.App.ViewModels;

/// <summary>
/// A copy of a collection for display, in the source's order or sorted. Items that arrive while a sort is on go to
/// their sorted place, after any equal ones, so a running scan does not reshuffle what is already shown.
/// </summary>
public sealed class SortedMirror<T> where T : class
{
    private readonly ObservableCollection<T> _source;
    private Comparison<T>? _comparison;

    public SortedMirror(ObservableCollection<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        _source = source;
        View = new ObservableCollection<T>(source);
        source.CollectionChanged += OnSourceChanged;
    }

    public ObservableCollection<T> View { get; }

    /// <summary>Sorts the view by the comparison (null for the source's order).</summary>
    public void SortBy(Comparison<T>? comparison, bool descending)
    {
        _comparison = comparison is null ? null : descending ? (left, right) => comparison(right, left) : comparison;
        Rebuild();
    }

    private void OnSourceChanged(object? sender, NotifyCollectionChangedEventArgs change)
    {
        switch (change.Action)
        {
            case NotifyCollectionChangedAction.Add when change.NewItems is { Count: 1 } && change.NewItems[0] is T item:
                View.Insert(_comparison is null ? change.NewStartingIndex : SortedPosition(item), item);
                break;
            case NotifyCollectionChangedAction.Remove when change.OldItems is { Count: 1 } && change.OldItems[0] is T item:
                View.Remove(item);
                break;
            default:
                Rebuild();
                break;
        }
    }

    // After the last item that sorts at or before the new one.
    private int SortedPosition(T item)
    {
        int low = 0, high = View.Count;
        while (low < high)
        {
            var middle = (low + high) / 2;
            if (_comparison!(View[middle], item) <= 0) low = middle + 1; else high = middle;
        }
        return low;
    }

    private void Rebuild()
    {
        // OrderBy is stable: equal items stay in the source's order.
        var ordered = _comparison is null ? _source.ToList() : _source.Order(Comparer<T>.Create(_comparison)).ToList();
        View.Clear();
        foreach (var item in ordered)
        {
            View.Add(item);
        }
    }
}
