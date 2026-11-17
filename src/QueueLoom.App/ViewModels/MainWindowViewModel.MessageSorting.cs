using System.Collections.ObjectModel;
using QueueLoom.App.Commands;

namespace QueueLoom.App.ViewModels;

public sealed partial class MainWindowViewModel
{
    private readonly MessageListSort _messageSort = new();
    private SortedMirror<MessageItemViewModel>? _messageMirror;
    private RelayCommand<string>? _sortMessagesCommand;

    /// <summary>The message list as shown: <see cref="Messages"/> in arrival order, or sorted by a column.</summary>
    public ObservableCollection<MessageItemViewModel> MessageList => (_messageMirror ??= new(Messages)).View;

    /// <summary>Sorts by the named column (a <see cref="MessageSortColumn"/>): ascending, descending, then off.</summary>
    public RelayCommand<string> SortMessagesCommand => _sortMessagesCommand ??= new(name =>
    {
        if (!Enum.TryParse<MessageSortColumn>(name, out var column))
        {
            return;
        }
        _messageSort.Toggle(column);
        (_messageMirror ??= new(Messages)).SortBy(_messageSort.Comparison, _messageSort.Descending);
        foreach (var header in new[]
                 {
                     nameof(SequenceHeader), nameof(EnvironmentHeader), nameof(SourceHeader), nameof(MessageHeader),
                     nameof(ReasonHeader), nameof(EnqueuedHeader)
                 })
        {
            OnPropertyChanged(header);
        }
    });

    public string SequenceHeader => _messageSort.Header(MessageSortColumn.Sequence);
    public string EnvironmentHeader => _messageSort.Header(MessageSortColumn.Environment);
    public string SourceHeader => _messageSort.Header(MessageSortColumn.Source);
    public string MessageHeader => _messageSort.Header(MessageSortColumn.Message);
    public string ReasonHeader => _messageSort.Header(MessageSortColumn.Reason);
    public string EnqueuedHeader => _messageSort.Header(MessageSortColumn.Enqueued);
}
