namespace QueueLoom.App.ViewModels;

public enum MessageSortColumn
{
    None,
    Sequence,
    Environment,
    Source,
    Message,
    Reason,
    Enqueued
}

/// <summary>Which column the message list is sorted by: a click sorts ascending, then descending, then not at all.</summary>
public sealed class MessageListSort
{
    public MessageSortColumn Column { get; private set; }

    public bool Descending { get; private set; }

    public void Toggle(MessageSortColumn column)
    {
        if (column != Column)
        {
            (Column, Descending) = (column, false);
        }
        else if (!Descending)
        {
            Descending = true;
        }
        else
        {
            (Column, Descending) = (MessageSortColumn.None, false);
        }
    }

    public string Header(MessageSortColumn column)
    {
        var name = column switch
        {
            MessageSortColumn.Sequence => "SEQ",
            MessageSortColumn.Environment => "ENV",
            MessageSortColumn.Source => "SOURCE",
            MessageSortColumn.Message => "MESSAGE",
            MessageSortColumn.Reason => "REASON",
            MessageSortColumn.Enqueued => "ENQUEUED",
            _ => string.Empty
        };
        return column != Column ? name : name + (Descending ? " ▼" : " ▲");
    }

    public Comparison<MessageItemViewModel>? Comparison => Column switch
    {
        MessageSortColumn.Sequence => (left, right) => left.SequenceNumber.CompareTo(right.SequenceNumber),
        MessageSortColumn.Environment => (left, right) => string.Compare(left.EnvironmentLabel, right.EnvironmentLabel, StringComparison.OrdinalIgnoreCase),
        MessageSortColumn.Source => (left, right) => string.Compare(left.SourceDisplay, right.SourceDisplay, StringComparison.OrdinalIgnoreCase),
        MessageSortColumn.Message => (left, right) => string.Compare(left.MessageId, right.MessageId, StringComparison.OrdinalIgnoreCase),
        MessageSortColumn.Reason => (left, right) => string.Compare(left.StatusDetail, right.StatusDetail, StringComparison.OrdinalIgnoreCase),
        // Messages without an enqueue time sort first.
        MessageSortColumn.Enqueued => (left, right) => Nullable.Compare(left.Message.EnqueuedAt, right.Message.EnqueuedAt),
        _ => null
    };
}
