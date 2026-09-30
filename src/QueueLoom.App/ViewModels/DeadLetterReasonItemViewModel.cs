namespace QueueLoom.App.ViewModels;

/// <summary>One dead-letter reason among the listed messages, e.g. "MaxDeliveryCountExceeded · 30".</summary>
public sealed class DeadLetterReasonItemViewModel(
    string reason,
    int count,
    bool isSelected,
    System.Windows.Input.ICommand? select = null)
{
    /// <summary>Ticks (or unticks) this reason's messages; set by the page's view model.</summary>
    public System.Windows.Input.ICommand? Select { get; } = select;

    public const string NoReason = "(no reason)";

    public string Reason { get; } = reason;

    public int Count { get; } = count;

    /// <summary>Every message with this reason is ticked.</summary>
    public bool IsSelected { get; } = isSelected;

    public string Label => $"{Reason} · {Count:N0}";

    public string ToolTip => IsSelected
        ? $"Untick the {Count:N0} message(s) with this reason"
        : $"Tick the {Count:N0} message(s) with this reason, and only those";

    public static string ReasonOf(MessageItemViewModel message) =>
        string.IsNullOrWhiteSpace(message.Message.DeadLetterReason) ? NoReason : message.Message.DeadLetterReason.Trim();
}
