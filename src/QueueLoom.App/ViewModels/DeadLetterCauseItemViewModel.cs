using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>
/// One cause among the listed dead letters: a reason and the shape of its error description, for example
/// "Order {n} was not found · 412". A click ticks exactly those messages.
/// </summary>
public sealed class DeadLetterCauseItemViewModel(DeadLetterCause cause, bool isSelected, System.Windows.Input.ICommand? select = null)
{
    public DeadLetterCause Cause { get; } = cause;

    public System.Windows.Input.ICommand? Select { get; } = select;

    public bool IsSelected { get; } = isSelected;

    /// <summary>The description pattern; the reason alone when the messages have no description.</summary>
    public string Text => Cause.Pattern ?? Cause.Reason;

    public int Count => Cause.Count;

    public string Label => $"{Text} · {Count:N0}";

    public string ToolTip =>
        $"Reason: {Cause.Reason}" +
        (Cause.Example is null ? string.Empty : $"\nFor example: {Cause.Example}") +
        (IsSelected ? $"\n\nUntick these {Count:N0} message(s)" : $"\n\nTick these {Count:N0} message(s), and only those");

    public bool Matches(MessageItemViewModel message) => message.CauseKey == (Cause.Reason, Cause.Pattern);
}
