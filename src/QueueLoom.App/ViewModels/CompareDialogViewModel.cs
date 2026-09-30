using System.Globalization;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed record DiffLineViewModel(DiffLine Line)
{
    public string Marker => Line.Kind switch { DiffKind.Removed => "−", DiffKind.Added => "+", _ => " " };

    public string LeftNumber => Line.LeftNumber?.ToString(CultureInfo.CurrentCulture) ?? string.Empty;

    public string RightNumber => Line.RightNumber?.ToString(CultureInfo.CurrentCulture) ?? string.Empty;

    public string Text => Line.Text;

    public bool IsRemoved => Line.Kind == DiffKind.Removed;

    public bool IsAdded => Line.Kind == DiffKind.Added;
}

/// <summary>Two messages side by side: which body lines and which properties differ.</summary>
public sealed class CompareDialogViewModel
{
    public CompareDialogViewModel(MessageItemViewModel left, MessageItemViewModel right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        Result = MessageComparison.Compare(left.Message, right.Message);
        LeftTitle = Describe(left);
        RightTitle = Describe(right);
        Lines = Result.BodyLines.Select(line => new DiffLineViewModel(line)).ToArray();
    }

    public MessageComparisonResult Result { get; }

    public string LeftTitle { get; }

    public string RightTitle { get; }

    public IReadOnlyList<DiffLineViewModel> Lines { get; }

    public IReadOnlyList<PropertyDifference> Properties => Result.Properties;

    public bool HasBodyChanges => Result.ChangedLines > 0;

    public string Summary => Result.AreEqual
        ? "The two messages are the same: body and properties."
        : $"{Count(Result.ChangedLines, "body line")} and {Count(Result.ChangedProperties, "property", "properties")} differ." +
          (Result.BodyTruncated ? $" Only the first {MessageComparison.MaximumLines:N0} lines of each body are compared." : string.Empty);

    private static string Describe(MessageItemViewModel message) =>
        $"{message.MessageId} · {message.SourceDisplay}" + (string.IsNullOrEmpty(message.EnqueuedAt) ? string.Empty : $" · {message.EnqueuedAt}");

    private static string Count(int count, string one, string? many = null) =>
        count == 1 ? $"1 {one}" : $"{count.ToString("N0", CultureInfo.CurrentCulture)} {many ?? one + "s"}";
}
