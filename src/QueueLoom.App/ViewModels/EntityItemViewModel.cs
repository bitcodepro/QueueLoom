using System.Globalization;
using Avalonia;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class EntityItemViewModel
{
    public EntityItemViewModel(
        ServiceBusEntityReference reference,
        ServiceBusEntityRuntime runtime,
        ServiceBusEntityStatus status,
        bool requiresSession,
        int indent,
        string? note = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(indent);
        Reference = reference;
        Runtime = runtime;
        Status = status;
        RequiresSession = requiresSession;
        Indent = indent;
        Note = note;
    }

    /// <summary>A provider-specific remark, for example "Dead-letter queue of orders" or "Pushes to https://…".</summary>
    public string? Note { get; }

    /// <summary>The second line under the name: the note when there is one, else the parent topic.</summary>
    public string Detail => Note ?? ParentPath;

    public bool HasDetail => !string.IsNullOrEmpty(Detail);

    public ServiceBusEntityReference Reference { get; }

    public ServiceBusEntityRuntime Runtime { get; }

    public ServiceBusEntityStatus Status { get; }

    public bool RequiresSession { get; }

    public int Indent { get; }

    public Thickness IndentMargin => new(Indent * 18, 0, 0, 0);

    public string Name => Reference.Kind == ServiceBusEntityKind.Subscription
        ? Reference.Name
        : Reference.DisplayName;

    public string ParentPath => Reference.Kind == ServiceBusEntityKind.Subscription
        ? Reference.TopicName ?? string.Empty
        : string.Empty;

    public bool IsQueue => Reference.Kind == ServiceBusEntityKind.Queue;

    public bool IsTopic => Reference.Kind == ServiceBusEntityKind.Topic;

    public bool IsSubscription => Reference.Kind == ServiceBusEntityKind.Subscription;

    public string KindLabel => Reference.Kind switch
    {
        ServiceBusEntityKind.Queue => "QUEUE",
        ServiceBusEntityKind.Topic => "TOPIC",
        ServiceBusEntityKind.Subscription => "SUBSCRIPTION",
        _ => "ENTITY"
    };

    public string KindName => KindLabel.ToLowerInvariant();

    public string StatusLabel => Status.ToString();

    public long Active => Runtime.MessageCounts.Active;

    public long DeadLetters => Runtime.MessageCounts.DeadLetter;

    public long TransferDeadLetters => Runtime.MessageCounts.TransferDeadLetter;

    public long Scheduled => Runtime.MessageCounts.Scheduled;
    // "—" means the service does not report this number, which is different from zero.
    public string ActiveDisplay => Runtime.CountsUnavailable ? "—" : Active.ToString("N0", CultureInfo.CurrentCulture);
    public string DeadLettersDisplay => Runtime.CountsUnavailable ? "—" : DeadLetters.ToString("N0", CultureInfo.CurrentCulture);
    public string ScheduledDisplay => Runtime.IsEmulatorSample || Runtime.CountsUnavailable
        ? "—"
        : Scheduled.ToString("N0", CultureInfo.CurrentCulture);
    public string TransferDeadLettersDisplay => HasTransferDeadLetterCount
        ? TransferDeadLetters.ToString("N0", CultureInfo.CurrentCulture)
        : "—";

    private bool HasTransferDeadLetterCount =>
        !Runtime.IsEmulatorSample && !Runtime.CountsUnavailable && Runtime.HasTransferDeadLetterCount;

    public bool HasDeadLetters => DeadLetters > 0;

    public bool HasTransferDeadLetters => HasTransferDeadLetterCount && TransferDeadLetters > 0;

    public bool HasActiveMessages => Active > 0;

    public bool HasScheduledMessages => !Runtime.IsEmulatorSample && Scheduled > 0;

    public string SessionLabel => RequiresSession ? "Sessions" : string.Empty;

    public bool CanBrowse => Reference.CanBrowse;

    public bool CanSend => Reference.CanSend;
}
