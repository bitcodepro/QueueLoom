using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class EntityItemViewModel
{
    public EntityItemViewModel(
        ServiceBusEntityReference reference,
        ServiceBusEntityRuntime runtime,
        ServiceBusEntityStatus status,
        bool requiresSession,
        int indent)
    {
        Reference = reference;
        Runtime = runtime;
        Status = status;
        RequiresSession = requiresSession;
        Indent = indent;
    }

    public ServiceBusEntityReference Reference { get; }

    public ServiceBusEntityRuntime Runtime { get; }

    public ServiceBusEntityStatus Status { get; }

    public bool RequiresSession { get; }

    public int Indent { get; }

    public string Name => Reference.Kind == ServiceBusEntityKind.Subscription
        ? Reference.Name
        : Reference.DisplayName;

    public string ParentPath => Reference.Kind == ServiceBusEntityKind.Subscription
        ? Reference.TopicName ?? string.Empty
        : string.Empty;

    public string KindLabel => Reference.Kind switch
    {
        ServiceBusEntityKind.Queue => "QUEUE",
        ServiceBusEntityKind.Topic => "TOPIC",
        ServiceBusEntityKind.Subscription => "SUBSCRIPTION",
        _ => "ENTITY"
    };

    public string StatusLabel => Status.ToString();

    public long Active => Runtime.MessageCounts.Active;

    public long DeadLetters => Runtime.MessageCounts.DeadLetter;

    public long TransferDeadLetters => Runtime.MessageCounts.TransferDeadLetter;

    public long Scheduled => Runtime.MessageCounts.Scheduled;

    public string SessionLabel => RequiresSession ? "Sessions" : string.Empty;

    public bool CanBrowse => Reference.CanBrowse;

    public bool CanSend => Reference.CanSend;
}
