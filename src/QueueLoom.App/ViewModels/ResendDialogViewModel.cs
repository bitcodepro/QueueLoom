using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>A destination choice in the resend dialog; no reference means "back to where each message came from".</summary>
public sealed record ResendDestinationOption(string Label, ServiceBusEntityReference? Reference);

public sealed record ResendOptions(ServiceBusEntityReference? Destination, ResendMode Mode, int MessagesPerSecond);

/// <summary>Options for resending the ticked messages: destination, copy or move, and pace.</summary>
public sealed class ResendDialogViewModel : ObservableObject
{
    public const int DefaultMessagesPerSecond = 10;

    private ResendDestinationOption _destination;
    private bool _moves;
    private int _messagesPerSecond = DefaultMessagesPerSecond;
    private string _typedConfirmation = string.Empty;

    public ResendDialogViewModel(
        IReadOnlyList<BrowsedMessage> messages,
        IEnumerable<ServiceBusEntityReference> destinations,
        string environmentName,
        bool requiresTypedConfirmation,
        bool canRemoveOriginals = true)
    {
        CanRemoveOriginals = canRemoveOriginals;
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            throw new ArgumentException("Choose at least one message.", nameof(messages));
        }

        Messages = messages;
        EnvironmentName = environmentName;
        RequiresTypedConfirmation = requiresTypedConfirmation;
        DestinationOptions =
        [
            new ResendDestinationOption("Back to where each message came from", null),
            .. destinations.Select(reference => new ResendDestinationOption(
                $"{(reference.Kind == ServiceBusEntityKind.Topic ? "Topic" : "Queue")} · {reference.DisplayName}", reference))
        ];
        _destination = DestinationOptions[0];
    }

    public IReadOnlyList<BrowsedMessage> Messages { get; }

    public string EnvironmentName { get; }

    public string Title => Messages.Count == 1 ? "Resend 1 message" : $"Resend {Messages.Count:N0} messages";

    public IReadOnlyList<ResendDestinationOption> DestinationOptions { get; }

    public ResendDestinationOption Destination
    {
        get => _destination;
        set
        {
            if (value is not null && SetProperty(ref _destination, value))
            {
                OnPropertyChanged(nameof(Summary));
            }
        }
    }

    /// <summary>False where the service cannot remove single messages (Kafka).</summary>
    public bool CanRemoveOriginals { get; }

    /// <summary>Only dead-lettered messages can be removed after sending, and only where the service allows it.</summary>
    public bool CanMove => CanRemoveOriginals && Messages.All(message => message.IsDeadLetter);

    public string MoveUnavailableReason => CanRemoveOriginals
        ? "Only dead-lettered messages can be moved; untick active messages to enable it."
        : "Kafka keeps every message until its retention ends, so originals cannot be removed one by one.";

    public bool Moves
    {
        get => _moves;
        set
        {
            if (SetProperty(ref _moves, value && CanMove))
            {
                OnPropertyChanged(nameof(Copies));
                OnPropertyChanged(nameof(Summary));
                OnPropertyChanged(nameof(ConfirmLabel));
            }
        }
    }

    public bool Copies
    {
        get => !Moves;
        set => Moves = !value;
    }

    public int MessagesPerSecond
    {
        get => _messagesPerSecond;
        set => SetProperty(ref _messagesPerSecond, Math.Clamp(value, 1, 100));
    }

    public bool RequiresTypedConfirmation { get; }

    public string TypedConfirmation
    {
        get => _typedConfirmation;
        set
        {
            if (SetProperty(ref _typedConfirmation, value))
            {
                OnPropertyChanged(nameof(CanConfirm));
            }
        }
    }

    public bool CanConfirm => !RequiresTypedConfirmation ||
                              string.Equals(TypedConfirmation.Trim(), EnvironmentName, StringComparison.Ordinal);

    public string ConfirmLabel => Moves ? "Send and remove originals" : "Send copies";

    public string Summary
    {
        get
        {
            var sources = Messages
                .GroupBy(message => message.Source.DisplayName)
                .Select(group => $"• {group.Key}: {group.Count():N0}")
                .Take(8);
            var targets = Destination.Reference is { } reference
                ? [reference]
                : Messages.Select(message => DeadLetterResender.OriginalDestination(message.Source)).Distinct().ToArray();
            var fanOut = targets.Any(target => target.Kind == ServiceBusEntityKind.Topic)
                ? "\n\nA topic delivers each copy to every subscription whose filter matches, not only to the one it came from."
                : string.Empty;
            var mode = Moves
                ? "Every message is sent first. Then the originals that were sent are backed up and removed from their " +
                  "dead-letter queues. If a send fails, that original stays where it is."
                : "Copies are sent; the originals stay where they are.";
            return $"Environment: {EnvironmentName}\n{string.Join("\n", sources)}\n\n{mode}{fanOut}";
        }
    }

    public ResendOptions ToOptions() => new(Destination.Reference, Moves ? ResendMode.Move : ResendMode.Copy, MessagesPerSecond);
}
