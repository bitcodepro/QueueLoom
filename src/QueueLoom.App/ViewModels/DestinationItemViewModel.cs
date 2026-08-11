using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class DestinationItemViewModel(ServiceBusEntityReference reference)
{
    public ServiceBusEntityReference Reference { get; } = reference;

    public string Name => Reference.DisplayName;

    public string KindLabel => Reference.Kind == ServiceBusEntityKind.Queue ? "QUEUE" : "TOPIC";

    public override string ToString() => Name;
}
