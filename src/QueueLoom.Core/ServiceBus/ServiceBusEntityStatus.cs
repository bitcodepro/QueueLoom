namespace QueueLoom.Core.ServiceBus;

public enum ServiceBusEntityStatus
{
    Unknown,
    Active,
    Disabled,
    SendDisabled,
    ReceiveDisabled,
    Creating,
    Deleting,
    Renaming,
    Restoring
}
