namespace QueueLoom.App.Models;

/// <summary>Explorer ordering. <see cref="Hierarchy"/> keeps subscriptions under their topic.</summary>
public enum EntitySortColumn
{
    Hierarchy,
    Name,
    Status,
    Active,
    DeadLetters,
    TransferDeadLetters,
    Scheduled
}
