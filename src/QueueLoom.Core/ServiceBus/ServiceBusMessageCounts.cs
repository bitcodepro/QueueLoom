namespace QueueLoom.Core.ServiceBus;

public sealed record ServiceBusMessageCounts
{
    public ServiceBusMessageCounts(
        long active = 0,
        long deadLetter = 0,
        long scheduled = 0,
        long transfer = 0,
        long transferDeadLetter = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(active);
        ArgumentOutOfRangeException.ThrowIfNegative(deadLetter);
        ArgumentOutOfRangeException.ThrowIfNegative(scheduled);
        ArgumentOutOfRangeException.ThrowIfNegative(transfer);
        ArgumentOutOfRangeException.ThrowIfNegative(transferDeadLetter);

        Active = active;
        DeadLetter = deadLetter;
        Scheduled = scheduled;
        Transfer = transfer;
        TransferDeadLetter = transferDeadLetter;
    }

    public long Active { get; }

    public long DeadLetter { get; }

    public long Scheduled { get; }

    public long Transfer { get; }

    public long TransferDeadLetter { get; }

    public long Total => checked(Active + DeadLetter + Scheduled + Transfer + TransferDeadLetter);

    public static ServiceBusMessageCounts Empty { get; } = new();

    public static ServiceBusMessageCounts Sum(IEnumerable<ServiceBusMessageCounts> counts)
    {
        ArgumentNullException.ThrowIfNull(counts);

        long active = 0;
        long deadLetter = 0;
        long scheduled = 0;
        long transfer = 0;
        long transferDeadLetter = 0;

        foreach (var count in counts)
        {
            ArgumentNullException.ThrowIfNull(count);
            active = checked(active + count.Active);
            deadLetter = checked(deadLetter + count.DeadLetter);
            scheduled = checked(scheduled + count.Scheduled);
            transfer = checked(transfer + count.Transfer);
            transferDeadLetter = checked(transferDeadLetter + count.TransferDeadLetter);
        }

        return new ServiceBusMessageCounts(
            active,
            deadLetter,
            scheduled,
            transfer,
            transferDeadLetter);
    }
}
