namespace QueueLoom.Core.ServiceBus;

public enum DeadLetterDisposition
{
    KeepOriginal,
    CompleteAfterSuccessfulSend
}
