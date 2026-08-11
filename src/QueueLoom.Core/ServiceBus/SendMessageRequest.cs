namespace QueueLoom.Core.ServiceBus;

public sealed record SendMessageRequest
{
    public SendMessageRequest(ServiceBusEntityReference destination, MessageDraft message)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(message);

        if (!destination.CanSend)
        {
            throw new ArgumentException("Messages can only be sent to queues or topics.", nameof(destination));
        }

        Destination = destination;
        Message = message;
    }

    public ServiceBusEntityReference Destination { get; }

    public MessageDraft Message { get; }
}
