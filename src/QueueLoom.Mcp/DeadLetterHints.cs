namespace QueueLoom.Mcp;

/// <summary>What well-known dead-letter reasons mean, so an assistant knows where to look next.</summary>
internal static class DeadLetterHints
{
    public static string? For(string reason) => reason switch
    {
        "MaxDeliveryCountExceeded" =>
            "The consumer received the message as many times as allowed and never completed it: it threw, timed out or abandoned it. " +
            "The error description, if set, is the consumer's; otherwise look at the consumer's logs around the enqueued time.",
        "TTLExpiredException" =>
            "The message expired before anyone completed it: the consumer was down or too slow, or the time to live is too short.",
        "HeaderSizeExceeded" => "The message's properties were too large for Service Bus.",
        "SessionIdIsMissing" => "The message was sent to a session-enabled entity without a SessionId.",
        "TransferHopCountExceeded" =>
            "The message was forwarded more than 4 times; check the forwarding chain with trace_forwarding for a loop.",
        "MessageSizeExceeded" => "The message is larger than the entity accepts.",
        "Moved by the redrive policy" =>
            "SQS moved the message after maxReceiveCount receives without a delete: the consumer failed or exceeded the visibility timeout.",
        "rejected" => "A RabbitMQ consumer rejected or nacked the message without requeueing it.",
        "expired" => "The message's or the queue's time to live ran out in RabbitMQ.",
        "maxlen" => "The RabbitMQ queue reached its length limit and dropped its oldest message here.",
        "delivery_limit" => "The quorum queue's delivery limit was reached: the consumer kept returning the message.",
        _ when reason.EndsWith("Exception", StringComparison.Ordinal) =>
            $"The consumer threw {reason}; the error description holds its message.",
        _ => null
    };
}
