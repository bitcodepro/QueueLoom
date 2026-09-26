namespace QueueLoom.Core.ServiceBus;

public sealed record ReplayProgress(Guid Id, int Sent, int Total, string Status);
