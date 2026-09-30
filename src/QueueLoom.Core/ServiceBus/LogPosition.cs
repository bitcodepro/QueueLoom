namespace QueueLoom.Core.ServiceBus;

/// <summary>Where a message sits in a partitioned log such as a Kafka topic.</summary>
public readonly record struct LogPosition(int Partition, long Offset);
