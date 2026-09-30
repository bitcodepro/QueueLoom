namespace QueueLoom.Core.Profiles;

/// <summary>The cloud messaging service an environment connects to.</summary>
public enum MessagingProvider
{
    AzureServiceBus,

    /// <summary>Amazon SQS queues and Amazon SNS topics in one AWS account and region.</summary>
    AmazonSqsSns,

    GooglePubSub,

    /// <summary>Queues and exchanges of one RabbitMQ virtual host, read through the management plugin and AMQP.</summary>
    RabbitMq,

    /// <summary>Topics of a Kafka cluster; dead-letter topics are recognised by their name.</summary>
    Kafka
}

public static class MessagingProviders
{
    public static string DisplayName(this MessagingProvider provider) => provider switch
    {
        MessagingProvider.AzureServiceBus => "Azure Service Bus",
        MessagingProvider.AmazonSqsSns => "Amazon SQS / SNS",
        MessagingProvider.GooglePubSub => "Google Cloud Pub/Sub",
        MessagingProvider.RabbitMq => "RabbitMQ",
        MessagingProvider.Kafka => "Apache Kafka",
        _ => provider.ToString()
    };

    public static string ShortName(this MessagingProvider provider) => provider switch
    {
        MessagingProvider.AzureServiceBus => "AZURE",
        MessagingProvider.AmazonSqsSns => "AWS",
        MessagingProvider.GooglePubSub => "GCP",
        MessagingProvider.RabbitMq => "RABBITMQ",
        MessagingProvider.Kafka => "KAFKA",
        _ => provider.ToString().ToUpperInvariant()
    };
}
