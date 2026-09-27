namespace QueueLoom.Core.Profiles;

/// <summary>The cloud messaging service an environment connects to.</summary>
public enum MessagingProvider
{
    AzureServiceBus,

    /// <summary>Amazon SQS queues and Amazon SNS topics in one AWS account and region.</summary>
    AmazonSqsSns,

    GooglePubSub
}

public static class MessagingProviders
{
    public static string DisplayName(this MessagingProvider provider) => provider switch
    {
        MessagingProvider.AzureServiceBus => "Azure Service Bus",
        MessagingProvider.AmazonSqsSns => "Amazon SQS / SNS",
        MessagingProvider.GooglePubSub => "Google Cloud Pub/Sub",
        _ => provider.ToString()
    };

    public static string ShortName(this MessagingProvider provider) => provider switch
    {
        MessagingProvider.AzureServiceBus => "AZURE",
        MessagingProvider.AmazonSqsSns => "AWS",
        MessagingProvider.GooglePubSub => "GCP",
        _ => provider.ToString().ToUpperInvariant()
    };
}
