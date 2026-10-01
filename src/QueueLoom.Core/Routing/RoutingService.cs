namespace QueueLoom.Core.Routing;

/// <summary>The service whose rules decide where a message sent to a topic (or exchange) goes.</summary>
public enum RoutingService
{
    /// <summary>Azure Service Bus: SQL and correlation rules on topic subscriptions.</summary>
    ServiceBus,

    /// <summary>Amazon SNS: a filter policy on a subscription, on its message attributes or its JSON body.</summary>
    Sns,

    /// <summary>Google Cloud Pub/Sub: a filter on a subscription's message attributes, fixed when it is created.</summary>
    PubSub,

    /// <summary>RabbitMQ: the bindings of an exchange to queues and other exchanges.</summary>
    RabbitMq
}

public static class RoutingServiceText
{
    public static string Name(this RoutingService service) => service switch
    {
        RoutingService.Sns => "SNS",
        RoutingService.PubSub => "Pub/Sub",
        RoutingService.RabbitMq => "RabbitMQ",
        _ => "Service Bus"
    };

    /// <summary>"topic", or "exchange" in RabbitMQ.</summary>
    public static string TopicWord(this RoutingService service) => service == RoutingService.RabbitMq ? "exchange" : "topic";

    /// <summary>"subscription", or "destination" (a bound queue or exchange) in RabbitMQ.</summary>
    public static string SubscriptionWord(this RoutingService service) =>
        service == RoutingService.RabbitMq ? "destination" : "subscription";

    /// <summary>What happens to a message that no subscription takes.</summary>
    public static string DropText(this RoutingService service) => service switch
    {
        RoutingService.Sns => "SNS accepts it and drops it without an error.",
        RoutingService.PubSub => "Pub/Sub accepts it and no subscription delivers it.",
        RoutingService.RabbitMq => "RabbitMQ drops it, or returns it to a publisher that set the mandatory flag.",
        _ => "Service Bus accepts it and drops it without an error."
    };

    /// <summary>How rules work in this service, one paragraph for the routing window.</summary>
    public static string Explanation(this RoutingService service) => service switch
    {
        RoutingService.Sns =>
            "A subscription without a filter policy receives every message; one with a policy receives only the messages it matches. " +
            "Attribute names and values are case-sensitive. A message that matches no subscription is dropped without an error.",
        RoutingService.PubSub =>
            "A subscription without a filter receives every message; one with a filter receives only the messages it matches, " +
            "and Pub/Sub acknowledges the others for it. Attribute names and values are case-sensitive. A filter cannot be changed after the subscription is created.",
        RoutingService.RabbitMq =>
            "The exchange copies a message to every queue or exchange with a binding that matches it: direct bindings compare the routing key (Subject), " +
            "topic bindings match it word by word (* is one word, # any number), headers bindings compare headers, fanout takes everything. " +
            "A message no binding takes goes to the alternate exchange, if there is one, or is dropped.",
        _ =>
            "Each subscription gets a copy of a message when at least one of its rules matches. Values are compared case-sensitively; " +
            "property names are not. A message that matches no subscription is dropped without an error."
    };
}
