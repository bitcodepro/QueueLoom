namespace QueueLoom.Core.Profiles;

public enum ProxyMode
{
    /// <summary>The computer's proxy settings, as before proxies could be set per environment.</summary>
    System,

    /// <summary>Connect directly, ignoring the computer's proxy.</summary>
    None,

    /// <summary>A proxy given here, optionally with a user name; its password is kept in the vault.</summary>
    Custom
}

/// <summary>
/// The proxy an environment's HTTP and WebSocket connections go through. Kafka brokers and RabbitMQ's AMQP port speak
/// their own protocols over TCP and connect directly whatever is set here.
/// </summary>
public sealed record ProxySettings(ProxyMode Mode, string? Address = null, string? UserName = null);
