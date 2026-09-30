namespace QueueLoom.Core.Profiles;

/// <summary>Non-secret settings of a RabbitMQ environment; the password is kept in the secret vault.</summary>
/// <param name="Host">Host name or IP address of the broker, for example rabbit.internal or localhost.</param>
/// <param name="UserName">The RabbitMQ user; it needs the management tag to list queues.</param>
/// <param name="VirtualHost">The virtual host, "/" by default.</param>
/// <param name="AmqpPort">5672, or 5671 with TLS.</param>
/// <param name="ManagementPort">The management plugin's HTTP port: 15672, or 15671 with TLS.</param>
/// <param name="UseTls">Connect with TLS (amqps and https).</param>
public sealed record RabbitMqSettings(
    string Host,
    string UserName,
    string VirtualHost = "/",
    int AmqpPort = 5672,
    int ManagementPort = 15672,
    bool UseTls = false)
{
    public Uri ManagementUri => new($"{(UseTls ? "https" : "http")}://{Host}:{ManagementPort}/");
}
