using System.Text.Json.Serialization;

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
    /// <summary>
    /// The host to connect to: without surrounding spaces, and an IPv6 address without the brackets it may be written
    /// in ([::1]), which a name lookup would not understand. Derived, so it is not serialized: profiles and the
    /// configuration identity hashed from them (scheduled resends, recoverable operations) keep their earlier shape.
    /// </summary>
    [JsonIgnore]
    public string HostName
    {
        get
        {
            var host = Host.Trim();
            return host.Length > 2 && host[0] == '[' && host[^1] == ']' ? host[1..^1] : host;
        }
    }

    /// <summary>The management plugin's base URL. An IPv6 address is bracketed there, as URLs require (http://[::1]:15672/).</summary>
    public Uri ManagementUri => new UriBuilder(UseTls ? "https" : "http", HostName, ManagementPort, "/").Uri;
}
