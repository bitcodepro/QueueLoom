using System.Globalization;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Routing;

/// <summary>What subscription rules can see of a message: its system properties and its typed application properties.</summary>
public sealed class RoutingMessage
{
    private readonly Dictionary<string, object?> _user;

    public RoutingMessage(EditableMessageProperties properties, IEnumerable<MessageApplicationProperty> applicationProperties)
        : this(properties, (applicationProperties ?? throw new ArgumentNullException(nameof(applicationProperties)))
            .Select(property => new KeyValuePair<string, object?>(property.Name, Typed(property))))
    {
    }

    /// <summary>A message whose application properties are already typed values (text, numbers, Guid, dates…).</summary>
    public RoutingMessage(EditableMessageProperties properties, IEnumerable<KeyValuePair<string, object?>> applicationProperties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        ArgumentNullException.ThrowIfNull(applicationProperties);
        Properties = properties;
        _user = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (name, value) in applicationProperties)
        {
            _user[name] = value;
        }
    }

    public EditableMessageProperties Properties { get; }

    public IReadOnlyDictionary<string, object?> UserProperties => _user;

    public static RoutingMessage From(MessageDraft draft) => new(draft.Properties, draft.ApplicationProperties);

    public static RoutingMessage From(BrowsedMessage message) => new(message.Properties, message.ApplicationProperties);

    /// <summary>A system property by its SQL filter name (sys.Label, sys.MessageId…).</summary>
    public object? System(string name, out bool exists)
    {
        var value = name.ToUpperInvariant() switch
        {
            "MESSAGEID" => Properties.MessageId,
            "CORRELATIONID" => Properties.CorrelationId,
            "TO" => Properties.To,
            "REPLYTO" => Properties.ReplyTo,
            "LABEL" or "SUBJECT" => Properties.Subject,
            "SESSIONID" => Properties.SessionId,
            "REPLYTOSESSIONID" => Properties.ReplyToSessionId,
            "CONTENTTYPE" => Properties.ContentType,
            "PARTITIONKEY" => Properties.PartitionKey,
            _ => throw new SqlFilterNotSupportedException(
                $"sys.{name} is only known once Service Bus accepts the message, so QueueLoom cannot check it beforehand.")
        };
        exists = value is not null;
        return value;
    }

    /// <summary>
    /// An application property by name. Service Bus matches property names without regard to case (values keep it),
    /// so "Region" finds "region"; an exact match wins when a message has both.
    /// </summary>
    public object? User(string name, out bool exists)
    {
        if (_user.TryGetValue(name, out var value))
        {
            exists = true;
            return value;
        }
        foreach (var (key, candidate) in _user)
        {
            if (string.Equals(key, name, StringComparison.OrdinalIgnoreCase))
            {
                exists = true;
                return candidate;
            }
        }
        exists = false;
        return null;
    }

    /// <summary>The value as Service Bus would carry it, with the same conversion used for sending.</summary>
    public static object? Typed(MessageApplicationProperty property)
    {
        try
        {
            return ApplicationPropertyValues.ToObject(property);
        }
        catch (FormatException)
        {
            // A value that does not fit its type cannot be sent either; compare it as the text it is.
            return property.Value;
        }
    }
}
