using System.Globalization;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Routing;

/// <summary>What subscription rules can see of a message: its system properties and its typed application properties.</summary>
public sealed class RoutingMessage
{
    private readonly Dictionary<string, object?> _user;

    public RoutingMessage(EditableMessageProperties properties, IEnumerable<MessageApplicationProperty> applicationProperties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        ArgumentNullException.ThrowIfNull(applicationProperties);
        Properties = properties;
        _user = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var property in applicationProperties)
        {
            _user[property.Name] = Typed(property);
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

    public object? User(string name, out bool exists)
    {
        exists = _user.TryGetValue(name, out var value);
        return value;
    }

    private static object? Typed(MessageApplicationProperty property)
    {
        var value = property.Value;
        return property.Type switch
        {
            ApplicationPropertyType.Boolean => bool.TryParse(value, out var flag) ? flag : value,
            ApplicationPropertyType.Byte or ApplicationPropertyType.SByte or ApplicationPropertyType.Int16 or ApplicationPropertyType.UInt16 or
                ApplicationPropertyType.Int32 or ApplicationPropertyType.UInt32 or ApplicationPropertyType.Int64 or ApplicationPropertyType.UInt64 =>
                long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integer) ? integer : value,
            ApplicationPropertyType.Single or ApplicationPropertyType.Double or ApplicationPropertyType.Decimal =>
                double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var real) ? real : value,
            _ => value
        };
    }
}
