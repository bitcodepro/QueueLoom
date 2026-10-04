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
        foreach (var property in applicationProperties)
        {
            _text[property.Name] = property.Value;
            if (property.WireType is { } wire)
            {
                _wireTypes[property.Name] = wire;
            }
        }
    }

    private readonly Dictionary<string, string> _wireTypes = new(StringComparer.Ordinal);

    /// <summary>The service's own type label of an application property (for example SNS "String.Array"), if any.</summary>
    public string? WireTypeOf(string name) => _wireTypes.GetValueOrDefault(name);

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

    /// <summary>The body as text, for SNS filter policies on the message body; null when it is not known.</summary>
    public string? Body { get; init; }

    public static RoutingMessage From(MessageDraft draft) =>
        new(draft.Properties, draft.ApplicationProperties) { Body = draft.Body.Format == MessageBodyFormat.Base64 ? null : draft.Body.Content };

    public static RoutingMessage From(BrowsedMessage message) => message.IsBodyTruncated
        ? new(message.Properties, message.ApplicationProperties)
        : From(message.CreateDraft());

    /// <summary>
    /// The attributes SNS and Pub/Sub see, as QueueLoom sends them: CorrelationId, Subject, ContentType, ReplyTo and To
    /// when set, then every application property under its exact name (names are case-sensitive there).
    /// </summary>
    public IReadOnlyDictionary<string, object?> Attributes
    {
        get
        {
            if (_attributes is null)
            {
                var attributes = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var (name, value) in MessageAttributeConventions.StandardAttributes(Properties))
                {
                    attributes[name] = value;
                }
                foreach (var (name, value) in _user)
                {
                    attributes[name] = value;
                }
                _attributes = attributes;
            }
            return _attributes;
        }
    }

    private Dictionary<string, object?>? _attributes;

    /// <summary>The text each application property was given, where the message came with it (SNS and Pub/Sub send that text).</summary>
    private readonly Dictionary<string, string> _text = new(StringComparer.Ordinal);

    /// <summary>The text an application property was given, when the message was built from typed text.</summary>
    public bool TryGetText(string name, out string text) => _text.TryGetValue(name, out text!);

    /// <summary>The text of an attribute (see <see cref="Attributes"/>) as SNS and Pub/Sub would carry it.</summary>
    public string AttributeTextOf(string name) =>
        _user.ContainsKey(name) && _text.TryGetValue(name, out var text)
            ? text
            : AttributeText(Attributes.GetValueOrDefault(name));

    /// <summary>A value as the attribute text Pub/Sub carries: text as it is, anything else in QueueLoom's invariant form.</summary>
    public static string AttributeText(object? value) => value switch
    {
        null => string.Empty,
        string text => text,
        bool flag => flag ? "true" : "false",
        _ => ApplicationPropertyValues.FromObject(string.Empty, value).Value
    };

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
