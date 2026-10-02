using System.Text.Json;

namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// Find and replace applied to messages before they are resent: in text and JSON bodies, and in the subject,
/// correlation ID and text application properties. Binary bodies and typed property values are left alone.
/// </summary>
public sealed record MessageRewrite(string Find, string Replacement, bool InBody = true, bool InProperties = false, bool MatchCase = true)
{
    public bool IsEmpty => string.IsNullOrEmpty(Find) || !InBody && !InProperties;

    private StringComparison Comparison => MatchCase ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;

    public MessageDraft Apply(MessageDraft draft) => Apply(draft, out _);

    public MessageDraft Apply(MessageDraft draft, out bool changed)
    {
        ArgumentNullException.ThrowIfNull(draft);
        changed = false;
        if (IsEmpty)
        {
            return draft;
        }

        var body = draft.Body;
        if (InBody && body.Format is MessageBodyFormat.Text or MessageBodyFormat.Json && Replace(body.Content) is { } content)
        {
            body = body with { Content = content };
            changed = true;
        }

        var properties = draft.Properties;
        var applicationProperties = draft.ApplicationProperties;
        if (InProperties)
        {
            if (Replace(properties.Subject) is { } subject)
            {
                properties = properties with { Subject = subject };
                changed = true;
            }
            if (Replace(properties.CorrelationId) is { } correlationId)
            {
                properties = properties with { CorrelationId = correlationId };
                changed = true;
            }
            var rewritten = applicationProperties
                .Select(property => property.Type == ApplicationPropertyType.String && Replace(property.Value) is { } value
                    ? property with { Value = value }
                    : property)
                .ToArray();
            if (!rewritten.SequenceEqual(applicationProperties))
            {
                applicationProperties = rewritten;
                changed = true;
            }
        }

        return changed ? new MessageDraft(body, properties, applicationProperties) { KafkaEnvelope = draft.KafkaEnvelope, LegacyAmqpMetadata = draft.LegacyAmqpMetadata } : draft;
    }

    /// <summary>True when a JSON body is no longer valid JSON after the replacement.</summary>
    public static bool BreaksJson(MessageDraft before, MessageDraft after)
    {
        if (before.Body.Format != MessageBodyFormat.Json || ReferenceEquals(before, after))
        {
            return false;
        }
        try
        {
            using var _ = JsonDocument.Parse(after.Body.Content);
            return false;
        }
        catch (JsonException)
        {
            return true;
        }
    }

    private string? Replace(string? text) =>
        text is not null && text.Contains(Find, Comparison) ? text.Replace(Find, Replacement, Comparison) : null;
}
