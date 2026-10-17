namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// A message saved from the Composer to start new messages from. It keeps what the operator typed, not a Message ID or
/// a scheduled time: each message made from it gets a new ID and is sent when the operator chooses.
/// </summary>
public sealed record ComposerTemplate(
    string Name,
    string Body,
    MessageBodyFormat BodyFormat,
    string ContentType = "",
    string Subject = "",
    string CorrelationId = "",
    string To = "",
    string ReplyTo = "",
    string SessionId = "",
    string ReplyToSessionId = "",
    string PartitionKey = "",
    string TimeToLiveSeconds = "",
    string ApplicationProperties = "{}")
{
    public const int MaximumNameLength = 80;

    /// <summary>The name trimmed, or an error when it is empty or too long.</summary>
    public static string NormalizeName(string? name)
    {
        var trimmed = name?.Trim() ?? string.Empty;
        if (trimmed.Length == 0)
        {
            throw new InvalidOperationException("Give the template a name.");
        }
        if (trimmed.Length > MaximumNameLength || trimmed.Any(char.IsControl))
        {
            throw new InvalidOperationException(
                $"A template name is at most {MaximumNameLength} characters, without line breaks or control characters.");
        }
        return trimmed;
    }
}

/// <summary>Templates saved on this computer. Names are compared without regard to case.</summary>
public interface IComposerTemplateStore
{
    Task<IReadOnlyList<ComposerTemplate>> ListAsync(CancellationToken cancellationToken = default);

    /// <summary>Saves the template, replacing one with the same name.</summary>
    Task SaveAsync(ComposerTemplate template, CancellationToken cancellationToken = default);

    Task DeleteAsync(string name, CancellationToken cancellationToken = default);
}
