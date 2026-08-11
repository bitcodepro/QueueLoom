using System.Text;
using System.Text.Json;

namespace QueueLoom.Core.ServiceBus;

public sealed record EditableMessageBody(string Content, MessageBodyFormat Format)
{
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    public static EditableMessageBody Empty { get; } = new(string.Empty, MessageBodyFormat.Text);

    public byte[] GetBytes() => Format switch
    {
        MessageBodyFormat.Text or MessageBodyFormat.Json => Encoding.UTF8.GetBytes(Content),
        MessageBodyFormat.Base64 => Convert.FromBase64String(Content),
        _ => throw new InvalidOperationException($"Unsupported message body format: {Format}.")
    };

    public bool TryGetBytes(out byte[] bytes)
    {
        bytes = [];
        if (Content is null || !Enum.IsDefined(Format))
        {
            return false;
        }

        try
        {
            bytes = GetBytes();
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    public static EditableMessageBody FromBytes(ReadOnlySpan<byte> bytes)
    {
        try
        {
            var text = StrictUtf8.GetString(bytes);
            return new EditableMessageBody(
                text,
                IsJson(text) ? MessageBodyFormat.Json : MessageBodyFormat.Text);
        }
        catch (DecoderFallbackException)
        {
            return new EditableMessageBody(Convert.ToBase64String(bytes), MessageBodyFormat.Base64);
        }
    }

    private static bool IsJson(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        try
        {
            using var _ = JsonDocument.Parse(value);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
