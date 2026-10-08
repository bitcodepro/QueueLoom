using QueueLoom.Core;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Mcp;

namespace QueueLoom.Tests;

// Long texts are cut to a number of UTF-16 characters for display, error messages, MCP results and the approval
// dialog. Cutting between the two halves of a character outside the Basic Multilingual Plane (an emoji, many CJK
// extensions) left a lone surrogate: shown as a broken glyph, or written into JSON as an unpaired \uD83D escape that a
// strict MCP client rejects. Every such cut now keeps the pair whole.
public sealed class TextHeadTests
{
    private const string Rocket = "\U0001F680"; // two UTF-16 characters

    private static bool HasLoneSurrogate(string text)
    {
        for (var index = 0; index < text.Length; index++)
        {
            if (char.IsHighSurrogate(text[index]))
            {
                if (index + 1 >= text.Length || !char.IsLowSurrogate(text[index + 1])) return true;
                index++;
            }
            else if (char.IsLowSurrogate(text[index])) return true;
        }
        return false;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void TheHeadNeverEndsInsideAPair(int lead)
    {
        var text = new string('a', lead) + Rocket + Rocket + "z";

        for (var maximum = 0; maximum <= text.Length; maximum++)
        {
            var head = TextLimits.Head(text, maximum);
            Assert.True(head.Length <= maximum);
            Assert.False(HasLoneSurrogate(head), $"maximum {maximum}: '{head}'");
            Assert.StartsWith(head, text, StringComparison.Ordinal);
            // At most one character shorter than asked, and only to keep a pair whole.
            Assert.True(head.Length >= maximum - 1);
        }
        Assert.Same(text, TextLimits.Head(text, text.Length + 5));
    }

    [Fact]
    public void AnExceptionSummaryCutAtAPairStaysWhole()
    {
        var message = new string('a', 599) + Rocket + "tail";

        var summary = SensitiveDataRedactor.SummarizeException(new InvalidOperationException(message));

        Assert.False(HasLoneSurrogate(summary));
        Assert.EndsWith("…", summary, StringComparison.Ordinal);
    }

    [Fact]
    public void AnMcpBodyCutAtAPairStaysWhole()
    {
        var body = new string('a', 3_999) + Rocket + "tail";
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1,
            System.Text.Encoding.UTF8.GetBytes(body), new EditableMessageProperties(MessageId: "m"));

        var info = McpMapping.ToInfo(message);

        Assert.True(info.BodyTruncated);
        Assert.False(HasLoneSurrogate(info.Body!));
        // Serialized for the client, the text holds no unpaired escape either.
        Assert.DoesNotContain("\\uD83D\"", System.Text.Json.JsonSerializer.Serialize(info.Body), StringComparison.Ordinal);
    }
}
