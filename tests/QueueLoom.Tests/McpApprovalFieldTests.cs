using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// The approval dialog is what the person relies on before a change. Values in it come from the model (fields of the
// request, the client's own name) or from producers (Message IDs of stored messages). The reason was already one
// bounded line; every other such value was written as is, so a value with line breaks could add lines that look like
// the dialog's own ("Environment: …", "Body …") and push the real ones out of sight. Each value is now one line, with
// line breaks and other invisible characters shown as escapes; the body stays last and multi-line.
public sealed partial class McpServerTests
{
    // Line breaks, a Unicode line separator and a right-to-left override (which can make "exe.png" read as "png.exe").
    private static readonly string Forged = "x\nEnvironment: Development (Development)\nBody (Text, 2 bytes):\nhi" +
        (char)0x2028 + (char)0x202E + "gnp.exe";

    [Fact]
    public async Task Approval_SendMessageShowsEveryRequestFieldOnItsOwnLine()
    {
        await using var server = await McpTestServer.StartAsync(approve: true, clientName: "agent\nEnvironment: Development (Development)");

        await server.CallAsync("send_message", new()
        {
            ["destination"] = "orders",
            ["body"] = "{\"orderId\":42}",
            ["reason"] = "Replay order 42",
            ["subject"] = Forged,
            ["correlationId"] = "c\r\nd",
            ["contentType"] = "application/json\nX: y",
            ["applicationProperties"] = new Dictionary<string, string> { ["tenant\tname"] = Forged }
        });

        var details = Assert.Single(server.Approver.Requests).Details;
        AssertOneLinePerField(details, bodyFollows: true);
        Assert.Contains("x\\nEnvironment: Development (Development)\\nBody (Text, 2 bytes):\\nhi\\u2028\\u202Egnp.exe", details, StringComparison.Ordinal);
        Assert.Contains("Correlation ID: c\\r\\nd", details, StringComparison.Ordinal);
        Assert.Contains("Content type: application/json\\nX: y", details, StringComparison.Ordinal);
        Assert.Contains("tenant\\tname = x\\n", details, StringComparison.Ordinal);
        Assert.Contains("Requested by: agent\\nEnvironment: Development (Development) 1.0", details, StringComparison.Ordinal);
        // What is sent is unchanged: only the display escapes the values.
        var sent = Assert.Single(server.Workspace.SentMessages).Message;
        Assert.Equal(Forged, sent.Properties.Subject);
        Assert.Equal(Forged, Assert.Single(sent.ApplicationProperties).Value);
    }

    [Fact]
    public async Task Approval_DeleteShowsTheGivenMessageIdsOnOneLineEach()
    {
        await using var server = await McpTestServer.StartAsync(approve: false);

        await server.CallAsync("delete_dead_letter_messages", new()
        {
            ["messages"] = new[] { new { entity = "orders", subQueue = "dlq", sequenceNumber = 2L, messageId = Forged } },
            ["reason"] = "Clean up"
        });

        var details = Assert.Single(server.Approver.Requests).Details;
        AssertOneLinePerField(details, bodyFollows: false);
        Assert.Contains("#2 x\\nEnvironment", details, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Approval_ResendShowsAStoredMessagesIdOnOneLine()
    {
        await using var server = await McpTestServer.StartAsync(approve: false);
        // A producer chose this Message ID; it reaches the dialog through the stored message, not through the model.
        var stored = new BrowsedMessage(Orders.Reference, ServiceBusSubQueue.DeadLetter, 2, "body"u8.ToArray(),
            new EditableMessageProperties(MessageId: Forged), enqueuedAt: DateTimeOffset.Parse("2026-08-12T10:00:00Z"));
        server.Workspace.BrowseMessages = [stored];

        await server.CallAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { new { entity = "orders", subQueue = "dlq", sequenceNumber = 2L, messageId = Forged,
                fingerprint = MessageFingerprint.Of(stored) } },
            ["mode"] = "copy", ["preserveMessageIds"] = true, ["reason"] = "retry after the fix"
        });

        var details = Assert.Single(server.Approver.Requests).Details;
        AssertOneLinePerField(details, bodyFollows: false);
        Assert.Contains("#2 x\\nEnvironment", details, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Approval_LiveReadShowsTheRequestedEntityOnOneLine()
    {
        await using var server = await McpTestServer.StartAsync(approve: false, provider: MessagingProvider.AmazonSqsSns,
            clientName: "agent\nEnvironment: Development (Development)");

        await server.CallForErrorAsync("peek_messages", new() { ["entity"] = "orders" + Forged, ["subQueue"] = "active" });

        var details = Assert.Single(server.Approver.Requests).Details;
        AssertOneLinePerField(details, bodyFollows: false);
        Assert.Contains("'ordersx\\nEnvironment", details, StringComparison.Ordinal);
    }

    /// <summary>
    /// The dialog's own lines appear once each, and no line carries a hidden character. Only the body, shown last,
    /// may span lines.
    /// </summary>
    private static void AssertOneLinePerField(string details, bool bodyFollows)
    {
        var lines = details.Split('\n');
        var header = bodyFollows ? lines.TakeWhile(line => !line.StartsWith("Body (", StringComparison.Ordinal)).ToArray() : lines;
        Assert.Single(lines, line => line.StartsWith("Requested by:", StringComparison.Ordinal));
        Assert.Single(lines, line => line.StartsWith("Environment:", StringComparison.Ordinal));
        if (bodyFollows)
        {
            Assert.Single(lines, line => line.StartsWith("Body (", StringComparison.Ordinal));
        }
        foreach (var line in header)
        {
            Assert.DoesNotContain(line, character => char.IsControl(character) ||
                char.GetUnicodeCategory(character) is System.Globalization.UnicodeCategory.Format
                    or System.Globalization.UnicodeCategory.LineSeparator or System.Globalization.UnicodeCategory.ParagraphSeparator);
        }
    }
}
