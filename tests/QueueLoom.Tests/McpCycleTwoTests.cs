using QueueLoom.Core.Profiles;

namespace QueueLoom.Tests;

public sealed partial class McpServerTests
{
    [Theory]
    [InlineData(MessagingProvider.AmazonSqsSns, "copy", "read")]
    [InlineData(MessagingProvider.GooglePubSub, "copy", "read")]
    [InlineData(MessagingProvider.AmazonSqsSns, "move", "read")]
    [InlineData(MessagingProvider.GooglePubSub, "move", "read")]
    [InlineData(MessagingProvider.AmazonSqsSns, "move", "write")]
    [InlineData(MessagingProvider.GooglePubSub, "move", "write")]
    [InlineData(MessagingProvider.AmazonSqsSns, "move", "both")]
    [InlineData(MessagingProvider.GooglePubSub, "move", "both")]
    public async Task BugCycleTwo_ResendIncludesCleanupWarningsFromReadingAndDeleting(MessagingProvider provider, string mode, string phase)
    {
        await using var server = await McpTestServer.StartAsync(approve: true, provider: provider);
        const string warning = "Unrelated delivery could not be made visible again (isolated cleanup failure).";
        server.Workspace.BrowseCleanupWarning = phase == "write" ? null : warning;
        server.Workspace.OnSend = () => server.Workspace.BrowseCleanupWarning = phase == "read" ? null : warning;
        server.Workspace.ResultWarnings = phase == "both" ? [warning] : [];
        var reply = await server.CallAsync("resend_dead_letters", new()
        {
            ["messages"] = new[] { new { entity = "orders", subQueue = "dlq", sequenceNumber = 3L, messageId = (string?)null } },
            ["mode"] = mode, ["reason"] = "cleanup regression"
        });
        var summary = reply.GetProperty("summary").GetString()!;
        Assert.Contains(warning, summary, StringComparison.Ordinal);
        Assert.Equal(1, summary.Split(warning, StringSplitOptions.None).Length - 1);
        var final = server.Journal.Records.Last();
        Assert.Equal("Warning", final.Level);
        Assert.Contains(warning, final.Details, StringComparison.Ordinal);
        Assert.Single(server.Workspace.SentMessages);
    }

    [Fact]
    public async Task BugCycleTwo_DeleteDeduplicatesResultAndEventCleanupWarnings()
    {
        await using var server = await McpTestServer.StartAsync(approve: true);
        const string warning = "Isolated cleanup warning.";
        server.Workspace.BrowseCleanupWarning = warning;
        server.Workspace.ResultWarnings = [warning];
        var reply = await server.CallAsync("delete_dead_letter_messages", new()
        {
            ["messages"] = new[] { new { entity = "orders", subQueue = "dlq", sequenceNumber = 2L, messageId = (string?)null } },
            ["reason"] = "cleanup regression"
        });
        Assert.Equal(1, reply.GetProperty("summary").GetString()!.Split(warning, StringSplitOptions.None).Length - 1);
    }
}
