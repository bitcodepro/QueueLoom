using System.Text;
using Azure.Messaging.ServiceBus;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class MessageComparisonSourceTruncationTests
{
    [Theory]
    [InlineData("text")]
    [InlineData("json")]
    [InlineData("binary")]
    public void ComparisonAudit_ProviderPreviewCannotEstablishEquality(string format)
    {
        var body = format switch
        {
            "json" => Encoding.UTF8.GetBytes("{\"value\":\"same\"}"),
            "binary" => new byte[] { 0xff, 0x00, 0x80 },
            _ => Encoding.UTF8.GetBytes("same retained single line")
        };
        var left = Message(body, body.Length + 5);
        var right = Message(body, body.Length + 5);

        Assert.True(left.IsBodyTruncated);
        Assert.True(right.IsBodyTruncated);
        var result = MessageComparison.Compare(left, right);
        Assert.True(result.BodyLines.Count < MessageComparison.MaximumLines);
        Assert.Equal(0, result.ChangedLines);
        Assert.Equal(0, result.ChangedProperties);
        Assert.True(result.BodyTruncated);
        Assert.False(result.AreEqual);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void ComparisonAudit_EitherSourcePreviewIsIncompleteEvenWithMatchingNormalizedBodies(bool truncateLeft, bool truncateRight)
    {
        var prefix = Encoding.UTF8.GetBytes("{\"value\":\"same\"}");
        var complete = Encoding.UTF8.GetBytes("{\"value\":\"same\"}  ");
        var left = Message(truncateLeft ? prefix : complete, complete.Length);
        var right = Message(truncateRight ? prefix : complete, complete.Length);

        Assert.Equal(truncateLeft, left.IsBodyTruncated);
        Assert.Equal(truncateRight, right.IsBodyTruncated);
        Assert.Equal(left.BodySize, right.BodySize);
        var result = MessageComparison.Compare(left, right);
        Assert.Equal(0, result.ChangedLines);
        Assert.Equal(0, result.ChangedProperties);
        Assert.True(result.BodyTruncated);
        Assert.False(result.AreEqual);
        var dialog = Dialog(left, right);
        Assert.Contains("incomplete", dialog.Summary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("retained", dialog.Summary, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(MessageComparison.MaximumLines.ToString("N0"), dialog.Summary, StringComparison.Ordinal);
        Assert.DoesNotContain("are the same", dialog.Summary, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ComparisonAudit_AzureMapperDiscardedSingleLineTailsCannotEstablishEquality()
    {
        var prefix = new string('a', AzureMessageMapper.MaxRetainedBodyBytes);
        var first = Encoding.UTF8.GetBytes(prefix + "first");
        var other = Encoding.UTF8.GetBytes(prefix + "other");
        Assert.False(first.AsSpan().SequenceEqual(other));
        BrowsedMessage Map(byte[] body) => AzureMessageMapper.FromAzure(
            ServiceBusModelFactory.ServiceBusReceivedMessage(body: new BinaryData(body), messageId: "same", sequenceNumber: 1),
            ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter);
        var left = Map(first);
        var right = Map(other);

        Assert.Equal(AzureMessageMapper.MaxRetainedBodyBytes, left.Body.Length);
        Assert.Equal(first.Length, left.BodySize);
        Assert.Equal(other.Length, right.BodySize);
        Assert.True(left.Body.Span.SequenceEqual(right.Body.Span));
        Assert.True(left.IsBodyTruncated);
        Assert.True(right.IsBodyTruncated);
        var result = MessageComparison.Compare(left, right);
        Assert.Single(result.BodyLines);
        Assert.Equal(0, result.ChangedLines);
        Assert.Equal(0, result.ChangedProperties);
        Assert.True(result.BodyTruncated);
        Assert.False(result.AreEqual);
        Assert.Contains("retained", Dialog(left, right).Summary, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ComparisonAudit_SourceAndLineTruncationAreBothDisclosed()
    {
        var body = Encoding.UTF8.GetBytes(string.Join('\n', Enumerable.Repeat("same", MessageComparison.MaximumLines + 1)));
        var message = Message(body, body.Length + 5);
        var summary = Dialog(message, message).Summary;

        Assert.Contains("retained", summary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(MessageComparison.MaximumLines.ToString("N0"), summary, StringComparison.Ordinal);
    }

    [Fact]
    public void ComparisonAudit_CompleteSmallBinaryBodyStillEstablishesEquality()
    {
        var message = Message([0xff, 0x00, 0x80], 3);
        var result = MessageComparison.Compare(message, message);

        Assert.False(result.BodyTruncated);
        Assert.True(result.AreEqual);
    }

    private static CompareDialogViewModel Dialog(BrowsedMessage left, BrowsedMessage right) =>
        new(new MessageItemViewModel(left), new MessageItemViewModel(right));

    private static BrowsedMessage Message(byte[] body, long originalSize) => new(ServiceBusEntityReference.Queue("orders"),
        ServiceBusSubQueue.DeadLetter, 1, body, EditableMessageProperties.Empty, originalBodySize: originalSize);
}
