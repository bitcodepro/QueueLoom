using System.Text;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class MessageComparisonAuditRegressionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ComparisonAudit_EqualDisplayedPrefixDoesNotEstablishWholeBodyEquality(bool binary)
    {
        var (left, right) = BeyondPreview(binary);

        var result = MessageComparison.Compare(left, right);

        Assert.True(result.BodyTruncated);
        Assert.Equal(0, result.ChangedLines);
        Assert.Equal(0, result.ChangedProperties);
        Assert.False(result.AreEqual);
    }

    [Fact]
    public void ComparisonAudit_DialogDisclosesIncompleteComparisonWhenNoVisibleDifferenceExists()
    {
        var (left, right) = BeyondPreview(binary: false);
        var dialog = new CompareDialogViewModel(new MessageItemViewModel(left), new MessageItemViewModel(right));

        Assert.Contains("incomplete", dialog.Summary, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("are the same", dialog.Summary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(MessageComparison.MaximumLines.ToString("N0"), dialog.Summary, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("ReplyToSessionId")]
    [InlineData("TransactionPartitionKey")]
    [InlineData("ScheduledEnqueueTime")]
    [InlineData("AmqpType")]
    [InlineData("AmqpAppId")]
    public void ComparisonAudit_EditableBrokerFieldsAreNotOmitted(string property)
    {
        var first = Properties(property, "first");
        var second = Properties(property, "other");
        var result = MessageComparison.Compare(Message("same", first), Message("same", second));

        Assert.False(result.AreEqual);
        Assert.Equal(1, result.ChangedProperties);
        Assert.Single(result.Properties, item => item.Differs);
    }

    [Fact]
    public void ComparisonAudit_ApplicationPropertyTypeChangeIsVisible()
    {
        var left = Message("same", application: [new("attempt", ApplicationPropertyType.String, "42")]);
        var right = Message("same", application: [new("attempt", ApplicationPropertyType.Int32, "42")]);

        var result = MessageComparison.Compare(left, right);

        Assert.False(result.AreEqual);
        var change = Assert.Single(result.Properties, item => item.Differs);
        Assert.Contains("String", change.Left);
        Assert.Contains("Int32", change.Right);
    }

    [Fact]
    public void ComparisonAudit_ApplicationNameCannotHideBrokerMessageIdDifference()
    {
        var left = Message("same", new(MessageId: "first"), [new("Message ID", ApplicationPropertyType.String, "constant")]);
        var right = Message("same", new(MessageId: "other"), [new("Message ID", ApplicationPropertyType.String, "constant")]);

        var result = MessageComparison.Compare(left, right);

        Assert.False(result.AreEqual);
        Assert.Contains(result.Properties, item => item.Left == "first" && item.Right == "other" && item.Differs);
        Assert.Contains(result.Properties, item => item.Name.Contains("Message ID", StringComparison.Ordinal) && !item.Differs);
    }

    [Fact]
    public void ComparisonAudit_CompleteIdenticalMessagesRemainEqual()
    {
        var body = string.Join('\n', Enumerable.Repeat("same", MessageComparison.MaximumLines));
        var message = Message(body, new(ReplyToSessionId: "session", AmqpAppId: "app"),
            [new("attempt", ApplicationPropertyType.Int32, "42")]);

        var result = MessageComparison.Compare(message, message);

        Assert.False(result.BodyTruncated);
        Assert.True(result.AreEqual);
    }

    private static (BrowsedMessage Left, BrowsedMessage Right) BeyondPreview(bool binary)
    {
        if (!binary)
        {
            var prefix = string.Join('\n', Enumerable.Repeat("same", MessageComparison.MaximumLines));
            return (Message(prefix + "\nfirst"), Message(prefix + "\nother"));
        }
        var left = Enumerable.Repeat((byte)0xFF, (MessageComparison.MaximumLines + 1) * 16).ToArray();
        var right = left.ToArray();
        right[^1] = 0xFE;
        return (Message(left), Message(right));
    }

    private static EditableMessageProperties Properties(string name, string value) => name switch
    {
        "ReplyToSessionId" => new(ReplyToSessionId: value),
        "TransactionPartitionKey" => new(TransactionPartitionKey: value),
        "ScheduledEnqueueTime" => new(ScheduledEnqueueTime: DateTimeOffset.Parse(value == "first"
            ? "2026-10-04T10:00:00.100Z" : "2026-10-04T10:00:00.200Z")),
        "AmqpType" => new(AmqpType: value),
        "AmqpAppId" => new(AmqpAppId: value),
        _ => throw new ArgumentException("Unsupported test case", nameof(name))
    };

    private static BrowsedMessage Message(string body, EditableMessageProperties? properties = null,
        MessageApplicationProperty[]? application = null) => Message(Encoding.UTF8.GetBytes(body), properties, application);

    private static BrowsedMessage Message(byte[] body, EditableMessageProperties? properties = null,
        MessageApplicationProperty[]? application = null) => new(ServiceBusEntityReference.Queue("orders"),
        ServiceBusSubQueue.DeadLetter, 1, body, properties ?? EditableMessageProperties.Empty, application);
}
