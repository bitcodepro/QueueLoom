using System.Text;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

public sealed class PullRequest64ReviewRegressionTests
{
    [Fact]
    public void CarriageReturnAndItsVisibleMarkAreDifferentBodies()
    {
        var left = Message("a\rb␍c");
        var right = Message("a␍b\rc");
        Assert.Equal(Encoding.UTF8.GetByteCount("a\rb␍c"), Encoding.UTF8.GetByteCount("a␍b\rc"));

        var result = MessageComparison.Compare(left, right);

        Assert.False(result.AreEqual);
        Assert.NotEqual(0, result.ChangedLines);
        Assert.All(result.BodyLines, line => Assert.DoesNotContain('\r', line.Text));
    }

    [Theory]
    [InlineData("1,000", 1000)]
    [InlineData("100-", -100)]
    [InlineData("1e3", 1000)]
    [InlineData("-2.5E-1", -0.25)]
    [InlineData("12.75", 12.75)]
    public void AcceptedDecimalDraftsAreSentByEveryMapper(string text, double expected)
    {
        var draft = new MessageDraft(new EditableMessageBody("body", MessageBodyFormat.Text),
            applicationProperties: [new("amount", ApplicationPropertyType.Decimal, text)]);

        Assert.True(MessageDraftValidator.Validate(draft).IsValid);
        Assert.Equal((decimal)expected, Assert.IsType<decimal>(AzureMessageMapper.ToAzure(draft).ApplicationProperties["amount"]));
        Assert.Equal((decimal)expected, Assert.IsType<decimal>(RabbitMqMessageMapper.ToAmqp(draft).Headers!["amount"]));
        Assert.Equal((decimal)expected, Assert.IsType<decimal>(RoutingMessage.From(draft).UserProperties["amount"]));
    }

    [Theory]
    [InlineData("0.10000000149011612")]
    [InlineData("0.1")]
    public void RabbitPreviewMatchesTheSingleTheMapperSends(string text)
    {
        var draft = new MessageDraft(new EditableMessageBody("body", MessageBodyFormat.Text),
            applicationProperties: [new("price", ApplicationPropertyType.Single, text)]);
        var sent = Assert.IsType<double>(RabbitMqMessageMapper.ToAmqp(draft).Headers!["price"]);
        var message = RoutingMessage.From(draft);

        foreach (var candidate in new[] { 0.1d, 0.10000000149011612d })
        {
            var rule = new Dictionary<string, object?> { ["x-match"] = "all", ["price"] = candidate };
            Assert.Equal(candidate == sent ? RoutingOutcome.Receives : RoutingOutcome.Skips,
                RabbitBindings.MatchHeaders(rule, message).Outcome);
        }
    }

    private static BrowsedMessage Message(string body) => new(ServiceBusEntityReference.Queue("orders"),
        ServiceBusSubQueue.DeadLetter, 1, Encoding.UTF8.GetBytes(body), EditableMessageProperties.Empty, null);
}
