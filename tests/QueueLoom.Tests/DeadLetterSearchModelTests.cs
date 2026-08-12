using System.Text;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class DeadLetterSearchModelTests
{
    [Fact]
    public void RequestDefaultsToBothDeadLetterSubQueues()
    {
        var request = new DeadLetterSearchRequest(
            "correlation-42",
            [ServiceBusEntityReference.Queue("orders")]);

        Assert.Equal(2, request.SubQueues.Count);
        Assert.Contains(ServiceBusSubQueue.DeadLetter, request.SubQueues);
        Assert.Contains(ServiceBusSubQueue.TransferDeadLetter, request.SubQueues);
        Assert.Equal(DeadLetterSearchRequest.DefaultMaximumMessagesPerSubQueue, request.MaximumMessagesPerSubQueue);
    }

    [Theory]
    [InlineData("correlation-42")]
    [InlineData("MESSAGE-7")]
    [InlineData("order.created")]
    [InlineData("customer-99")]
    [InlineData("tenant-id")]
    [InlineData("northwind")]
    public void MatcherSearchesBrokerBodyAndApplicationProperties(string query)
    {
        var message = Message(
            enqueuedAt: DateTimeOffset.Parse("2026-08-12T10:00:00Z"),
            body: "{\"customer\":\"customer-99\"}",
            properties: new EditableMessageProperties(
                MessageId: "message-7",
                CorrelationId: "correlation-42",
                Subject: "order.created"),
            applicationProperties: [
                new MessageApplicationProperty("tenant-id", ApplicationPropertyType.String, "northwind")]);

        Assert.True(DeadLetterSearchMatcher.IsMatch(message, query));
    }

    [Fact]
    public void ResultBuildsOldestFirstTimelineAndReportsIncompleteSources()
    {
        var source = ServiceBusEntityReference.Queue("orders");
        var newer = Message(DateTimeOffset.Parse("2026-08-12T11:00:00Z"), "newer");
        var older = Message(DateTimeOffset.Parse("2026-08-12T10:00:00Z"), "older");
        var now = DateTimeOffset.UtcNow;
        var result = new DeadLetterSearchResult(
            Guid.NewGuid(),
            now,
            now,
            [new DeadLetterSearchSourceResult(
                source,
                ServiceBusSubQueue.DeadLetter,
                2,
                [newer, older],
                ScanLimitReached: true)]);

        Assert.Equal([older, newer], result.Matches);
        Assert.Equal(2, result.ScannedMessageCount);
        Assert.False(result.IsComplete);
    }

    [Fact]
    public void RequestRejectsTopicsAndBlankQueries()
    {
        Assert.Throws<ArgumentException>(() => new DeadLetterSearchRequest(
            " ",
            [ServiceBusEntityReference.Queue("orders")]));
        Assert.Throws<ArgumentException>(() => new DeadLetterSearchRequest(
            "value",
            [ServiceBusEntityReference.Topic("orders")]));
    }

    private static BrowsedMessage Message(
        DateTimeOffset enqueuedAt,
        string body,
        EditableMessageProperties? properties = null,
        IEnumerable<MessageApplicationProperty>? applicationProperties = null) =>
        new(
            ServiceBusEntityReference.Queue("orders"),
            ServiceBusSubQueue.DeadLetter,
            sequenceNumber: 1,
            Encoding.UTF8.GetBytes(body),
            properties ?? new EditableMessageProperties(),
            applicationProperties,
            enqueuedAt: enqueuedAt);
}
