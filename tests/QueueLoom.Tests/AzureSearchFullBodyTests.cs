using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class AzureSearchFullBodyTests
{
    // Larger than the 1 MiB the message list retains, as a Premium dead letter can be.
    private static ServiceBusReceivedMessage LargeJson(string status) => ServiceBusModelFactory.ServiceBusReceivedMessage(
        body: BinaryData.FromString("{\"padding\":\"" + new string('x', 2 * 1024 * 1024) + "\",\"order\":{\"status\":\"" + status + "\"}}"),
        messageId: "large", contentType: "application/json");

    [Fact]
    public void AJsonConditionSeesAFieldBeyondTheRetainedPartOfTheBody()
    {
        Assert.True(AzureServiceBusWorkspace.MatchesSearch(LargeJson("failed"), MessageSearchQuery.Parse("$.order.status == 'failed'")));
        Assert.False(AzureServiceBusWorkspace.MatchesSearch(LargeJson("shipped"), MessageSearchQuery.Parse("$.order.status == 'failed'")));
    }

    [Fact]
    public void TextBeyondTheRetainedPartOfTheBodyIsFound()
    {
        Assert.True(AzureServiceBusWorkspace.MatchesSearch(LargeJson("ORD-1042-failed"), MessageSearchQuery.Parse("ORD-1042")));
    }
}
