using System.Net;
using System.Text;
using QueueLoom.App.Services;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Mcp;

namespace QueueLoom.Tests;

/// <summary>Gap 4: a release candidate was never offered its final release (the assembly version has no pre-release tag).</summary>
public sealed class PrereleaseUpdateCheckTests
{
    private const string FinalRelease = """
        [{"tag_name":"v1.0.0","draft":false,"prerelease":false},{"tag_name":"v1.0.0-rc.2","draft":false,"prerelease":true}]
        """;

    [Theory]
    [InlineData("1.0.0-rc.1")]
    [InlineData("1.0.0-rc.1+3f2a9c1")]
    public async Task AReleaseCandidateIsOfferedItsFinalRelease(string running)
    {
        using var client = new HttpClient(new StubHandler(FinalRelease));
        using var checker = new GitHubUpdateChecker(client, running);

        var result = await checker.CheckAsync();

        Assert.NotNull(result);
        Assert.Equal(new Version(1, 0, 0), result.Version);
        Assert.Equal("v1.0.0", result.Tag);
    }

    [Theory]
    [InlineData("1.0.0")]
    [InlineData("1.0.0+3f2a9c1")]
    [InlineData("1.0.1-rc.1")]
    public async Task TheFinalReleaseIsNotOfferedToItselfOrToALaterCandidate(string running)
    {
        using var client = new HttpClient(new StubHandler(FinalRelease));
        using var checker = new GitHubUpdateChecker(client, running);

        Assert.Null(await checker.CheckAsync());
    }

    private sealed class StubHandler(string json) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json) });
    }
}

/// <summary>Gap 6: MCP replies capped bodies at 4,000 characters but returned properties and dead-letter texts in full.</summary>
public sealed class McpMessageInfoCapTests
{
    private static BrowsedMessage LongDeadLetter()
    {
        var stackTrace = new string('s', 50_000);
        return new BrowsedMessage(
            ServiceBusEntityReference.Queue("orders"),
            ServiceBusSubQueue.DeadLetter,
            7,
            Encoding.UTF8.GetBytes("{}"),
            new EditableMessageProperties(MessageId: "m-7"),
            [new MessageApplicationProperty("kafka_dlt-exception-stacktrace", ApplicationPropertyType.String, stackTrace),
             new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "contoso")],
            deadLetterReason: new string('r', 9_000),
            deadLetterErrorDescription: stackTrace);
    }

    private static BrowsedMessage ManyProperties() => new(
        ServiceBusEntityReference.Queue("orders"),
        ServiceBusSubQueue.DeadLetter,
        8,
        Encoding.UTF8.GetBytes("body"),
        new EditableMessageProperties(MessageId: "m-8"),
        Enumerable.Range(0, 80).Select(index =>
            new MessageApplicationProperty($"p{index:D2}", ApplicationPropertyType.String, "v")));

    [Fact]
    public void LongPropertiesAndDeadLetterTextsAreCapped()
    {
        var info = McpMapping.ToInfo(LongDeadLetter());

        Assert.Equal(1_000, info.ApplicationProperties["kafka_dlt-exception-stacktrace"].Length);
        Assert.Equal("contoso", info.ApplicationProperties["tenant"]);
        Assert.Equal(4_000, info.DeadLetterDescription!.Length);
        Assert.Equal(4_000, info.DeadLetterReason!.Length);
        Assert.False(info.BodyTruncated);
    }

    [Fact]
    public void LongPropertiesAndDeadLetterTextsSayTheyWereCut()
    {
        var info = McpMapping.ToInfo(LongDeadLetter());

        Assert.True(info.ApplicationPropertiesTruncated);
        Assert.True(info.DeadLetterTextTruncated);
        Assert.False(info.DecodedBodyTruncated);
    }

    [Fact]
    public void PropertiesBeyondTheFirstFiftyAreLeftOut()
    {
        Assert.Equal(50, McpMapping.ToInfo(ManyProperties()).ApplicationProperties.Count);
    }

    [Fact]
    public void PropertiesBeyondTheFirstFiftySayTheyWereLeftOut()
    {
        Assert.True(McpMapping.ToInfo(ManyProperties()).ApplicationPropertiesTruncated);
    }

    [Fact]
    public void ShortFieldsAreReturnedWholeWithoutTruncationFlags()
    {
        var message = new BrowsedMessage(
            ServiceBusEntityReference.Queue("orders"),
            ServiceBusSubQueue.DeadLetter,
            9,
            Encoding.UTF8.GetBytes("body"),
            new EditableMessageProperties(MessageId: "m-9"),
            [new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "contoso")],
            deadLetterReason: "MaxDeliveryCountExceeded",
            deadLetterErrorDescription: "Ten deliveries failed");

        var info = McpMapping.ToInfo(message);

        Assert.Equal("contoso", info.ApplicationProperties["tenant"]);
        Assert.Equal("Ten deliveries failed", info.DeadLetterDescription);
        Assert.False(info.ApplicationPropertiesTruncated);
        Assert.False(info.DeadLetterTextTruncated);
    }
}
