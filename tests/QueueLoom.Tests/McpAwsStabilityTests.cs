using System.Reflection;
using QueueLoom.Infrastructure.Aws;

namespace QueueLoom.Tests;

public sealed class McpAwsStabilityTests
{
    private static int? MaxReceiveCount(string? policy) =>
        (int?)typeof(AwsSqsSnsWorkspace)
            .GetMethod("MaxReceiveCount", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [policy]);

    [Theory]
    [InlineData("not json")]
    [InlineData("{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:1:dlq\",\"maxReceiveCount\":null}")]
    [InlineData("{\"maxReceiveCount\":\"many\"}")]
    [InlineData("{\"maxReceiveCount\":99999999999}")]
    [InlineData("[1]")]
    public void UnreadableRedrivePolicy_DoesNotBreakQueueSettings(string policy) =>
        Assert.Null(MaxReceiveCount(policy));

    [Theory]
    [InlineData("{\"maxReceiveCount\":5}", 5)]
    [InlineData("{\"maxReceiveCount\":\"7\"}", 7)]
    public void ReadableRedrivePolicy_StillReadsCount(string policy, int expected) =>
        Assert.Equal(expected, MaxReceiveCount(policy));
}
