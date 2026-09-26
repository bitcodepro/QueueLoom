using System.Text;
using QueueLoom.App.Models;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class PresentationTests
{
    [Theory]
    [InlineData(EnvironmentKind.Development, Tone.Accent)]
    [InlineData(EnvironmentKind.Test, Tone.Violet)]
    [InlineData(EnvironmentKind.Production, Tone.Danger)]
    [InlineData(EnvironmentKind.Custom, Tone.Warning)]
    public void EnvironmentTone_IsStablePerEnvironmentKind(EnvironmentKind environment, Tone expected) =>
        Assert.Equal(expected, Tones.ForEnvironment(environment));

    [Theory]
    [InlineData("Production", Tone.Danger)]
    [InlineData("dev", Tone.Accent)]
    [InlineData("Staging", Tone.Warning)]
    public void BackupEnvironmentTone_FollowsTheStoredLabel(string environment, Tone expected) =>
        Assert.Equal(expected, Tones.ForEnvironmentName(environment));

    [Fact]
    public void JsonBody_IsIndentedForDisplayButCopiedVerbatim()
    {
        var item = new MessageItemViewModel(Message("""{"a":1,"b":[true,null]}"""));

        Assert.Equal("""{"a":1,"b":[true,null]}""", item.BodyText);
        Assert.Contains("\n", item.BodyDisplayText, StringComparison.Ordinal);
        Assert.Contains("\"b\": [", item.BodyDisplayText, StringComparison.Ordinal);
    }

    [Fact]
    public void NonJsonBody_IsDisplayedUnchanged()
    {
        var item = new MessageItemViewModel(Message("plain text {not json"));

        Assert.Equal(item.BodyText, item.BodyDisplayText);
    }

    private static BrowsedMessage Message(string body) =>
        new(
            ServiceBusEntityReference.Queue("orders"),
            ServiceBusSubQueue.DeadLetter,
            1,
            Encoding.UTF8.GetBytes(body),
            EditableMessageProperties.Empty);
}
