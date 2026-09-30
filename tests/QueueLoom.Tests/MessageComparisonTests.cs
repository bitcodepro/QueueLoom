using System.Text;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class MessageComparisonTests
{
    [Fact]
    public void Diff_KeepsCommonLinesAndMarksTheRest()
    {
        var lines = MessageComparison.Diff(["a", "b", "c", "d"], ["a", "x", "c", "d", "e"]);

        Assert.Equal(["  a", "- b", "+ x", "  c", "  d", "+ e"], lines.Select(line => line.Kind switch
        {
            DiffKind.Removed => "- ",
            DiffKind.Added => "+ ",
            _ => "  "
        } + line.Text));
        Assert.Equal((3, 3), (lines[3].LeftNumber, lines[3].RightNumber));
    }

    [Fact]
    public void Compare_IgnoresJsonFormattingAndListsDifferingPropertiesFirst()
    {
        var source = ServiceBusEntityReference.Queue("orders");
        var left = new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 1, Encoding.UTF8.GetBytes("""{"id":1,"total":5}"""),
            new EditableMessageProperties(MessageId: "a", Subject: "order"), [new("tenant", ApplicationPropertyType.String, "eu")]);
        var right = new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 2, Encoding.UTF8.GetBytes("{\n  \"id\": 1,\n  \"total\": 7\n}"),
            new EditableMessageProperties(MessageId: "b", Subject: "order"), [new("tenant", ApplicationPropertyType.String, "eu")]);

        var result = MessageComparison.Compare(left, right);

        Assert.Equal(2, result.ChangedLines);
        Assert.Equal(["  \"total\": 5", "  \"total\": 7"], result.BodyLines.Where(line => line.Kind != DiffKind.Same).Select(line => line.Text));
        Assert.Equal(["Body size", "Message ID"], result.Properties.Take(2).Select(property => property.Name));
        Assert.All(result.Properties.Take(2), property => Assert.True(property.Differs));
        Assert.False(result.Properties.Single(property => property.Name == "tenant").Differs);
        Assert.Equal(2, result.ChangedProperties);
    }
}
