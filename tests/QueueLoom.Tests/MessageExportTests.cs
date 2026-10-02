using System.Text;
using System.Text.Json;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class MessageExportTests
{
    private static readonly BrowsedMessage Text = new(
        ServiceBusEntityReference.Subscription("events", "billing"),
        ServiceBusSubQueue.DeadLetter,
        7,
        Encoding.UTF8.GetBytes("line 1, \"quoted\"\nline 2"),
        new EditableMessageProperties(MessageId: "m-7", CorrelationId: "=HYPERLINK(\"x\")", Subject: "Invoice"),
        [new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "contoso")],
        deliveryCount: 5,
        enqueuedAt: new DateTimeOffset(2026, 9, 30, 8, 0, 0, TimeSpan.Zero),
        deadLetterReason: "MaxDeliveryCountExceeded");

    private static readonly BrowsedMessage Binary = new(
        ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 8, new byte[] { 0xff, 0x00, 0x10 },
        new EditableMessageProperties(MessageId: "m-8"));

    [Fact]
    public async Task Json_keeps_every_detail_and_encodes_binary_bodies_as_base64()
    {
        using var stream = new MemoryStream();

        await MessageExport.WriteJsonAsync(stream, [new("Dev", Text), new("Dev", Binary)], CancellationToken.None);

        using var document = JsonDocument.Parse(stream.ToArray());
        var first = document.RootElement[0];
        Assert.Equal("events / billing", first.GetProperty("source").GetString());
        Assert.Equal("m-7", first.GetProperty("messageId").GetString());
        Assert.Equal(7, first.GetProperty("sequenceNumber").GetInt64());
        Assert.Equal("contoso", first.GetProperty("applicationProperties").GetProperty("tenant").GetString());
        Assert.Equal("line 1, \"quoted\"\nline 2", first.GetProperty("body").GetString());
        Assert.Equal("base64", document.RootElement[1].GetProperty("bodyEncoding").GetString());
        Assert.Equal("/wAQ", document.RootElement[1].GetProperty("body").GetString());
    }

    [Fact]
    public async Task Csv_quotes_values_and_defuses_spreadsheet_formulas()
    {
        using var writer = new StringWriter();

        await MessageExport.WriteCsvAsync(writer, [new("Dev", Text)], CancellationToken.None);

        var csv = writer.ToString();
        Assert.StartsWith("environment,source,subQueue,messageId", csv, StringComparison.Ordinal);
        Assert.Contains("\"'=HYPERLINK(\"\"x\"\")\"", csv, StringComparison.Ordinal);
        Assert.Contains("\"line 1, \"\"quoted\"\"\nline 2\"", csv, StringComparison.Ordinal);
        Assert.Contains("2026-09-30T08:00:00.0000000+00:00", csv, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("export.csv", MessageExportFormat.Csv)]
    [InlineData("export.CSV", MessageExportFormat.Csv)]
    [InlineData("export.json", MessageExportFormat.Json)]
    [InlineData("export", MessageExportFormat.Json)]
    public void The_file_extension_picks_the_format(string path, MessageExportFormat format) =>
        Assert.Equal(format, MessageExport.FormatFor(path));

    [Fact]
    public async Task Exports_are_written_to_the_chosen_file()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "dead-letters.csv");

        await MessageExport.WriteAsync(path, [new("Dev", Text), new("Dev", Binary)]);

        var lines = await File.ReadAllLinesAsync(path);
        Assert.Equal("environment,source,subQueue,messageId,correlationId,subject,contentType,sessionId,enqueuedAtUtc,deliveryCount,deadLetterReason,deadLetterDescription,applicationProperties,amqpType,amqpAppId,partition,offset,bodyEncoding,body", lines[0]);
        Assert.Contains(lines, line => line.Contains("m-8", StringComparison.Ordinal) && line.EndsWith(",base64,/wAQ", StringComparison.Ordinal));
    }
}
