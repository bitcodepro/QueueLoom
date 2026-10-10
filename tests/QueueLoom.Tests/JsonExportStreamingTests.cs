using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class JsonExportStreamingTests
{
    private static ExportedMessage Message(int number) => new("isolated", new BrowsedMessage(
        ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.Active, number, new byte[64 * 1024],
        new EditableMessageProperties(MessageId: "id-" + number)));

    [Fact]
    public async Task EachMessageReachesTheStreamBeforeTheNextIsWritten()
    {
        // Utf8JsonWriter keeps everything in memory until it is flushed; a 1 MB body per message across a
        // "load all" result must not pile up into one buffer that can exceed the largest possible array.
        var messages = Enumerable.Range(1, 5).Select(Message).ToArray();
        using var stream = new RecordingStream();
        await MessageExport.WriteJsonAsync(stream, messages, CancellationToken.None);

        Assert.True(stream.WritesBeforeEnd >= messages.Length, $"Only {stream.WritesBeforeEnd} write(s) reached the stream.");
        stream.Position = 0;
        using var document = JsonDocument.Parse(stream);
        Assert.Equal(messages.Length, document.RootElement.GetArrayLength());
    }

    private sealed class RecordingStream : MemoryStream
    {
        public int WritesBeforeEnd { get; private set; }
        public override void Write(ReadOnlySpan<byte> buffer) { WritesBeforeEnd++; base.Write(buffer); }
        public override void Write(byte[] buffer, int offset, int count) { WritesBeforeEnd++; base.Write(buffer, offset, count); }
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        { WritesBeforeEnd++; return base.WriteAsync(buffer, cancellationToken); }
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        { WritesBeforeEnd++; return base.WriteAsync(buffer, offset, count, cancellationToken); }
    }
}
