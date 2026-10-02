using System.Collections;
using System.Text;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class CycleTwoExportRegressionTests
{
    private static readonly ExportedMessage Message = new("isolated", new BrowsedMessage(
        ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.Active, 1, "body"u8.ToArray(),
        new EditableMessageProperties(MessageId: "id")));

    [Theory]
    [InlineData("json", true)]
    [InlineData("csv", true)]
    [InlineData("json", false)]
    [InlineData("csv", false)]
    public async Task CycleTwoExport_PreCancelledExportPreservesDestination(string extension, bool exists)
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "messages." + extension);
        var original = Encoding.UTF8.GetBytes("previous complete export");
        if (exists) await File.WriteAllBytesAsync(path, original);
        using var cancellation = new CancellationTokenSource(); cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => MessageExport.WriteAsync(path, [Message], cancellation.Token));
        if (exists) Assert.Equal(original, await File.ReadAllBytesAsync(path));
        else Assert.False(File.Exists(path));
        Assert.Equal(exists ? 1 : 0, Directory.GetFiles(directory.Path).Length);
    }

    [Theory]
    [InlineData("json", true)]
    [InlineData("csv", true)]
    [InlineData("json", false)]
    [InlineData("csv", false)]
    public async Task CycleTwoExport_FailureAfterOneRowPreservesDestinationAndRemovesOnlyOwnTemporaryFile(string extension, bool exists)
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "messages." + extension);
        var foreign = Path.Combine(directory.Path, ".foreign.tmp");
        await File.WriteAllTextAsync(foreign, "foreign evidence");
        if (exists) await File.WriteAllTextAsync(path, "previous complete export");
        await Assert.ThrowsAsync<IOException>(() => MessageExport.WriteAsync(path, new FailingRows()));
        if (exists) Assert.Equal("previous complete export", await File.ReadAllTextAsync(path));
        else Assert.False(File.Exists(path));
        Assert.Equal("foreign evidence", await File.ReadAllTextAsync(foreign));
        Assert.Equal(exists ? 2 : 1, Directory.GetFiles(directory.Path).Length);
    }

    [Theory]
    [InlineData("json")]
    [InlineData("csv")]
    public async Task CycleTwoExport_CompletedExportReplacesDestination(string extension)
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "messages." + extension);
        await File.WriteAllTextAsync(path, "old");
        await MessageExport.WriteAsync(path, [Message]);
        Assert.Contains("body", await File.ReadAllTextAsync(path), StringComparison.Ordinal);
        Assert.Single(Directory.GetFiles(directory.Path));
    }

    private sealed class FailingRows : IReadOnlyList<ExportedMessage>
    {
        public int Count => 2;
        public ExportedMessage this[int index] => index == 0 ? Message : throw new IOException("serialization source failed");
        public IEnumerator<ExportedMessage> GetEnumerator() { yield return Message; throw new IOException("serialization source failed"); }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    [Theory]
    [InlineData("json")]
    [InlineData("csv")]
    public async Task CycleTwoExport_CancellationBetweenRowsPreservesThePreviousExport(string extension)
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "messages." + extension);
        await File.WriteAllTextAsync(path, "previous complete export");
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => MessageExport.WriteAsync(path, new CancellingRows(cancellation), cancellation.Token));
        Assert.Equal("previous complete export", await File.ReadAllTextAsync(path));
        Assert.Single(Directory.GetFiles(directory.Path));
    }
    private sealed class CancellingRows(CancellationTokenSource cancellation) : IReadOnlyList<ExportedMessage>
    {
        public int Count => 2;
        public ExportedMessage this[int index] => Message;
        public IEnumerator<ExportedMessage> GetEnumerator() { yield return Message; cancellation.Cancel(); yield return Message; }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
