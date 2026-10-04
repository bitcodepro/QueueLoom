using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class UnpublishedOperationCleanupTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    private static BrowsedMessage Dead(long sequence, string messageId) =>
        new(Orders, ServiceBusSubQueue.DeadLetter, sequence, "secret body"u8.ToArray(), new EditableMessageProperties(MessageId: messageId));

    [Fact]
    public async Task AResendThatCannotBePreparedLeavesNoBodySnapshot()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var valid = Dead(1, "ok");
        var invalid = Dead(2, new string('x', 200));
        ResendItem[] items = [new(valid, Orders, valid.CreateDraft()), new(invalid, Orders, invalid.CreateDraft())];

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.CreateResendAsync(Guid.NewGuid(), items, ResendMode.Copy, 10, null, "identity", "Resend", default));

        Assert.Empty(Directory.GetDirectories(directory.Path));
    }

    [Fact]
    public async Task AReplayThatCannotBePreparedLeavesNoBodySnapshot()
    {
        using var directory = new TemporaryDirectory();
        var store = new BatchReplayStore(directory.Path);
        var drafts = new[]
        {
            (Dead(1, "ok").CreateDraft(), "first"),
            (Dead(2, "ok").CreateDraft() is var draft ? new MessageDraft(draft.Body, draft.Properties with { PartitionKey = new string('k', 200) }) : null!, "second")
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.CreateAsync(Guid.NewGuid(), Orders, drafts, preserveIds: false, rate: 10, default));

        Assert.Empty(Directory.GetDirectories(directory.Path));
    }
}
