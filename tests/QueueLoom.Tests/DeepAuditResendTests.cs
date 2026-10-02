using System.Reflection;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class DeepAuditResendTests
{
    [Fact]
    public async Task PreparedNewIdsSurviveSchedulingAndRetryWithoutDuplicateDelivery()
    {
        var source = ServiceBusEntityReference.Queue("orders");
        var originals = Enumerable.Range(1, 2).Select(i => new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, i,
            "{}"u8.ToArray(), new EditableMessageProperties(MessageId: $"old-{i}"))).ToArray();
        var items = originals.Select(o => new ResendItem(o, source, o.CreateDraft()).WithNewMessageId()).ToArray();
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        var store = new QueueLoom.Infrastructure.Persistence.JsonScheduledResendStore(
            QueueLoom.Infrastructure.Persistence.QueueLoomPaths.ForRoot(directory.Path));
        store.Save([new ScheduledResend(Guid.NewGuid(), Guid.NewGuid(), "Test", DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow.AddHours(1), ResendMode.Move, 10, "orders", items.Select(ScheduledResendItem.From).ToArray())]);
        var restored = Assert.Single(store.Load()).Items.Select(i => i.ToResendItem()).ToArray();
        Assert.Equal(items.Select(i => i.Message.Properties.MessageId), restored.Select(i => i.Message.Properties.MessageId));
        Assert.Equal(2, restored.Select(i => i.Message.Properties.MessageId).Distinct().Count());
        var broker = new DeduplicatingBroker("old-1", "old-2");
        var result = await DeadLetterResender.ResendAsync(broker.Workspace, restored, ResendMode.Move);
        Assert.Equal(2, result.MovedCount);
        Assert.Equal(2, broker.Delivered.Count);
        // A transport retry reuses the prepared draft and the broker suppresses its repeated delivery.
        foreach (var item in restored) await broker.Workspace.SendMessageAsync(new SendMessageRequest(source, item.Message));
        Assert.Equal(2, broker.Delivered.Count);
    }

    [Fact]
    public void AzureMoveRequiresNewIdsInTheVisibleDialog()
    {
        var original = new BrowsedMessage(ServiceBusEntityReference.Queue("q"), ServiceBusSubQueue.DeadLetter, 1,
            "{}"u8.ToArray(), new EditableMessageProperties(MessageId: "old"));
        var dialog = new QueueLoom.App.ViewModels.ResendDialogViewModel([original], [], "Dev", false, requiresNewIdsForMove: true);
        Assert.Contains("distinct new Message ID", dialog.Summary, StringComparison.Ordinal);
        dialog.Moves = true; dialog.PreserveMessageIds = true;
        Assert.False(dialog.CanConfirm);
        Assert.Contains("suppress delivery", dialog.Summary, StringComparison.Ordinal);
        dialog.Copies = true;
        Assert.True(dialog.CanConfirm);
    }
    [Fact]
    public async Task AzureMoveCannotRemoveAnOriginalAfterADeduplicatedSend()
    {
        var source = ServiceBusEntityReference.Queue("orders");
        var original = new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, 1, "{}"u8.ToArray(), new EditableMessageProperties(MessageId: "original-id"));
        var broker = new DeduplicatingBroker("original-id");
        await Assert.ThrowsAsync<InvalidOperationException>(() => DeadLetterResender.ResendAsync(broker.Workspace,
            [new ResendItem(original, source, original.CreateDraft())], ResendMode.Move));
        Assert.Equal(0, broker.Deleted);
        Assert.Empty(broker.Delivered);
    }

    internal sealed class DeduplicatingBroker
    {
        private readonly HashSet<string> _seen;
        public List<string> Delivered { get; } = [];
        public int Deleted { get; private set; }
        public MessagingProvider Provider { get; set; } = MessagingProvider.AzureServiceBus;
        public IServiceBusWorkspace Workspace { get; }
        public DeduplicatingBroker(params string[] seen)
        {
            _seen = [.. seen];
            var proxy = DispatchProxy.Create<IServiceBusWorkspace, BrokerProxy>();
            ((BrokerProxy)proxy).Call = (method, args) =>
            {
                switch (method.Name)
                {
                    case "get_ConnectedProvider": return (MessagingProvider?)Provider;
                    case "get_ConnectionState": return WorkspaceConnectionState.Connected;
                    case "SendMessageAsync":
                        var id = ((SendMessageRequest)args![0]!).Message.Properties.MessageId!;
                        // Azure accepts a repeated MessageId but silently suppresses its delivery.
                        if (_seen.Add(id)) Delivered.Add(id);
                        return Task.CompletedTask;
                    case "DeleteDeadLetterMessagesAsync":
                        var request = (DeleteDeadLetterMessagesRequest)args![0]!;
                        Deleted += request.Messages.Count;
                        return Task.FromResult(new DeleteDeadLetterMessagesResult(Guid.NewGuid(), DateTimeOffset.UtcNow, DateTimeOffset.UtcNow,
                            request.Messages.Select(key => new DeadLetterMessageDeletionResult(key, DeadLetterMessageDeletionOutcome.Deleted)), "fake-backup"));
                    default: throw new NotSupportedException(method.Name);
                }
            };
            Workspace = proxy;
        }
    }
    public class BrokerProxy : DispatchProxy
    {
        public Func<MethodInfo, object?[]?, object?> Call { get; set; } = null!;
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => Call(targetMethod!, args);
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task BulkResendUsesDistinctNewMessageIdsByDefault()
    {
        var (vm, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = vm;
        dialogs.ResendChoice = dialog => dialog.ToOptions();
        vm.AreAllMessagesMarked = true;
        var originalIds = vm.Messages.Select(m => m.Message.Properties.MessageId).ToArray();
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        Assert.Equal(originalIds.Length, workspace.SentMessages.Count);
        Assert.All(workspace.SentMessages, send => Assert.DoesNotContain(send.Message.Properties.MessageId, originalIds));
        Assert.Equal(originalIds.Length, workspace.SentMessages.Select(s => s.Message.Properties.MessageId).Distinct().Count());
    }
}
