using System.Collections.Concurrent;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(ResendMode.Copy)]
    [InlineData(ResendMode.Move)]
    public async Task ResendCompleted_LateProgressCannotReplaceTerminalStatus(ResendMode mode)
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var lifetime = viewModel;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { Mode = mode };
        viewModel.Messages.First().IsMarked = true;
        var context = new DeferredResendProgressContext();
        var previous = SynchronizationContext.Current;
        Task operation;
        SynchronizationContext.SetSynchronizationContext(context);
        try { operation = viewModel.ResendMarkedMessagesCommand.ExecuteAsync(); }
        finally { SynchronizationContext.SetSynchronizationContext(previous); }
        await operation;
        var terminal = viewModel.StatusText;
        Assert.Contains("1 of 1 sent", terminal, StringComparison.Ordinal);
        Assert.True(context.PendingCount > 0);
        context.FlushProgress();
        Assert.Equal(terminal, viewModel.StatusText);
        Assert.Single(workspace.SentMessages);
    }

    private sealed class DeferredResendProgressContext : SynchronizationContext
    {
        private readonly ConcurrentQueue<(SendOrPostCallback Callback, object? State)> _pending = new();
        public int PendingCount => _pending.Count;
        public override void Post(SendOrPostCallback callback, object? state)
        {
            if (state is ResendProgress) _pending.Enqueue((callback, state));
            else ThreadPool.QueueUserWorkItem(_ => callback(state));
        }
        public void FlushProgress()
        {
            while (_pending.TryDequeue(out var update)) update.Callback(update.State);
        }
    }
}