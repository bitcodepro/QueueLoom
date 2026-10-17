using System.Globalization;
using QueueLoom.App.Commands;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>
/// Amazon SQS dead-letter queue redrive: AWS moves every message of a dead-letter queue back to the queue it came from,
/// without the 1,000-message limit of a resend and without receiving the messages here.
/// </summary>
public sealed partial class MainWindowViewModel
{
    public bool SupportsNativeRedrive => IsConnected && _workspace.SupportsNativeRedrive;

    public AsyncRelayCommand RedriveSelectedDeadLettersCommand { get; private set; } = null!;

    private void InitializeNativeRedrive()
    {
        RedriveSelectedDeadLettersCommand = _commands.Create(
            token => RunWorkspaceOperationAsync($"Redriving {SelectedEntity?.Name}", RedriveSelectedDeadLettersAsync, token),
            () => !IsBusy && SupportsNativeRedrive && CanWrite && SelectedEntity is { IsQueue: true });
        PropertyChanged += (_, args) =>
        {
            if (args.PropertyName is nameof(IsBusy) or nameof(IsConnected) or nameof(SelectedEntity) or nameof(CanWrite))
            {
                OnPropertyChanged(nameof(SupportsNativeRedrive));
                RedriveSelectedDeadLettersCommand.NotifyCanExecuteChanged();
            }
        };
    }

    private async Task RedriveSelectedDeadLettersAsync(CancellationToken cancellationToken)
    {
        var entity = SelectedEntity ?? throw new InvalidOperationException("Select a queue first.");
        var plan = await _workspace.PlanNativeRedriveAsync(entity.Reference, cancellationToken).ConfigureAwait(true);
        if (plan.ApproximateMessages == 0)
        {
            StatusText = $"Dead-letter queue '{plan.DeadLetterQueue}' is empty; nothing to redrive.";
            return;
        }
        var others = plan.SourceQueues.Where(queue => queue != entity.Name).ToArray();
        var message =
            $"AWS will move about {plan.ApproximateMessages.ToString("N0", CultureInfo.CurrentCulture)} message(s) from " +
            $"dead-letter queue '{plan.DeadLetterQueue}' back to the queue each one came from" +
            (others.Length == 0
                ? $" ('{entity.Name}')."
                : $". It is shared, so messages of {string.Join(", ", others.Select(queue => $"'{queue}'"))} go back there too.") +
            "\n\nConsumers will receive them again. The move happens in AWS, so QueueLoom takes no backup; it cannot be undone " +
            "here. Follow its progress by refreshing the counters.";
        if (!await _dialogs.ConfirmAsync("Redrive dead-letter queue", message, true, cancellationToken: cancellationToken).ConfigureAwait(true))
        {
            return;
        }
        var handle = await _workspace.StartNativeRedriveAsync(plan, cancellationToken).ConfigureAwait(true);
        AddActivity("Success", "DLQ redrive started",
            $"{_connectedProfile?.Name} · {plan.DeadLetterQueue} → source queues · about {plan.ApproximateMessages:N0} message(s) · task {handle}",
            entity.Reference);
        StatusText = $"AWS is moving the messages of '{plan.DeadLetterQueue}' back. Refresh the counters to follow it.";
    }
}
