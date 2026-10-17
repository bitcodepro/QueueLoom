using System.Globalization;
using QueueLoom.App.Commands;
using QueueLoom.Core.Profiles;
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

    private void RequireRedriveWriteAccess(ServiceBusProfile expected)
    {
        var current = _connectedProfile ?? throw new InvalidOperationException("Connect to an environment first.");
        if (current.Id != expected.Id || _workspace.ConnectedProfileId != expected.Id ||
            ScheduledResend.IdentityFor(current) != ScheduledResend.IdentityFor(expected))
            throw new InvalidOperationException("The connected environment changed. Start the redrive again. Nothing was moved.");
        if (!CanWrite)
            throw new InvalidOperationException("Write access expired while the confirmation was open. Unlock it and start the redrive again. Nothing was moved.");
    }

    private async Task RedriveSelectedDeadLettersAsync(CancellationToken cancellationToken)
    {
        var entity = SelectedEntity ?? throw new InvalidOperationException("Select a queue first.");
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect to an environment first.");
        var plan = await _workspace.PlanNativeRedriveAsync(entity.Reference, cancellationToken).ConfigureAwait(true);
        if (plan.ApproximateMessages == 0)
        {
            StatusText = $"Dead-letter queue '{plan.DeadLetterQueue}' is empty; nothing to redrive.";
            return;
        }
        // AWS returns each message to the queue it came from, which no reading of the current setup can list: a queue
        // that used this dead-letter queue before gets its messages back too. The confirmation says so.
        var message =
            $"Environment: {profile.Name}\n{profile.Provider.DisplayName()}: {profile.EndpointDisplay}\n\n" +
            $"AWS will move every message in dead-letter queue '{plan.DeadLetterQueue}' (about " +
            $"{plan.ApproximateMessages.ToString("N0", CultureInfo.CurrentCulture)} now) back to the queue each one came from. " +
            $"Queues using it now: {string.Join(", ", plan.SourceQueues.Select(queue => $"'{queue}'"))}. Messages from queues " +
            "that used it before go back to those queues as well.\n\n" +
            "Consumers will receive them again. The move happens in AWS, so QueueLoom takes no backup; it cannot be undone " +
            "here. Follow its progress by refreshing the counters.";
        if (!await _dialogs.ConfirmAsync("Redrive dead-letter queue", message, true, cancellationToken: cancellationToken).ConfigureAwait(true))
        {
            return;
        }
        // The dialog held the operation open: write access may have expired, or the environment changed, meanwhile.
        RequireRedriveWriteAccess(profile);
        // A durable record before the move, which cannot be undone; when it cannot be written, nothing is moved.
        RecordOperationIntent("DLQ redrive started",
            $"{profile.Name} · {plan.DeadLetterQueue} ({plan.DeadLetterArn}) → the queue each message came from · " +
            $"about {plan.ApproximateMessages:N0} message(s) · queues using it now: {string.Join(", ", plan.SourceQueues)}",
            entity.Reference);
        var handle = await _workspace.StartNativeRedriveAsync(plan, cancellationToken).ConfigureAwait(true);
        AddActivity("Success", "DLQ redrive accepted by AWS",
            $"{profile.Name} · {plan.DeadLetterQueue} → source queues · task {handle}",
            entity.Reference);
        StatusText = $"AWS is moving the messages of '{plan.DeadLetterQueue}' back. Refresh the counters to follow it.";
    }
}
