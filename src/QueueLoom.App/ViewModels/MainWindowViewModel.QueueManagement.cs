using QueueLoom.App.Commands;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>
/// Creating, changing and deleting queues. Offered only when the environment allows it, the service supports it
/// and write access is on; deleting always asks for the queue's name.
/// </summary>
public sealed partial class MainWindowViewModel
{
    public AsyncRelayCommand CreateQueueCommand { get; private set; } = null!;

    public AsyncRelayCommand EditQueueSettingsCommand { get; private set; } = null!;

    public AsyncRelayCommand DeleteQueueCommand { get; private set; } = null!;

    /// <summary>The environment allows queue management and its service supports it (write access may still be off).</summary>
    public bool CanManageQueues => IsConnected && _connectedProfile?.AllowQueueManagement == true && _workspace.QueueManagement is not null;

    public string QueueKindName => _workspace.QueueManagement?.QueueKindName ?? "queue";

    public string NewQueueLabel => $"New {QueueKindName}…";

    public string QueueSettingsLabel => $"{char.ToUpperInvariant(QueueKindName[0])}{QueueKindName[1..]} settings…";

    public string DeleteQueueLabel => $"Delete {QueueKindName}…";

    public string ManageQueuesHint => CanWrite
        ? $"Create, change or delete a {QueueKindName}"
        : $"Unlock write access to create, change or delete a {QueueKindName}";

    private bool IsManageableSelection => SelectedEntity is { IsQueue: true };

    private void InitializeQueueManagement()
    {
        CreateQueueCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync($"Creating a {QueueKindName}", CreateQueueAsync, token),
            () => !IsBusy && CanManageQueues && CanWrite);
        EditQueueSettingsCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync($"Changing {SelectedEntity?.Name}", EditQueueSettingsAsync, token),
            () => !IsBusy && CanManageQueues && CanWrite && IsManageableSelection && _workspace.QueueManagement?.CanUpdate == true);
        DeleteQueueCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync($"Deleting {SelectedEntity?.Name}", DeleteQueueAsync, token),
            () => !IsBusy && CanManageQueues && CanWrite && IsManageableSelection);
    }

    private void NotifyQueueManagement()
    {
        OnPropertyChanged(nameof(CanManageQueues));
        OnPropertyChanged(nameof(QueueKindName));
        OnPropertyChanged(nameof(NewQueueLabel));
        OnPropertyChanged(nameof(QueueSettingsLabel));
        OnPropertyChanged(nameof(DeleteQueueLabel));
        OnPropertyChanged(nameof(ManageQueuesHint));
        CreateQueueCommand?.NotifyCanExecuteChanged();
        EditQueueSettingsCommand?.NotifyCanExecuteChanged();
        DeleteQueueCommand?.NotifyCanExecuteChanged();
    }

    private async Task CreateQueueAsync(CancellationToken cancellationToken)
    {
        var (profile, capabilities) = RequireQueueManagement();
        var dialog = new QueueDialogViewModel(capabilities, profile.Name);
        if (await _dialogs.EditQueueAsync(dialog, cancellationToken).ConfigureAwait(true) is not QueueDefinition definition)
        {
            StatusText = $"No {QueueKindName} was created";
            return;
        }

        RecordOperationIntent($"Create {QueueKindName} started", definition.Name, null);
        await _workspace.CreateQueueAsync(definition, cancellationToken).ConfigureAwait(true);
        await RefreshAfterQueueChangeAsync(cancellationToken).ConfigureAwait(true);
        SelectedEntity = Entities.FirstOrDefault(entity => entity.IsQueue && entity.Name == definition.Name) ?? SelectedEntity;
        StatusText = $"Created {QueueKindName} {definition.Name}" + (definition.CreateDeadLetterQueue ? " and its dead-letter queue" : string.Empty);
        AddActivity("Success", $"{Capitalized(QueueKindName)} created", $"{profile.Name} · {definition.Name}", ServiceBusEntityReference.Queue(definition.Name));
    }

    private async Task EditQueueSettingsAsync(CancellationToken cancellationToken)
    {
        var (profile, capabilities) = RequireQueueManagement();
        var queue = SelectedEntity is { IsQueue: true } entity ? entity.Name : throw new InvalidOperationException($"Select a {QueueKindName} first.");
        var current = await _workspace.GetQueueSettingsAsync(queue, cancellationToken).ConfigureAwait(true);
        var dialog = new QueueDialogViewModel(capabilities, profile.Name, queue, current);
        if (await _dialogs.EditQueueAsync(dialog, cancellationToken).ConfigureAwait(true) is not QueueSettings settings)
        {
            StatusText = $"{Capitalized(QueueKindName)} {queue} was not changed";
            return;
        }

        RecordOperationIntent($"Change {QueueKindName} started", queue, ServiceBusEntityReference.Queue(queue));
        await _workspace.UpdateQueueSettingsAsync(queue, settings, cancellationToken).ConfigureAwait(true);
        await RefreshAfterQueueChangeAsync(cancellationToken).ConfigureAwait(true);
        StatusText = $"Saved the settings of {queue}";
        AddActivity("Success", $"{Capitalized(QueueKindName)} settings changed", $"{profile.Name} · {queue}", ServiceBusEntityReference.Queue(queue));
    }

    private async Task DeleteQueueAsync(CancellationToken cancellationToken)
    {
        var (profile, _) = RequireQueueManagement();
        var entity = SelectedEntity is { IsQueue: true } selected ? selected : throw new InvalidOperationException($"Select a {QueueKindName} first.");
        var messages = entity.Active + entity.DeadLetters;
        var confirmed = await _dialogs.ConfirmAsync(
            $"Delete {QueueKindName} {entity.Name}",
            $"Environment: {profile.Name}\n{profile.Provider.DisplayName()}: {profile.EndpointDisplay}\n\n" +
            $"The {QueueKindName} {entity.Name} and every message in it are deleted" +
            (messages > 0 ? $": {entity.Active:N0} active and {entity.DeadLetters:N0} dead-lettered right now. " : ". ") +
            "Messages are not backed up; export or back them up first if you may need them. This cannot be undone.\n\n" +
            $"Type the name of the {QueueKindName} to confirm.",
            isDangerous: true,
            requiredText: entity.Name,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            StatusText = $"{Capitalized(QueueKindName)} {entity.Name} was not deleted";
            return;
        }

        RecordOperationIntent($"Delete {QueueKindName} started", entity.Name, entity.Reference);
        await _workspace.DeleteQueueAsync(entity.Name, cancellationToken).ConfigureAwait(true);
        await RefreshAfterQueueChangeAsync(cancellationToken).ConfigureAwait(true);
        StatusText = $"Deleted {QueueKindName} {entity.Name}";
        AddActivity("Warning", $"{Capitalized(QueueKindName)} deleted", $"{profile.Name} · {entity.Name} · {messages:N0} messages", entity.Reference);
    }

    private (ServiceBusProfile Profile, QueueManagementCapabilities Capabilities) RequireQueueManagement()
    {
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect to an environment first.");
        profile.EnsureQueueManagementAllowed();
        if (!CanWrite)
        {
            throw new InvalidOperationException("Unlock write access first.");
        }
        return (profile, _workspace.QueueManagement ?? throw new InvalidOperationException(
            $"{profile.Provider.DisplayName()} queues cannot be managed from QueueLoom."));
    }

    private async Task RefreshAfterQueueChangeAsync(CancellationToken cancellationToken) =>
        ApplyTopology(await _workspace.GetTopologyAsync(forceRefresh: true, cancellationToken).ConfigureAwait(true));

    private static string Capitalized(string text) => char.ToUpperInvariant(text[0]) + text[1..];
}
