using QueueLoom.App.Commands;

namespace QueueLoom.App.ViewModels;

public sealed partial class MainWindowViewModel
{
    private string _sessionStateText = string.Empty;

    /// <summary>The state of the session named in Explorer, once read; empty until then.</summary>
    public string SessionStateText
    {
        get => _sessionStateText;
        private set
        {
            if (SetProperty(ref _sessionStateText, value))
            {
                OnPropertyChanged(nameof(HasSessionStateText));
            }
        }
    }

    public bool HasSessionStateText => SessionStateText.Length > 0;

    public AsyncRelayCommand ShowSessionStateCommand { get; private set; } = null!;

    public AsyncRelayCommand ClearSessionStateCommand { get; private set; } = null!;

    private void InitializeSessionStateCommands()
    {
        ShowSessionStateCommand = _commands.Create(
            token => RunWorkspaceOperationAsync("Reading session state", ShowSessionStateAsync, token),
            () => !IsBusy && CanOpenSessionById && Core.ServiceBus.BrowseMessagesRequest.IsValidSessionId(SessionIdToOpen));
        ClearSessionStateCommand = _commands.Create(
            token => RunWorkspaceOperationAsync("Clearing session state", ClearSessionStateAsync, token),
            () => !IsBusy && CanWrite && CanOpenSessionById && Core.ServiceBus.BrowseMessagesRequest.IsValidSessionId(SessionIdToOpen));
    }

    private void NotifySessionStateCommands()
    {
        // Null while the constructor is still creating the commands.
        ShowSessionStateCommand?.NotifyCanExecuteChanged();
        ClearSessionStateCommand?.NotifyCanExecuteChanged();
    }

    private async Task ShowSessionStateAsync(CancellationToken cancellationToken)
    {
        var entity = SelectedEntity ?? throw new InvalidOperationException("Select a session-enabled queue or subscription first.");
        var sessionId = SessionIdToOpen;
        var state = await _workspace.GetSessionStateAsync(entity.Reference, sessionId, cancellationToken).ConfigureAwait(true);
        if (SessionIdToOpen == sessionId && SelectedEntity == entity)
        {
            SessionStateText = SessionStateDisplay.Describe(state);
        }
    }

    private async Task ClearSessionStateAsync(CancellationToken cancellationToken)
    {
        var entity = SelectedEntity ?? throw new InvalidOperationException("Select a session-enabled queue or subscription first.");
        var sessionId = SessionIdToOpen;
        var confirmed = await _dialogs.ConfirmAsync(
            "Clear session state",
            $"Remove the state of session '{sessionId}' in {entity.Reference.DisplayName}? The consumer that stored it starts " +
            "that session again from no state. Its messages are not changed.",
            isDangerous: true,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            return;
        }
        await _workspace.ClearSessionStateAsync(entity.Reference, sessionId, cancellationToken).ConfigureAwait(true);
        if (SessionIdToOpen == sessionId && SelectedEntity == entity)
        {
            SessionStateText = SessionStateDisplay.Describe(null);
        }
    }
}
