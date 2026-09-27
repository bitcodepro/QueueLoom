using Microsoft.Extensions.Logging;
using QueueLoom.App.Models;
using QueueLoom.App.Services;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Saved environments and the workspace connection.</summary>
public sealed partial class MainWindowViewModel
{
    public ProfileItemViewModel? SelectedProfile
    {
        get => _selectedProfile;
        set
        {
            if (SetProperty(ref _selectedProfile, value))
            {
                RefreshDeadLetterEnvironmentFilters();
                OnPropertyChanged(nameof(HasSelectedProfile));
                OnPropertyChanged(nameof(SelectedProfileName));
                OnPropertyChanged(nameof(IsSelectedProfileConnected));
                OnPropertyChanged(nameof(EnvironmentActionLabel));
                OnPropertyChanged(nameof(EnvironmentActionCommand));
                NotifyCommandStates();
            }
        }
    }

    public bool HasSelectedProfile => SelectedProfile is not null;

    public bool HasProfiles => Profiles.Count > 0;

    public string EnvironmentActionLabel => !HasProfiles
        ? "Add environment"
        : IsSelectedProfileConnected
            ? "Disconnect"
            : "Connect";

    public System.Windows.Input.ICommand EnvironmentActionCommand =>
        !HasProfiles
            ? AddEnvironmentCommand
            : IsSelectedProfileConnected
                ? DisconnectCommand
                : ConnectCommand;

    public string SelectedProfileName => SelectedProfile?.Name ?? "No environment selected";

    public bool IsSelectedProfileConnected => SelectedProfile?.IsConnected == true;

    public bool IsConnected => _workspace.ConnectionState == WorkspaceConnectionState.Connected;

    public Guid? ConnectedProfileId => IsConnected ? _workspace.ConnectedProfileId : null;

    public string ConnectedProfileName => IsConnected
        ? _connectedProfile?.Name ?? "Connected environment"
        : "No environment connected";

    public string ConnectedAuthenticationLabel => _connectedProfile?.AuthenticationDisplayName ?? "Not connected";

    /// <summary>
    /// Service Bus can peek. SQS and Pub/Sub cannot, so QueueLoom receives messages, holds them for a moment
    /// and hands them back unchanged.
    /// </summary>
    public string BrowseModeLabel => ConnectedProvider switch
    {
        MessagingProvider.AmazonSqsSns or MessagingProvider.GooglePubSub => "Receive and release",
        _ => "Non-destructive Peek"
    };

    public string BrowseModeDescription => ConnectedProvider switch
    {
        MessagingProvider.AmazonSqsSns =>
            "SQS has no peek. Messages are received, hidden for up to 3 minutes and returned unchanged; their receive count goes up by one.",
        MessagingProvider.GooglePubSub =>
            "Pub/Sub has no peek. Messages are pulled, held for up to 3 minutes and returned unchanged; with a dead-letter policy each read counts as a delivery attempt.",
        _ => "Messages are peeked without locking or changing them."
    };

    public string DeadLetterCountCaption => ConnectedProvider switch
    {
        MessagingProvider.GooglePubSub => "Pub/Sub does not report counts",
        MessagingProvider.AmazonSqsSns => "Approximate SQS counts",
        _ => "DLQ + transfer DLQ"
    };

    /// <summary>The cloud of the connected environment, shown as a badge in the top bar.</summary>
    public MessagingProvider? ConnectedProvider => IsConnected ? _connectedProfile?.Provider : null;

    public string ConnectionLabel => IsConnected
        ? $"CONNECTED · {_connectedProfile?.Name ?? "environment"}"
        : "OFFLINE";

    public Tone ConnectionTone => IsConnected ? Tone.Success : Tone.Neutral;

    public string ConnectedNamespace => _connectedProfile?.EndpointDisplay
        ?? (IsConnected ? "Namespace connection" : "Connect an environment to begin");

    private async Task AddEnvironmentAsync(CancellationToken cancellationToken)
    {
        var result = await _dialogs.EditProfileAsync(null, cancellationToken).ConfigureAwait(true);
        if (result is null)
        {
            return;
        }

        await SaveProfileAsync(result, cancellationToken).ConfigureAwait(true);
        await ReloadProfilesAsync(cancellationToken, result.Profile.Id).ConfigureAwait(true);
        AddActivity("Success", "Environment added", $"{result.Profile.Name} · {result.Profile.EnvironmentDisplayName}");
        StatusText = $"Environment '{result.Profile.Name}' saved";
    }

    private async Task EditEnvironmentAsync(CancellationToken cancellationToken)
    {
        var selected = SelectedProfile ?? throw new InvalidOperationException("Select an environment first.");
        var result = await _dialogs.EditProfileAsync(selected.Profile, cancellationToken).ConfigureAwait(true);
        if (result is null)
        {
            return;
        }

        var secretKey = ProfileSecretKey.ConnectionString(result.Profile.Id);
        string? removedConnectionString = null;
        // Switching to a method without a stored secret (for example to Entra ID) removes the old secret.
        var removesConnectionString = selected.Profile.Authentication.Kind.UsesStoredSecret() &&
                                      !result.Profile.Authentication.Kind.UsesStoredSecret();
        if (removesConnectionString)
        {
            removedConnectionString = await _secretVault.RetrieveAsync(secretKey, cancellationToken)
                .ConfigureAwait(true);
            if (removedConnectionString is not null)
            {
                await _secretVault.RemoveAsync(secretKey, cancellationToken).ConfigureAwait(true);
            }
        }

        try
        {
            await SaveProfileAsync(result, cancellationToken).ConfigureAwait(true);
        }
        catch
        {
            if (removedConnectionString is not null)
            {
                await _secretVault.StoreAsync(secretKey, removedConnectionString, CancellationToken.None)
                    .ConfigureAwait(true);
            }
            throw;
        }

        await StopMonitorForConfigurationChangeAsync().ConfigureAwait(true);
        InvalidateProfileArtifacts(result.Profile.Id, "Environment configuration changed; the previous draft is no longer sendable.");

        if (_workspace.ConnectedProfileId == result.Profile.Id)
        {
            await _workspace.DisconnectAsync(cancellationToken).ConfigureAwait(true);
            await StopWriteUnlockTimerAsync().ConfigureAwait(true);
            ClearConnectedState();
        }
        else if (_writeUnlockProfileId == result.Profile.Id)
        {
            await StopWriteUnlockTimerAsync().ConfigureAwait(true);
        }

        await ReloadProfilesAsync(cancellationToken, result.Profile.Id).ConfigureAwait(true);
        AddActivity("Success", "Environment updated", result.Profile.Name);
    }

    private async Task SaveProfileAsync(ProfileEditorResult result, CancellationToken cancellationToken)
    {
        var secretKey = ProfileSecretKey.ConnectionString(result.Profile.Id);
        var previousProfile = await _profileRepository.GetAsync(result.Profile.Id, cancellationToken)
            .ConfigureAwait(true);
        var previousSelectedProfileId = await _profileRepository.GetSelectedProfileIdAsync(cancellationToken)
            .ConfigureAwait(true);
        var replacesConnectionString = result.ReplacesConnectionString && result.ConnectionString is not null;
        string? previousConnectionString = null;
        if (replacesConnectionString)
        {
            previousConnectionString = await _secretVault.RetrieveAsync(secretKey, cancellationToken)
                .ConfigureAwait(true);
        }

        var secretReplacementAttempted = false;
        try
        {
            if (replacesConnectionString)
            {
                secretReplacementAttempted = true;
                await _secretVault.StoreAsync(
                    secretKey,
                    result.ConnectionString!,
                    cancellationToken).ConfigureAwait(true);
            }

            await _profileRepository.UpsertAsync(result.Profile, cancellationToken).ConfigureAwait(true);
            await _profileRepository.SetSelectedProfileIdAsync(result.Profile.Id, cancellationToken).ConfigureAwait(true);
        }
        catch (Exception saveException)
        {
            var rollbackFailures = new List<Exception>();

            try
            {
                if (previousProfile is null)
                {
                    await _profileRepository.DeleteAsync(result.Profile.Id, CancellationToken.None)
                        .ConfigureAwait(true);
                }
                else
                {
                    await _profileRepository.UpsertAsync(previousProfile, CancellationToken.None)
                        .ConfigureAwait(true);
                }
            }
            catch (Exception rollbackException)
            {
                rollbackFailures.Add(rollbackException);
            }

            try
            {
                await _profileRepository.SetSelectedProfileIdAsync(
                        previousSelectedProfileId,
                        CancellationToken.None)
                    .ConfigureAwait(true);
            }
            catch (Exception rollbackException)
            {
                rollbackFailures.Add(rollbackException);
            }

            if (secretReplacementAttempted)
            {
                try
                {
                    if (previousConnectionString is null)
                    {
                        await _secretVault.RemoveAsync(secretKey, CancellationToken.None).ConfigureAwait(true);
                    }
                    else
                    {
                        await _secretVault.StoreAsync(secretKey, previousConnectionString, CancellationToken.None)
                            .ConfigureAwait(true);
                    }
                }
                catch (Exception rollbackException)
                {
                    rollbackFailures.Add(rollbackException);
                }
            }

            if (rollbackFailures.Count > 0)
            {
                throw new AggregateException(
                    "Saving the environment failed and its previous state could not be fully restored.",
                    [saveException, .. rollbackFailures]);
            }

            throw;
        }
    }

    private async Task DeleteEnvironmentAsync(CancellationToken cancellationToken)
    {
        var selected = SelectedProfile ?? throw new InvalidOperationException("Select an environment first.");
        var confirmed = await _dialogs.ConfirmAsync(
            "Delete environment",
            $"Remove '{selected.Name}' and its locally encrypted credential? Azure resources are not changed.",
            isDangerous: true,
            requiredText: selected.Name,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            return;
        }

        if (_workspace.ConnectedProfileId == selected.Id)
        {
            await _workspace.DisconnectAsync(cancellationToken).ConfigureAwait(true);
            ClearConnectedState();
        }

        var secretKey = ProfileSecretKey.ConnectionString(selected.Id);
        var connectionString = await _secretVault.RetrieveAsync(secretKey, cancellationToken)
            .ConfigureAwait(true);
        if (connectionString is not null)
        {
            await _secretVault.RemoveAsync(secretKey, cancellationToken).ConfigureAwait(true);
        }

        try
        {
            if (!await _profileRepository.DeleteAsync(selected.Id, cancellationToken).ConfigureAwait(true))
            {
                throw new InvalidOperationException("The environment no longer exists.");
            }
        }
        catch
        {
            if (connectionString is not null)
            {
                await _secretVault.StoreAsync(secretKey, connectionString, CancellationToken.None)
                    .ConfigureAwait(true);
            }
            throw;
        }
        if (_writeUnlockProfileId == selected.Id)
        {
            await StopWriteUnlockTimerAsync().ConfigureAwait(true);
        }
        await StopMonitorForConfigurationChangeAsync().ConfigureAwait(true);
        InvalidateProfileArtifacts(selected.Id, "Its environment was removed; the previous draft is no longer sendable.");
        AddActivity("Warning", "Environment removed", selected.Name);
        await ReloadProfilesAsync(cancellationToken).ConfigureAwait(true);
    }

    private async Task ReloadProfilesAsync(CancellationToken cancellationToken, Guid? selectId = null)
    {
        var profiles = await _profileRepository.ListAsync(cancellationToken).ConfigureAwait(true);
        var selectedId = selectId ?? await _profileRepository.GetSelectedProfileIdAsync(cancellationToken)
            .ConfigureAwait(true);

        Profiles.Clear();
        foreach (var profile in profiles)
        {
            Profiles.Add(new ProfileItemViewModel(profile));
        }
        SelectedProfile = Profiles.FirstOrDefault(item => item.Id == selectedId) ?? Profiles.FirstOrDefault();
        UpdateProfileConnectionStates();
        RefreshDeadLetterEnvironmentFilters();
        OnPropertyChanged(nameof(HasProfiles));
        OnPropertyChanged(nameof(EnvironmentActionLabel));
        OnPropertyChanged(nameof(EnvironmentActionCommand));
        NotifyCommandStates();
    }

    private async Task ConnectSelectedAsync(CancellationToken cancellationToken)
    {
        var selected = SelectedProfile ?? throw new InvalidOperationException("Select an environment first.");
        await ConnectProfileAsync(selected, selected.Profile, loadTopology: true, cancellationToken).ConfigureAwait(true);
        SelectedDeadLetterEnvironmentFilter = DeadLetterEnvironmentFilters
            .FirstOrDefault(filter => filter.ProfileId == selected.Id);
        await _profileRepository.SetSelectedProfileIdAsync(selected.Id, cancellationToken).ConfigureAwait(true);
        StatusText = $"Connected to {selected.Name}";
        AddActivity("Success", "Connected", $"{selected.Name} · {ConnectedNamespace}");
    }

    private async Task DisconnectSelectedAsync(CancellationToken cancellationToken)
    {
        var selected = SelectedProfile ?? throw new InvalidOperationException("Select an environment first.");
        if (_workspace.ConnectedProfileId != selected.Id)
        {
            return;
        }

        await _workspace.DisconnectAsync(cancellationToken).ConfigureAwait(true);
        await StopWriteUnlockTimerAsync().ConfigureAwait(true);
        ClearConnectedState();
        StatusText = $"Disconnected from {selected.Name}";
        AddActivity("Info", "Disconnected", selected.Name);
    }

    private async Task ConnectProfileAsync(
        ProfileItemViewModel item,
        ServiceBusProfile connectionProfile,
        bool loadTopology,
        CancellationToken cancellationToken)
    {
        var previousConnectedProfileId = _connectedProfile?.Id;
        ServiceBusTopology? topology = null;
        try
        {
            await _workspace.ConnectAsync(connectionProfile, cancellationToken).ConfigureAwait(true);
            if (loadTopology)
            {
                topology = await _workspace.GetTopologyAsync(forceRefresh: true, cancellationToken)
                    .ConfigureAwait(true);
            }
        }
        catch
        {
            if (_workspace.ConnectionState == WorkspaceConnectionState.Connected)
            {
                try
                {
                    await _workspace.DisconnectAsync(CancellationToken.None).ConfigureAwait(true);
                }
                catch (Exception disconnectException)
                {
                    // Preserve the original connection/topology error for the operator.
                    _logger.LogWarning(disconnectException, "Disconnect after a failed connection also failed");
                }
            }
            ClearConnectedState();
            throw;
        }

        _connectedProfile = connectionProfile;
        var profileChanged = previousConnectedProfileId.HasValue &&
                             previousConnectedProfileId.Value != connectionProfile.Id;
        if (profileChanged)
        {
            Messages.Clear();
            SelectedMessage = null;
            SelectedDestination = null;
        }
        NotifyConnectionState();

        if (topology is not null)
        {
            ApplyTopology(topology, preserveDestination: !profileChanged);
        }
    }

    private async Task RestoreConnectedProfileAsync(
        ProfileItemViewModel item,
        ServiceBusProfile previouslyConnectedProfile,
        DateTimeOffset? temporaryWriteExpiry,
        CancellationToken cancellationToken)
    {
        // Always reconnect with the persisted access mode first. A captured temporary
        // ReadWrite record must never become permanent if a scan/search cleared its timer.
        await ConnectProfileAsync(item, item.Profile, loadTopology: true, cancellationToken)
            .ConfigureAwait(true);

        if (!item.Profile.CanWrite &&
            previouslyConnectedProfile.CanWrite &&
            temporaryWriteExpiry is { } expiresAt &&
            expiresAt > DateTimeOffset.UtcNow)
        {
            try
            {
                await _workspace.SetAccessModeAsync(ProfileAccessMode.ReadWrite, cancellationToken)
                    .ConfigureAwait(true);
                _connectedProfile = previouslyConnectedProfile;
                RestoreTemporaryWriteUnlockTimer(item.Id, expiresAt);
                NotifyConnectionState();
            }
            catch
            {
                try
                {
                    await _workspace.DisconnectAsync(CancellationToken.None).ConfigureAwait(true);
                }
                catch (Exception disconnectException)
                {
                    // Preserve the access-mode restoration failure.
                    _logger.LogWarning(disconnectException, "Disconnect after a failed access-mode restore also failed");
                }
                ClearConnectedState();
                throw;
            }
        }
    }

    private void RestoreTemporaryWriteUnlockTimer(Guid profileId, DateTimeOffset expiresAt)
    {
        if (_writeUnlockProfileId == profileId &&
            _writeUnlockExpiresAt == expiresAt &&
            _writeUnlockTask is not null)
        {
            return;
        }

        CancelWriteUnlockTimerWithoutWaiting();
        _writeUnlockCancellation = new CancellationTokenSource();
        _writeUnlockProfileId = profileId;
        _writeUnlockExpiresAt = expiresAt;
        _writeUnlockTask = RelockAfterDelayAsync(profileId, expiresAt, _writeUnlockCancellation.Token);
    }

    private void ClearConnectedState()
    {
        ResetBrowsePaging();
        _connectedProfile = null;
        CancelWriteUnlockTimerWithoutWaiting();
        _topology = null;
        _allEntities.Clear();
        Entities.Clear();
        SelectedEntity = null;
        Destinations.Clear();
        Messages.Clear();
        SelectedMessage = null;
        SelectedDestination = null;
        NotifyConnectionState();
        NotifyStatistics();
    }

    private void InvalidateProfileArtifacts(Guid profileId, string draftNotice)
    {
        foreach (var source in DeadLetterSources.Where(item => item.ProfileId == profileId).ToArray())
        {
            DeadLetterSources.Remove(source);
        }
        if (_preferredDlqSourceProfileId == profileId)
        {
            _preferredDlqSourceProfileId = null;
            _preferredDlqSourceEntity = null;
            _preferredDlqSourceSubQueue = null;
        }
        ApplyDeadLetterEnvironmentFilter();

        var keyPrefix = $"{profileId:N}|";
        foreach (var key in _previousDlqCounts.Keys.Where(key => key.StartsWith(keyPrefix, StringComparison.Ordinal)).ToArray())
        {
            _previousDlqCounts.Remove(key);
        }
        foreach (var key in _monitorNotifications.Keys.Where(key => key.StartsWith(keyPrefix, StringComparison.Ordinal)).ToArray())
        {
            var notification = _monitorNotifications[key];
            _monitorNotifications.Remove(key);
            MonitorNotifications.Remove(notification);
        }
        NotifyMonitorNotificationsChanged();

        Messages.Clear();
        SelectedMessage = null;
        if (_draftProfileId == profileId)
        {
            _draftProfileId = null;
            _draftProfileName = null;
            _draftSourceMessage = null;
            _draftSourceIsLocalBackup = false;
            SelectedDestination = null;
            DraftOriginNotice = draftNotice;
            OnPropertyChanged(nameof(HasDraftEnvironmentMismatch));
            OnPropertyChanged(nameof(DraftEnvironmentWarning));
            SendDraftCommand.NotifyCanExecuteChanged();
        }
        NotifyStatistics();
    }

    private ProfileItemViewModel GetConnectedProfileItem()
    {
        var connectedProfileId = _workspace.ConnectedProfileId
            ?? throw new InvalidOperationException("Connect to an environment first.");
        return Profiles.FirstOrDefault(profile => profile.Id == connectedProfileId)
            ?? throw new InvalidOperationException("The connected environment no longer exists.");
    }

    private void NotifyConnectionState()
    {
        UpdateProfileConnectionStates();
        OnPropertyChanged(nameof(IsConnected));
        OnPropertyChanged(nameof(ConnectedProfileId));
        OnPropertyChanged(nameof(ConnectedProfileName));
        OnPropertyChanged(nameof(ConnectedAuthenticationLabel));
        OnPropertyChanged(nameof(ConnectionLabel));
        OnPropertyChanged(nameof(ConnectionTone));
        OnPropertyChanged(nameof(ConnectedNamespace));
        OnPropertyChanged(nameof(ConnectedProvider));
        OnPropertyChanged(nameof(BrowseModeLabel));
        OnPropertyChanged(nameof(BrowseModeDescription));
        OnPropertyChanged(nameof(DeadLetterCountCaption));
        OnPropertyChanged(nameof(IsSelectedProfileConnected));
        OnPropertyChanged(nameof(EnvironmentActionLabel));
        OnPropertyChanged(nameof(EnvironmentActionCommand));
        OnPropertyChanged(nameof(CanOpenBackupAsDraft));
        OnPropertyChanged(nameof(BackupDraftHint));
        OnPropertyChanged(nameof(HasBackupDraftHint));
        OnPropertyChanged(nameof(CanWrite));
        OnPropertyChanged(nameof(CanUnlockWrites));
        OnPropertyChanged(nameof(HasDraftEnvironmentMismatch));
        OnPropertyChanged(nameof(DraftEnvironmentWarning));
        OnPropertyChanged(nameof(MonitorTargetPreview));
        OnPropertyChanged(nameof(WriteAccessLabel));
        OnPropertyChanged(nameof(WriteAccessTone));
        NotifyCommandStates();
    }

    private void UpdateProfileConnectionStates()
    {
        var connectedProfileId = IsConnected ? _workspace.ConnectedProfileId : null;
        foreach (var profile in Profiles)
        {
            profile.UpdateConnectionState(profile.Id == connectedProfileId);
        }
    }
}
