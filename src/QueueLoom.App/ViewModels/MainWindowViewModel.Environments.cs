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
                if (_rebuildingEnvironmentLists == 0)
                {
                    RefreshDeadLetterEnvironmentFilters();
                }
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

    /// <summary>
    /// The operator's environment. A monitor check of another environment temporarily connects the workspace there;
    /// what the window shows and offers follows the operator's environment throughout. Work that depends on the actual
    /// connection reads <c>_workspace.ConnectedProfileId</c> under the workspace gate, which a check gives back first.
    /// </summary>
    public Guid? ConnectedProfileId => IsConnected ? _connectedProfile?.Id : null;

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
        MessagingProvider.AmazonSqsSns or MessagingProvider.GooglePubSub or MessagingProvider.RabbitMq => "Receive and release",
        MessagingProvider.Kafka => "Read by offset",
        _ => "Non-destructive Peek"
    };

    public string BrowseModeDescription => ConnectedProvider switch
    {
        MessagingProvider.AmazonSqsSns =>
            "SQS has no peek. Messages are received, hidden for up to 3 minutes and returned unchanged; their receive count goes up by one.",
        MessagingProvider.GooglePubSub =>
            "Pub/Sub has no peek. Messages are pulled, held for up to 3 minutes and returned unchanged; with a dead-letter policy each read counts as a delivery attempt.",
        MessagingProvider.RabbitMq =>
            "RabbitMQ has no peek. Messages are taken with basic.get and requeued unchanged; they are marked as redelivered, and quorum queues on RabbitMQ 4.2 and earlier count this as a delivery toward their delivery limit (4.3 and later do not).",
        MessagingProvider.Kafka =>
            "Kafka keeps messages after they are read. QueueLoom reads them by offset, without a consumer group, so nothing is committed or changed.",
        _ => "Messages are peeked without locking or changing them."
    };

    public string DeadLetterCountCaption => ConnectedProvider switch
    {
        MessagingProvider.GooglePubSub => "Pub/Sub does not report counts",
        MessagingProvider.AmazonSqsSns => "Approximate SQS counts",
        MessagingProvider.RabbitMq => "Ready messages in dead-letter queues",
        MessagingProvider.Kafka => "Messages kept in dead-letter topics",
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

    private async Task EditEnvironmentAsync(ProfileItemViewModel? target, CancellationToken cancellationToken)
    {
        var selected = target ?? throw new InvalidOperationException("Select an environment first.");
        var result = await _dialogs.EditProfileAsync(selected.Profile, cancellationToken).ConfigureAwait(true);
        if (result is null)
        {
            return;
        }

        result = result with { Profile = result.Profile with { ConfigurationRevision = Guid.NewGuid() } };
        await SaveProfileAsync(result, cancellationToken, selected.Profile).ConfigureAwait(true);

        await StopMonitorForConfigurationChangeAsync(result.Profile.Id).ConfigureAwait(true);
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

    private async Task SaveProfileAsync(ProfileEditorResult result, CancellationToken cancellationToken, ServiceBusProfile? expectedProfile = null)
    {
        var coordinator = _profileRepository as IProfileMutationCoordinator;
        await using var mutation = coordinator is null ? null : await coordinator.AcquireProfileMutationAsync(cancellationToken).ConfigureAwait(true);
        var secretKey = ProfileSecretKey.ConnectionString(result.Profile.Id);
        var previousProfile = await _profileRepository.GetAsync(result.Profile.Id, cancellationToken)
            .ConfigureAwait(true);
        EnsureProfileUnchanged(expectedProfile, previousProfile);
        var previousSelectedProfileId = await _profileRepository.GetSelectedProfileIdAsync(cancellationToken)
            .ConfigureAwait(true);
        var replacesConnectionString = result.ReplacesConnectionString && result.ConnectionString is not null;
        var removesConnectionString = previousProfile?.Authentication.Kind.UsesStoredSecret() == true &&
                                      !result.Profile.Authentication.Kind.UsesStoredSecret();
        var changesConnectionString = replacesConnectionString || removesConnectionString;
        string? previousConnectionString = null;
        if (changesConnectionString)
        {
            previousConnectionString = await _secretVault.RetrieveAsync(secretKey, cancellationToken)
                .ConfigureAwait(true);
        }
        var registryKey = ProfileSecretKey.SchemaRegistryPassword(result.Profile.Id);
        var changesRegistryPassword = result.SchemaRegistryPassword is not null || result.RemovesSchemaRegistryPassword;
        var alreadyPending = coordinator?.IsCredentialUpdatePending(result.Profile.Id) == true;
        if (alreadyPending &&
            (result.Profile.Authentication.Kind.UsesStoredSecret() && !replacesConnectionString ||
             result.Profile.Kafka?.SchemaRegistryUserName is not null && result.SchemaRegistryPassword is null))
            throw new InvalidOperationException("The previous credential update is incomplete. Re-enter all credentials used by this environment before saving.");
        string? previousRegistryPassword = null;
        if (changesRegistryPassword)
        {
            previousRegistryPassword = await _secretVault.RetrieveAsync(registryKey, cancellationToken)
                .ConfigureAwait(true);
        }

        var secretReplacementAttempted = false;
        var registryPasswordAttempted = false;
        var metadataRollbackNeeded = false;
        var pendingMarked = coordinator is not null && (changesConnectionString || changesRegistryPassword || alreadyPending);
        if (pendingMarked)
            await coordinator!.MarkCredentialUpdatePendingAsync(result.Profile.Id, cancellationToken).ConfigureAwait(true);
        try
        {
            if (changesConnectionString)
            {
                secretReplacementAttempted = true;
                if (replacesConnectionString)
                    await _secretVault.StoreAsync(secretKey, result.ConnectionString!, cancellationToken).ConfigureAwait(true);
                else
                    await _secretVault.RemoveAsync(secretKey, cancellationToken).ConfigureAwait(true);
            }
            if (changesRegistryPassword)
            {
                registryPasswordAttempted = true;
                await SaveSchemaRegistryPasswordAsync(result, cancellationToken).ConfigureAwait(true);
            }

            // The outer profile lock owns the entire save/rollback. Individual vault and
            // repository calls take the separate storage lock sequentially.
            if (_profileRepository is IAtomicProfileRepository atomicRepository)
            {
                await atomicRepository.UpsertAndSelectAsync(result.Profile, cancellationToken).ConfigureAwait(true);
            }
            else
            {
                metadataRollbackNeeded = true;
                await _profileRepository.UpsertAsync(result.Profile, cancellationToken).ConfigureAwait(true);
                await _profileRepository.SetSelectedProfileIdAsync(result.Profile.Id, cancellationToken).ConfigureAwait(true);
            }
        }
        catch (Exception saveException)
        {
            var rollbackFailures = new List<Exception>();
            if (metadataRollbackNeeded)
            {
                try
                {
                    if (previousProfile is null)
                        await _profileRepository.DeleteAsync(result.Profile.Id, CancellationToken.None).ConfigureAwait(true);
                    else
                        await _profileRepository.UpsertAsync(previousProfile, CancellationToken.None).ConfigureAwait(true);
                }
                catch (Exception rollbackException)
                {
                    rollbackFailures.Add(rollbackException);
                }
                try
                {
                    await _profileRepository.SetSelectedProfileIdAsync(
                        previousSelectedProfileId, CancellationToken.None).ConfigureAwait(true);
                }
                catch (Exception rollbackException)
                {
                    rollbackFailures.Add(rollbackException);
                }
            }
            if (registryPasswordAttempted)
            {
                try
                {
                    if (previousRegistryPassword is null)
                        await _secretVault.RemoveAsync(registryKey, CancellationToken.None).ConfigureAwait(true);
                    else
                        await _secretVault.StoreAsync(registryKey, previousRegistryPassword, CancellationToken.None)
                            .ConfigureAwait(true);
                }
                catch (Exception rollbackException)
                {
                    rollbackFailures.Add(rollbackException);
                }
            }
            if (secretReplacementAttempted)
            {
                try
                {
                    if (previousConnectionString is null)
                        await _secretVault.RemoveAsync(secretKey, CancellationToken.None).ConfigureAwait(true);
                    else
                        await _secretVault.StoreAsync(secretKey, previousConnectionString, CancellationToken.None)
                            .ConfigureAwait(true);
                }
                catch (Exception rollbackException)
                {
                    rollbackFailures.Add(rollbackException);
                }
            }
            if (rollbackFailures.Count == 0 && pendingMarked && !alreadyPending)
            {
                try { coordinator!.CompleteCredentialUpdate(result.Profile.Id); }
                catch (Exception exception) { rollbackFailures.Add(exception); }
            }
            if (rollbackFailures.Count > 0)
            {
                throw new AggregateException(
                    "Saving the environment failed and its previous state could not be fully restored.",
                    [saveException, .. rollbackFailures]);
            }
            throw;
        }
        // If marker cleanup fails, retain the committed profile and credentials together.
        // Readers remain blocked; do not roll credentials back after metadata committed.
        if (pendingMarked) coordinator!.CompleteCredentialUpdate(result.Profile.Id);
    }

    private static void EnsureProfileUnchanged(ServiceBusProfile? expected, ServiceBusProfile? current)
    {
        if (expected is not null && System.Text.Json.JsonSerializer.Serialize(expected) != System.Text.Json.JsonSerializer.Serialize(current))
            throw new InvalidOperationException("The environment changed or was removed in another window. Refresh environments and edit it again.");
    }

    private async Task SaveSchemaRegistryPasswordAsync(ProfileEditorResult result, CancellationToken cancellationToken)
    {
        var key = ProfileSecretKey.SchemaRegistryPassword(result.Profile.Id);
        if (result.SchemaRegistryPassword is { } password)
        {
            await _secretVault.StoreAsync(key, password, cancellationToken).ConfigureAwait(true);
        }
        else if (result.RemovesSchemaRegistryPassword)
        {
            await _secretVault.RemoveAsync(key, cancellationToken).ConfigureAwait(true);
        }
    }

    private async Task DeleteEnvironmentAsync(ProfileItemViewModel? target, CancellationToken cancellationToken)
    {
        var selected = target ?? throw new InvalidOperationException("Select an environment first.");
        // A scheduled resend runs only in its own environment, and a removed environment never comes back under the
        // same ID (an import gets a new one): its resends are cancelled with it rather than left waiting forever.
        var waitingResends = ScheduledResends.Count(item => item.Resend.ProfileId == selected.Id);
        var confirmed = await _dialogs.ConfirmAsync(
            "Delete environment",
            $"Remove '{selected.Name}' and its locally encrypted credential? Nothing changes in " +
            $"{selected.Profile.Provider.DisplayName()}: its queues, topics and messages stay." +
            (waitingResends > 0
                ? $"\n\n{waitingResends:N0} scheduled resend(s) waiting for this environment are cancelled; nothing is sent."
                : string.Empty),
            isDangerous: true,
            requiredText: selected.Name,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            return;
        }

        var coordinator = _profileRepository as IProfileMutationCoordinator;
        await using var mutation = coordinator is null ? null : await coordinator.AcquireProfileMutationAsync(cancellationToken).ConfigureAwait(true);
        EnsureProfileUnchanged(selected.Profile, await _profileRepository.GetAsync(selected.Id, cancellationToken).ConfigureAwait(true));

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
        await _secretVault.RemoveAsync(ProfileSecretKey.SchemaRegistryPassword(selected.Id), CancellationToken.None)
            .ConfigureAwait(true);
        coordinator?.CompleteCredentialUpdate(selected.Id);
        if (_writeUnlockProfileId == selected.Id)
        {
            await StopWriteUnlockTimerAsync().ConfigureAwait(true);
        }
        await StopMonitorForConfigurationChangeAsync(selected.Id).ConfigureAwait(true);
        InvalidateProfileArtifacts(selected.Id, "Its environment was removed; the previous draft is no longer sendable.");
        // Every resend saved for this environment is cancelled, including ones another window scheduled after this
        // window loaded its list: the saved list is what windows run from. This happens while the environment
        // change is still held, so no window can claim one of them in between (see RunScheduledAsync).
        // The environment is gone already: a failure to read or update the saved schedules is reported as unfinished
        // cleanup, and the list of environments is refreshed regardless.
        var cancelledResends = 0;
        var removedIds = new HashSet<Guid>();
        string? cleanupProblem = null;
        IReadOnlyList<ScheduledResend> saved;
        try
        {
            saved = _scheduledStore is not null
                ? (await _scheduledStore.LoadAsync(cancellationToken).ConfigureAwait(true)).Where(resend => resend.ProfileId == selected.Id).ToArray()
                : ScheduledResends.Where(item => item.Resend.ProfileId == selected.Id).Select(item => item.Resend).ToArray();
            // Listed here but no longer saved: another window ran or cancelled it. Nothing of it is left to cancel.
            if (_scheduledStore is not null)
            {
                removedIds.UnionWith(ScheduledResends.Where(item => item.Resend.ProfileId == selected.Id)
                    .Select(item => item.Resend.Id).Except(saved.Select(resend => resend.Id)));
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or InvalidDataException or System.Text.Json.JsonException)
        {
            cleanupProblem = exception.Message;
            saved = ScheduledResends.Where(item => item.Resend.ProfileId == selected.Id).Select(item => item.Resend).ToArray();
        }
        await ReportSetAsideSchedulesAsync().ConfigureAwait(true);
        foreach (var resend in saved)
        {
            try
            {
                if (await RemoveScheduledAsync(resend, cancellationToken).ConfigureAwait(true))
                {
                    cancelledResends++;
                }
                removedIds.Add(resend.Id);
            }
            catch (IOException exception)
            {
                // Left listed; it can still be cancelled on Activity.
                cleanupProblem ??= exception.Message;
            }
        }
        foreach (var item in ScheduledResends.Where(item => item.Resend.ProfileId == selected.Id && removedIds.Contains(item.Resend.Id)).ToArray())
        {
            ScheduledResends.Remove(item);
        }
        AddActivity("Warning", "Environment removed", selected.Name +
            (cancelledResends > 0 ? $" · {cancelledResends:N0} scheduled resend(s) cancelled; nothing was sent" : string.Empty) +
            (cleanupProblem is null
                ? string.Empty
                : $" · its scheduled resends could not all be checked or cancelled ({cleanupProblem}); any left are never sent, since " +
                  "their environment is gone, and can be cancelled on Activity"));
        await ReloadProfilesAsync(cancellationToken).ConfigureAwait(true);
    }

    private async Task ReloadProfilesAsync(CancellationToken cancellationToken, Guid? selectId = null)
    {
        var profiles = await _profileRepository.ListAsync(cancellationToken).ConfigureAwait(true);
        var selectedId = selectId ?? await _profileRepository.GetSelectedProfileIdAsync(cancellationToken)
            .ConfigureAwait(true);

        // While the list is rebuilt, the bound ComboBox sets SelectedProfile to null; the search scope must not follow.
        var scope = _selectedDeadLetterEnvironmentFilter;
        _rebuildingEnvironmentLists++;
        try
        {
            Profiles.Clear();
            foreach (var profile in profiles)
            {
                Profiles.Add(new ProfileItemViewModel(profile));
            }
            SelectedProfile = Profiles.FirstOrDefault(item => item.Id == selectedId) ?? Profiles.FirstOrDefault();
        }
        finally
        {
            _rebuildingEnvironmentLists--;
        }
        UpdateProfileConnectionStates();
        RefreshDeadLetterEnvironmentFilters(scope);
        OnPropertyChanged(nameof(HasProfiles));
        OnPropertyChanged(nameof(EnvironmentActionLabel));
        OnPropertyChanged(nameof(EnvironmentActionCommand));
        NotifyCommandStates();
    }

    private async Task ConnectSelectedAsync(ProfileItemViewModel? target, CancellationToken cancellationToken)
    {
        var selected = target ?? throw new InvalidOperationException("Select an environment first.");
        await ConnectProfileAsync(selected, selected.Profile, loadTopology: true, cancellationToken).ConfigureAwait(true);
        SelectedDeadLetterEnvironmentFilter = DeadLetterEnvironmentFilters
            .FirstOrDefault(filter => filter.ProfileId == selected.Id);
        await _profileRepository.SetSelectedProfileIdAsync(selected.Id, cancellationToken).ConfigureAwait(true);
        StatusText = $"Connected to {selected.Name}";
        AddActivity("Success", "Connected", $"{selected.Name} · {ConnectedNamespace}");
    }

    private async Task DisconnectSelectedAsync(ProfileItemViewModel? target, CancellationToken cancellationToken)
    {
        var selected = target ?? throw new InvalidOperationException("Select an environment first.");
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
        RefreshConsumerRows();
        Entities.Clear();
        SelectedEntity = null;
        Destinations.Clear();
        Messages.Clear();
        SelectedMessage = null;
        SelectedDestination = null;
        ReplayDestination = null;
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

        // Only the rows of the changed or removed environment go; another environment's results and ticks stay.
        using (BatchMessageUpdates())
        {
            foreach (var item in Messages.Where(item => item.ProfileId == profileId).ToArray())
            {
                Messages.Remove(item);
            }
        }
        if (SelectedMessage is not null && !Messages.Contains(SelectedMessage))
        {
            SelectedMessage = Messages.FirstOrDefault();
        }
        if (_draftProfileId == profileId)
        {
            _draftProfileId = null;
            _draftProfileName = null;
            _draftSourceMessage = null;
            _draftSubjectSource = null;
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
        OnPropertyChanged(nameof(SubjectHint));
        OnPropertyChanged(nameof(HasSubjectHint));
        OnPropertyChanged(nameof(BrowseModeLabel));
        OnPropertyChanged(nameof(BrowseModeDescription));
        NotifyBrowseDeliveryNotes();
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
