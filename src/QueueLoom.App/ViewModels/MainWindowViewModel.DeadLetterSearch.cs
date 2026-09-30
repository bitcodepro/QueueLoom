using QueueLoom.App.Models;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Searching dead-letter queues across environments.</summary>
public sealed partial class MainWindowViewModel
{
    private bool _deepSearch;

    /// <summary>
    /// Deep search reads up to 20,000 dead letters per queue and keeps up to 20,000 matches, for large dead-letter
    /// queues; it may take several minutes. The normal search stops at 1,000 per queue and 500 matches.
    /// </summary>
    public bool DeepSearch
    {
        get => _deepSearch;
        set => SetProperty(ref _deepSearch, value);
    }

    private async Task SearchDeadLettersAsync(CancellationToken cancellationToken)
    {
        ResetBrowsePaging();
        var query = DeadLetterSearchQuery.Trim();
        if (query.Length == 0)
        {
            throw new InvalidOperationException("Enter a search value first.");
        }

        var filter = SelectedDeadLetterEnvironmentFilter
            ?? throw new InvalidOperationException("Select an environment filter first.");
        var profilesToSearch = filter.ProfileId is { } profileId
            ? Profiles.Where(profile => profile.Id == profileId).ToArray()
            : [];
        if (profilesToSearch.Length == 0)
        {
            throw new InvalidOperationException("The selected search scope contains no environments.");
        }

        var deep = DeepSearch;
        var maximumResults = deep ? 20_000 : DeadLetterSearchRequest.DefaultMaximumResults;
        var perTarget = deep ? 20_000 : DeadLetterSearchRequest.DefaultMaximumMessagesPerTarget;
        var connectedProfileBeforeSearch = _connectedProfile;
        var wasConnected = IsConnected && connectedProfileBeforeSearch is not null;
        var temporaryWriteExpiryBeforeSearch = connectedProfileBeforeSearch is not null &&
                                               _writeUnlockProfileId == connectedProfileBeforeSearch.Id
            ? _writeUnlockExpiresAt
            : null;
        var results = new List<MessageItemViewModel>();
        var scannedMessages = 0;
        var searchedTargets = 0;
        var sourceFailures = 0;
        var environmentFailures = 0;
        var timedOutEnvironments = 0;
        var incomplete = false;
        var restoreFailed = false;
        using var totalSearchTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        totalSearchTimeout.CancelAfter(deep ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60));
        var searchToken = totalSearchTimeout.Token;

        try
        {
            foreach (var profile in profilesToSearch)
            {
                if (searchToken.IsCancellationRequested)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    timedOutEnvironments++;
                    incomplete = true;
                    break;
                }
                if (results.Count >= maximumResults)
                {
                    incomplete = true;
                    break;
                }

                StatusText = $"Reading topology and DLQ counts in {profile.Name}...";
                try
                {
                    if (_workspace.ConnectedProfileId != profile.Id)
                    {
                        var readOnlyProfile = profile.Profile with { AccessMode = ProfileAccessMode.ReadOnly };
                        await ConnectProfileAsync(profile, readOnlyProfile, loadTopology: false, searchToken)
                            .ConfigureAwait(true);
                    }

                    var topology = await _workspace.GetTopologyAsync(forceRefresh: true, searchToken)
                        .ConfigureAwait(true);
                    var targets = DeadLetterSearchTargets.ForTopology(topology);
                    if (targets.Length == 0)
                    {
                        continue;
                    }

                    StatusText = $"Searching {targets.Length:N0} DLQ sources in {profile.Name}...";
                    using var environmentTimeout = CancellationTokenSource.CreateLinkedTokenSource(searchToken);
                    environmentTimeout.CancelAfter(deep ? TimeSpan.FromMinutes(10) : TimeSpan.FromSeconds(30));
                    try
                    {
                        var search = await _workspace.SearchDeadLettersAsync(
                                new DeadLetterSearchRequest(
                                    query,
                                    targets,
                                    maximumMessagesPerTarget: perTarget,
                                    maximumResults: maximumResults - results.Count),
                                environmentTimeout.Token)
                            .ConfigureAwait(true);
                        scannedMessages = checked(scannedMessages + search.ScannedMessageCount);
                        searchedTargets += search.Sources.Count;
                        sourceFailures += search.Sources.Count(source => !source.IsSuccessful);
                        incomplete |= !search.IsComplete;
                        results.AddRange(search.Matches.Select(message => new MessageItemViewModel(
                            message,
                            profile.Id,
                            profile.Name,
                            profile.EnvironmentLabel,
                            profile.EnvironmentTone)));
                        cancellationToken.ThrowIfCancellationRequested();
                        if (environmentTimeout.IsCancellationRequested)
                        {
                            timedOutEnvironments++;
                            incomplete = true;
                            if (totalSearchTimeout.IsCancellationRequested)
                            {
                                break;
                            }
                        }
                    }
                    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                    {
                        timedOutEnvironments++;
                        incomplete = true;
                        if (totalSearchTimeout.IsCancellationRequested)
                        {
                            break;
                        }
                    }
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    timedOutEnvironments++;
                    incomplete = true;
                    break;
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    environmentFailures++;
                    incomplete = true;
                    AddActivity(
                        "Error",
                        "DLQ search environment failed",
                        $"{profile.Name} | {SanitizeException(exception)}");
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Messages.Clear();
            SelectedMessage = null;
            MessageListTitle = "Search cancelled";
            DeadLetterSearchStatus = "Search cancelled; partial results were not applied.";
            throw;
        }
        finally
        {
            if (!_isDisposed)
            {
                using var restoreCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                try
                {
                    if (wasConnected && connectedProfileBeforeSearch is not null)
                    {
                        var originalProfile = Profiles.FirstOrDefault(profile =>
                            profile.Id == connectedProfileBeforeSearch.Id);
                        if (originalProfile is not null &&
                            (_workspace.ConnectedProfileId != originalProfile.Id ||
                             _connectedProfile != connectedProfileBeforeSearch))
                        {
                            await RestoreConnectedProfileAsync(
                                    originalProfile,
                                    connectedProfileBeforeSearch,
                                    temporaryWriteExpiryBeforeSearch,
                                    restoreCancellation.Token)
                                .ConfigureAwait(true);
                        }
                    }
                    else if (_workspace.ConnectionState == WorkspaceConnectionState.Connected)
                    {
                        await _workspace.DisconnectAsync(restoreCancellation.Token).ConfigureAwait(true);
                        ClearConnectedState();
                    }
                }
                catch (Exception exception)
                {
                    restoreFailed = true;
                    ClearConnectedState();
                    AddActivity("Error", "Environment restore failed", SanitizeException(exception));
                }
            }
        }

        var windowed = ApplySearchWindow(results, out var outsideWindow);
        ReplaceMessages(windowed
            .OrderBy(result => result.Message.EnqueuedAt ?? DateTimeOffset.MaxValue)
            .ThenBy(result => result.Message.SequenceNumber)
            .ThenBy(result => result.ProfileName, StringComparer.OrdinalIgnoreCase));
        SelectedMessage = Messages.FirstOrDefault();
        var scopeName = filter.ProfileId.HasValue ? filter.Name : "all environments";
        var qualifier = incomplete || restoreFailed
            ? " | incomplete: limits or errors occurred"
            : " | complete within the current DLQ snapshot";
        DeadLetterSearchStatus =
            $"{Messages.Count:N0} matches | {scannedMessages:N0} messages inspected | " +
            $"{searchedTargets:N0} sources | oldest first{qualifier}" +
            (timedOutEnvironments > 0 ? $" | {timedOutEnvironments:N0} environment timeouts" : string.Empty) +
            (outsideWindow > 0 ? $" | {outsideWindow:N0} older matches hidden ({SearchWindow.Label.ToLowerInvariant()})" : string.Empty);
        MessageListTitle = $"Search timeline | {scopeName} | body search reads up to the first 1 MiB";
        StatusText = $"Found {Messages.Count:N0} matching dead-letter messages in {scopeName}";
        AddActivity(
            incomplete || restoreFailed ? "Warning" : "Info",
            "DLQ search",
            $"{scopeName} | {Messages.Count:N0} matches | {scannedMessages:N0} inspected | " +
            $"{sourceFailures:N0} source errors | {environmentFailures:N0} environment errors | " +
            $"{timedOutEnvironments:N0} timeouts");
        ClearDeadLetterSearchCommand.NotifyCanExecuteChanged();
    }

    private void ClearDeadLetterSearch()
    {
        ResetBrowsePaging();
        DeadLetterSearchQuery = string.Empty;
        Messages.Clear();
        SelectedMessage = null;
        MessageListTitle = "Peeked messages";
        DeadLetterSearchStatus = "Search Correlation ID, Message ID, body, or application properties.";
        ClearDeadLetterSearchCommand.NotifyCanExecuteChanged();
    }
}
