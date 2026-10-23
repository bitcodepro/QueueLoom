using System.Text.Json;
using Microsoft.Extensions.Logging;
using QueueLoom.Core.Monitoring;

namespace QueueLoom.App.ViewModels;

/// <summary>
/// The monitor's open incidents survive a restart (an update, a reboot, the tray relaunching the app): the first check
/// after it compares with what was already alerted instead of alerting every non-empty dead-letter queue as new.
/// </summary>
public sealed partial class MainWindowViewModel
{
    /// <summary>Unchanged incidents are saved again this often, so the file's age says the monitor still saw them.</summary>
    private static readonly TimeSpan UnchangedIncidentsResaveInterval = TimeSpan.FromHours(1);

    private readonly IMonitorIncidentStore? _monitorIncidentStore;
    private bool _monitorIncidentsRestored;
    private string? _lastSavedIncidents;
    private DateTimeOffset _lastIncidentsSavedAt;
    private Task _monitorIncidentSave = Task.CompletedTask;

    /// <summary>Tests and shutdown: the saves started so far have finished.</summary>
    internal Task MonitorIncidentsSaved => _monitorIncidentSave;

    /// <summary>
    /// Incidents of environments that still exist come back as they were, but are not listed: they were alerted (and
    /// perhaps dismissed with Clear) before the restart, and are shown again only when they alert again, like a
    /// dismissed one.
    /// </summary>
    private async Task RestoreMonitorIncidentsAsync(CancellationToken cancellationToken)
    {
        if (_monitorIncidentStore is null || _monitorIncidentsRestored)
        {
            return;
        }
        try
        {
            var profiles = Profiles.ToDictionary(profile => profile.Id);
            foreach (var incident in await _monitorIncidentStore.LoadAsync(cancellationToken).ConfigureAwait(true))
            {
                if (incident.ProfileId is not { } profileId || !profiles.TryGetValue(profileId, out var profile) ||
                    _monitorNotifications.ContainsKey(incident.Key))
                {
                    continue;
                }
                _monitorNotifications[incident.Key] = new MonitorNotificationItemViewModel(
                    incident.Key, profile.Name, incident.Source, incident.SubQueueLabel, incident.Count, incident.FirstDetectedAt)
                {
                    CountQuality = incident.CountQuality,
                    LastDetectedAt = incident.LastDetectedAt,
                    LastMeasuredAt = incident.LastMeasuredAt,
                    MeasuredFrom = incident.MeasuredFrom,
                    SharedName = incident.SharedName,
                    ContentMarkers = incident.ContentMarkers,
                    Offsets = incident.Offsets
                };
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // Without them the monitor alerts what it finds as new, as before: never a reason not to start.
            _logger.LogWarning(exception, "The monitor incidents could not be restored");
        }
        _monitorIncidentsRestored = true;
    }

    /// <summary>
    /// Saves the open incidents when they changed, one save after the other, off the window's thread. Nothing is saved
    /// before the saved ones were restored, which would replace them with none.
    /// </summary>
    private void SaveMonitorIncidents()
    {
        if (_monitorIncidentStore is null || !_monitorIncidentsRestored || _isDisposed)
        {
            return;
        }
        var incidents = _monitorNotifications.Values.Select(item =>
            new MonitorIncident(item.Key, item.Source, item.SubQueueLabel, item.Count, item.CountQuality, item.FirstDetectedAt, item.LastDetectedAt)
            {
                LastMeasuredAt = item.LastMeasuredAt,
                MeasuredFrom = item.MeasuredFrom,
                SharedName = item.SharedName,
                ContentMarkers = item.ContentMarkers,
                Offsets = item.Offsets
            }).ToArray();
        var text = JsonSerializer.Serialize(incidents);
        var now = DateTimeOffset.UtcNow;
        if (text == _lastSavedIncidents && now - _lastIncidentsSavedAt < UnchangedIncidentsResaveInterval)
        {
            return;
        }
        _lastSavedIncidents = text;
        _lastIncidentsSavedAt = now;
        var store = _monitorIncidentStore;
        var previous = _monitorIncidentSave;
        _monitorIncidentSave = Task.Run(async () =>
        {
            await previous.ConfigureAwait(false);
            try
            {
                // Not cancelled at shutdown: the last incidents are what the next start compares with.
                await store.SaveAsync(incidents).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                // The next change tries again; until then a restart may alert an incident once more.
                _lastSavedIncidents = null;
                _logger.LogWarning(exception, "The monitor incidents could not be saved");
            }
        });
    }
}
