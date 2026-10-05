using System.Collections.ObjectModel;
using QueueLoom.App.Commands;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>
/// Resends that wait for a time. They are saved, so they survive a restart, and run while QueueLoom is open,
/// connected to their environment with write access on. Nothing connects or unlocks by itself.
/// </summary>
public sealed partial class MainWindowViewModel
{
    private static readonly TimeSpan ScheduleCheckInterval = TimeSpan.FromSeconds(20);
    private IScheduledResendStore? _scheduledStore;
    private CancellationTokenSource? _scheduleCancellation;
    private Task? _scheduleTask;

    /// <summary>The clock for scheduled resends; tests replace it.</summary>
    public TimeProvider Clock { get; set; } = TimeProvider.System;

    public ObservableCollection<ScheduledResendItemViewModel> ScheduledResends { get; } = [];

    public bool HasScheduledResends => ScheduledResends.Count > 0;

    public RelayCommand<ScheduledResendItemViewModel> RunScheduledResendCommand { get; private set; } = null!;

    public RelayCommand<ScheduledResendItemViewModel> CancelScheduledResendCommand { get; private set; } = null!;

    private void InitializeScheduledResends(IScheduledResendStore? store)
    {
        _scheduledStore = store;
        RunScheduledResendCommand = new RelayCommand<ScheduledResendItemViewModel>(
            item => _ = RunScheduledNowAsync(item!),
            item => item is not null && !IsBusy);
        CancelScheduledResendCommand = new RelayCommand<ScheduledResendItemViewModel>(CancelScheduled, item => item is not null);
        ScheduledResends.CollectionChanged += (_, _) => OnPropertyChanged(nameof(HasScheduledResends));
        foreach (var resend in store?.Load() ?? [])
        {
            ScheduledResends.Add(CreateScheduledItem(resend));
        }
        ReportSetAsideSchedules();
        UpdateScheduledStatuses();
    }

    // The store sets a damaged list aside whenever it reads one: at start, when a job is added or removed, when an
    // environment is deleted, or in a background read. Its jobs will not run, so they leave the list here too and
    // the operator is told.
    private void ReportSetAsideSchedules()
    {
        if (_scheduledStore?.TakeSetAsideFile() is not { } aside)
        {
            return;
        }
        HashSet<Guid>? pending;
        try
        {
            pending = _scheduledStore.Load().Select(resend => resend.Id).ToHashSet();
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            pending = null;
        }
        var lost = pending is null ? [] : ScheduledResends.Where(item => !pending.Contains(item.Resend.Id)).ToArray();
        foreach (var item in lost)
        {
            ScheduledResends.Remove(item);
        }
        AddActivity("Warning", "Scheduled resends not loaded",
            $"The list of scheduled resends was damaged and could not be read, so none of its resends will run" +
            (lost.Length > 0 ? $" ({lost.Length:N0} removed from this list)" : string.Empty) +
            $". It was kept as {aside}; schedule them again from their dead-letter queues.");
    }

    /// <summary>Starts checking for due resends; called once the window is up.</summary>
    public void StartScheduledResends()
    {
        if (_isDisposed || _scheduleTask is not null)
        {
            return;
        }
        _scheduleCancellation = new CancellationTokenSource();
        _scheduleTask = ScheduleLoopAsync(_scheduleCancellation.Token);
    }

    private async Task ScheduleLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(ScheduleCheckInterval, Clock, cancellationToken).ConfigureAwait(true);
                await RunDueScheduledResendsAsync(cancellationToken).ConfigureAwait(true);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    /// <summary>Runs every due resend whose environment is connected with write access, one at a time.</summary>
    public async Task RunDueScheduledResendsAsync(CancellationToken cancellationToken = default)
    {
        // A damaged list can be found by a read elsewhere (the background history cleanup); it is reported here at
        // the latest.
        ReportSetAsideSchedules();
        UpdateScheduledStatuses();
        var now = Clock.GetUtcNow();
        foreach (var item in ScheduledResends.ToArray())
        {
            if (!item.Resend.IsDue(now) || IsBusy || ConnectedProfileId != item.Resend.ProfileId || !CanWrite)
            {
                continue;
            }
            if (_connectedProfile is null || item.Resend.ConfigurationIdentity != ScheduledResend.IdentityFor(_connectedProfile)) continue;
            await RunWorkspaceOperationAsync("Running a scheduled resend", ct => RunScheduledAsync(item, ct), cancellationToken,
                allowCancellation: true).ConfigureAwait(true);
        }
    }

    /// <summary>"Run now" on Activity: the same checks as when it is due, just earlier.</summary>
    public Task RunScheduledNowAsync(ScheduledResendItemViewModel item) =>
        RunWorkspaceOperationAsync("Running a scheduled resend", ct => RunScheduledAsync(item, ct), CancellationToken.None, allowCancellation: true);

    private void ScheduleResend(ServiceBusProfile profile, IReadOnlyList<ResendItem> items, ResendOptions options, DateTimeOffset sendAt)
    {
        if (ScheduledResends.Count >= ScheduledResend.MaximumPending)
        {
            throw new InvalidOperationException($"At most {ScheduledResend.MaximumPending} resends can wait at once. Run or cancel one on Activity.");
        }
        var resend = new ScheduledResend(
            Guid.NewGuid(),
            profile.Id,
            profile.Name,
            Clock.GetUtcNow(),
            sendAt.ToUniversalTime(),
            options.Mode,
            options.MessagesPerSecond,
            options.Destination?.DisplayName ?? "their sources",
            items.Select(ScheduledResendItem.From).ToArray())
        { ConfigurationIdentity = ScheduledResend.IdentityFor(profile) };
        try
        {
            _scheduledStore?.Add(resend);
        }
        finally
        {
            // Also when saving failed after the old list was found damaged and set aside.
            ReportSetAsideSchedules();
        }
        ScheduledResends.Add(CreateScheduledItem(resend));
        UpdateScheduledStatuses();
        StatusText = $"Scheduled for {sendAt.ToLocalTime():ddd HH:mm}: {ScheduledResends[^1].Title}. It is listed on Activity.";
        AddActivity("Info", "Resend scheduled", $"{profile.Name} · {ScheduledResends[^1].Title} · {sendAt.ToLocalTime():g}", options.Destination);
    }

    private async Task RunScheduledAsync(ScheduledResendItemViewModel item, CancellationToken cancellationToken)
    {
        if (!ScheduledResends.Contains(item)) return;
        var resend = item.Resend;
        if (ConnectedProfileId != resend.ProfileId)
        {
            throw new InvalidOperationException($"Connect {resend.EnvironmentName} first; the resend belongs to it.");
        }
        if (!CanWrite)
        {
            throw new InvalidOperationException($"Unlock write access to {resend.EnvironmentName} first.");
        }
        if (_connectedProfile is null || resend.ConfigurationIdentity != ScheduledResend.IdentityFor(_connectedProfile))
        {
            throw new InvalidOperationException("The environment configuration changed or this is a legacy schedule. Cancel it and schedule again after reviewing the destination.");
        }

        DeadLetterResender.EnsureSafeMessageIds(_connectedProfile.Provider,
            resend.Items.Select(entry => entry.ToResendItem()).ToArray(), resend.Mode);
        RecordOperationIntent(resend.Mode == ResendMode.Move ? "Scheduled move started" : "Scheduled resend started",
            $"{resend.Items.Count:N0} messages · {resend.DestinationDisplay}", null);
        var preparedPlan = _replayStore is null ? null : await _replayStore.CreateResendAsync(resend.ProfileId,
            resend.Items.Select(entry => entry.ToResendItem()).ToArray(), resend.Mode, resend.MessagesPerSecond,
            _connectedProfile.EndpointDisplay, resend.ConfigurationIdentity!, "Scheduled resend", cancellationToken, deferActivation: true,
            provider: _connectedProfile.Provider);
        // Taken off the list before sending, so a crash in the middle never sends the same messages twice. The saved
        // environment is checked and the job claimed while environment changes are held: an environment deleted or
        // changed in another window (whose deletion cancels its saved jobs under the same hold) is never sent to from
        // this window's cached connection.
        var claimed = false;
        var environmentGone = false;
        {
            var coordinator = _profileRepository as IProfileMutationCoordinator;
            await using var mutation = coordinator is null ? null
                : await coordinator.AcquireProfileMutationAsync(cancellationToken).ConfigureAwait(true);
            var current = await _profileRepository.GetAsync(resend.ProfileId, cancellationToken).ConfigureAwait(true);
            environmentGone = current is null || ScheduledResend.IdentityFor(current) != resend.ConfigurationIdentity;
            claimed = _scheduledStore is null || RemoveScheduled(resend);
        }
        if (!claimed || environmentGone)
        {
            ScheduledResends.Remove(item);
            RefreshOperationHistory();
            StatusText = environmentGone
                ? $"{resend.EnvironmentName} was removed or changed in another window; the scheduled resend was cancelled and nothing was sent."
                : "The scheduled resend was cancelled, started or changed in another window; nothing was sent.";
            if (claimed)
            {
                AddActivity("Warning", "Scheduled resend cancelled", $"{resend.EnvironmentName} · {item.Title} · its environment was removed or changed; nothing was sent");
            }
            return;
        }
        ScheduledResends.Remove(item);
        if (preparedPlan is not null)
        {
            try { await _replayStore!.ActivateScheduledAsync(preparedPlan, cancellationToken); }
            finally { RefreshOperationHistory(); }
        }
        var progressFinished = false;
        var progress = new Progress<ResendProgress>(update =>
        {
            if (progressFinished) return;
            StatusText = $"Scheduled resend: processed {update.Processed:N0} of {update.Total:N0}";
        });
        ResendResult result;
        try
        {
            result = await RunDurableResendAsync(resend.Items.Select(entry => entry.ToResendItem()).ToArray(),
                resend.Mode, resend.MessagesPerSecond, "Scheduled resend", progress, cancellationToken, preparedPlan).ConfigureAwait(true);
        }
        finally { progressFinished = true; }
        RemoveResentOriginals(result);

        var summary = $"{result.SentCount:N0} of {resend.Items.Count:N0} sent" +
                      (resend.Mode == ResendMode.Move ? $" · {result.MovedCount:N0} originals removed" : string.Empty) +
                      (result.OriginalsKeptCount > 0 ? $" · {result.OriginalsKeptCount:N0} originals kept" : string.Empty) +
                      (result.FailedCount > 0 ? $" · {result.FailedCount:N0} failed or uncertain (review history)" : string.Empty) +
                      (result.CancelledCount > 0 ? $" · {result.CancelledCount:N0} not sent (cancelled)" : string.Empty);
        StatusText = $"Scheduled resend: {summary}";
        AddActivity(result.FailedCount == 0 && result.OriginalsKeptCount == 0 && result.CancelledCount == 0 ? "Success" : "Warning",
            "Scheduled resend finished", $"{resend.EnvironmentName} · {item.Title} · {summary}");
        _notifications?.Show("QueueLoom", $"Scheduled resend finished: {summary}");
    }

    private void CancelScheduled(ScheduledResendItemViewModel? item)
    {
        if (item is null || !ScheduledResends.Contains(item))
        {
            return;
        }
        try
        {
            if (_scheduledStore is not null && !RemoveScheduled(item.Resend))
            {
                ScheduledResends.Remove(item);
                StatusText = "The scheduled resend is no longer pending or changed in another window.";
                return;
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            return;
        }
        ScheduledResends.Remove(item);
        StatusText = "Scheduled resend cancelled; nothing was sent";
        AddActivity("Info", "Scheduled resend cancelled", $"{item.Resend.EnvironmentName} · {item.Title}");
    }

    private void UpdateScheduledStatuses()
    {
        var now = Clock.GetUtcNow();
        foreach (var item in ScheduledResends)
        {
            var resend = item.Resend;
            item.Status = resend.ConfigurationIdentity is null ||
                          _connectedProfile?.Id == resend.ProfileId && resend.ConfigurationIdentity != ScheduledResend.IdentityFor(_connectedProfile)
                ? "Configuration changed or legacy schedule: cancel and schedule again"
                : !resend.IsDue(now)
                ? $"Waits {Until(resend.DueAt - now)}"
                : ConnectedProfileId != resend.ProfileId
                    ? $"Due: connect {resend.EnvironmentName} to send"
                    : !CanWrite
                        ? $"Due: unlock write access to {resend.EnvironmentName} to send"
                        : "Due: sending when the current operation ends";
        }
    }

    private static string Until(TimeSpan span) => span.TotalMinutes < 60
        ? $"{Math.Max(1, (int)Math.Ceiling(span.TotalMinutes))} min"
        : span.TotalHours < 48 ? $"{(int)span.TotalHours} h {span.Minutes} min" : $"{(int)span.TotalDays} days";

    private ScheduledResendItemViewModel CreateScheduledItem(ScheduledResend resend) =>
        new(resend, RunScheduledResendCommand, CancelScheduledResendCommand);

    private bool RemoveScheduled(ScheduledResend resend)
    {
        try
        {
            return _scheduledStore?.TryRemove(resend) ?? true;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            ErrorText = $"Scheduled resends could not be saved: {exception.Message}";
            throw new IOException(ErrorText, exception);
        }
        finally
        {
            ReportSetAsideSchedules();
        }
    }

    private async Task StopScheduledResendsAsync()
    {
        _scheduleCancellation?.Cancel();
        if (_scheduleTask is not null)
        {
            await _scheduleTask.ConfigureAwait(true);
        }
        _scheduleCancellation?.Dispose();
        _scheduleCancellation = null;
        _scheduleTask = null;
    }
}
