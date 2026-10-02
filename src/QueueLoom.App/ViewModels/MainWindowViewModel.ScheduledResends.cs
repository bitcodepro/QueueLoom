using System.Collections.ObjectModel;
using QueueLoom.App.Commands;
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
        UpdateScheduledStatuses();
    }

    /// <summary>Starts checking for due resends; called once the window is up.</summary>
    public void StartScheduledResends()
    {
        if (_scheduleTask is not null)
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
        _scheduledStore?.Add(resend);
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
        // Taken off the list before sending, so a crash in the middle never sends the same messages twice.
        if (_scheduledStore is not null && !RemoveScheduled(resend))
        {
            ScheduledResends.Remove(item);
            StatusText = "The scheduled resend was cancelled, started or changed in another window; nothing was sent.";
            return;
        }
        ScheduledResends.Remove(item);
        RecordOperationIntent(resend.Mode == ResendMode.Move ? "Scheduled move started" : "Scheduled resend started",
            $"{resend.Items.Count:N0} messages · {resend.DestinationDisplay}", null);
        var progress = new Progress<ResendProgress>(update =>
            StatusText = $"Scheduled resend: sent {update.Processed:N0} of {update.Total:N0}" +
                         (update.Failed > 0 ? $" · {update.Failed:N0} failed" : string.Empty));
        var result = await DeadLetterResender.ResendAsync(_workspace, resend.Items.Select(entry => entry.ToResendItem()).ToArray(),
            resend.Mode, resend.MessagesPerSecond, progress, cancellationToken).ConfigureAwait(true);
        RemoveResentOriginals(result);

        var summary = $"{result.SentCount:N0} of {resend.Items.Count:N0} sent" +
                      (resend.Mode == ResendMode.Move ? $" · {result.MovedCount:N0} originals removed" : string.Empty) +
                      (result.FailedCount > 0 ? $" · {result.FailedCount:N0} failed" : string.Empty) +
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
