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
    private string? _scheduledReadError;

    /// <summary>The clock for scheduled resends; tests replace it.</summary>
    public TimeProvider Clock { get; set; } = TimeProvider.System;

    public ObservableCollection<ScheduledResendItemViewModel> ScheduledResends { get; } = [];

    public bool HasScheduledResends => ScheduledResends.Count > 0;

    public RelayCommand<ScheduledResendItemViewModel> RunScheduledResendCommand { get; private set; } = null!;

    public RelayCommand<ScheduledResendItemViewModel> CancelScheduledResendCommand { get; private set; } = null!;

    /// <summary>The cancellation the command last started; it completes once the shared list is updated.</summary>
    internal Task PendingScheduledCancellation { get; private set; } = Task.CompletedTask;

    private void InitializeScheduledResends(IScheduledResendStore? store)
    {
        _scheduledStore = store;
        RunScheduledResendCommand = new RelayCommand<ScheduledResendItemViewModel>(
            item => _ = RunScheduledNowAsync(item!),
            item => item is not null && !IsBusy);
        CancelScheduledResendCommand = new RelayCommand<ScheduledResendItemViewModel>(
            item => PendingScheduledCancellation = CancelScheduledAsync(item), item => item is not null);
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
        ReportSetAside(aside, pending);
    }

    /// <summary>As <see cref="ReportSetAsideSchedules"/>, reading the list without holding the window's thread.</summary>
    private async Task ReportSetAsideSchedulesAsync()
    {
        if (_scheduledStore?.TakeSetAsideFile() is not { } aside)
        {
            return;
        }
        HashSet<Guid>? pending;
        try
        {
            pending = (await _scheduledStore.LoadAsync().ConfigureAwait(true)).Select(resend => resend.Id).ToHashSet();
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            pending = null;
        }
        ReportSetAside(aside, pending);
    }

    private void ReportSetAside(string aside, HashSet<Guid>? pending)
    {
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
        cancellationToken.ThrowIfCancellationRequested();
        if (_scheduledStore is not null)
        {
            IReadOnlyList<ScheduledResend> persisted;
            // Another window can hold the list for a while: wait for it without holding this window's thread.
            try { persisted = await _scheduledStore.LoadAsync(cancellationToken).ConfigureAwait(true); }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                var error = $"Scheduled resends could not be read: {exception.Message}";
                if (_scheduledReadError != error) { _scheduledReadError = error; ErrorText = error; }
                return; // Never execute a stale cache after a failed read.
            }
            _scheduledReadError = null;
            await ReportSetAsideSchedulesAsync().ConfigureAwait(true); // Report lost cached rows before reconciliation removes them.
            var currentIds = persisted.Select(resend => resend.Id).ToHashSet();
            foreach (var item in ScheduledResends.Where(item => !currentIds.Contains(item.Resend.Id)).ToArray())
                ScheduledResends.Remove(item);
            foreach (var resend in persisted)
            {
                var cached = ScheduledResends.FirstOrDefault(item => item.Resend.Id == resend.Id);
                if (cached is null) ScheduledResends.Add(CreateScheduledItem(resend));
                else if (!SameScheduledJob(cached.Resend, resend))
                    ScheduledResends[ScheduledResends.IndexOf(cached)] = CreateScheduledItem(resend);
            }
        }
        // A damaged list can be found by a read elsewhere (the background history cleanup); it is reported here at
        // the latest, again without holding the window's thread.
        await ReportSetAsideSchedulesAsync().ConfigureAwait(true);
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

    private static bool SameScheduledJob(ScheduledResend left, ScheduledResend right)
    {
        var empty = Array.Empty<ScheduledResendItem>();
        if (left with { Items = empty } != right with { Items = empty } || left.Items.Count != right.Items.Count) return false;
        for (var i = 0; i < left.Items.Count; i++)
        {
            var a = left.Items[i]; var b = right.Items[i];
            if (a with { Message = MessageDraft.Empty } != b with { Message = MessageDraft.Empty } ||
                a.Message.Body != b.Message.Body || a.Message.Properties != b.Message.Properties ||
                a.Message.LegacyAmqpMetadata != b.Message.LegacyAmqpMetadata ||
                !a.Message.ApplicationProperties.SequenceEqual(b.Message.ApplicationProperties) ||
                !SameKafkaEnvelope(a.Message.KafkaEnvelope, b.Message.KafkaEnvelope)) return false;
        }
        return true;
    }

    private static bool SameKafkaEnvelope(KafkaEnvelope? left, KafkaEnvelope? right)
    {
        if (ReferenceEquals(left, right)) return true;
        if (left is null || right is null || left.IsTombstone != right.IsTombstone ||
            left.OriginalProperties != right.OriginalProperties || !SameBytes(left.Key, right.Key) ||
            !left.OriginalApplicationProperties.SequenceEqual(right.OriginalApplicationProperties) ||
            left.Headers.Count != right.Headers.Count) return false;
        return left.Headers.Zip(right.Headers).All(pair => pair.First.Name == pair.Second.Name && SameBytes(pair.First.Value, pair.Second.Value));
    }

    private static bool SameBytes(byte[]? left, byte[]? right) =>
        left is null ? right is null : right is not null && left.AsSpan().SequenceEqual(right);

    /// <summary>"Run now" on Activity: the same checks as when it is due, just earlier.</summary>
    public Task RunScheduledNowAsync(ScheduledResendItemViewModel item) =>
        RunWorkspaceOperationAsync("Running a scheduled resend", ct => RunScheduledAsync(item, ct), CancellationToken.None, allowCancellation: true);

    private async Task ScheduleResendAsync(ServiceBusProfile profile, IReadOnlyList<ResendItem> items, ResendOptions options, DateTimeOffset sendAt,
        CancellationToken cancellationToken = default)
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
            // Cancelled while another window holds the list: nothing is saved, so nothing can be sent later.
            if (_scheduledStore is not null) await _scheduledStore.AddAsync(resend, cancellationToken).ConfigureAwait(true);
        }
        finally
        {
            // Also when saving failed after the old list was found damaged and set aside.
            await ReportSetAsideSchedulesAsync().ConfigureAwait(true);
        }
        // The background check may already have listed the saved job while this one waited: one row per job.
        var row = ScheduledResends.FirstOrDefault(item => item.Resend.Id == resend.Id);
        if (row is null)
        {
            row = CreateScheduledItem(resend);
            ScheduledResends.Add(row);
        }
        UpdateScheduledStatuses();
        StatusText = $"Scheduled for {sendAt.ToLocalTime():ddd HH:mm}: {row.Title}. It is listed on Activity.";
        AddActivity("Info", "Resend scheduled", $"{profile.Name} · {row.Title} · {sendAt.ToLocalTime():g}", options.Destination);
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
            // Cancelled while waiting for the list: the job stays pending, untouched.
            claimed = _scheduledStore is null || await RemoveScheduledAsync(resend, cancellationToken).ConfigureAwait(true);
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

    /// <summary>Cancels a scheduled resend; the shared list is updated without holding the window's thread.</summary>
    public async Task CancelScheduledAsync(ScheduledResendItemViewModel? item)
    {
        if (item is null || !ScheduledResends.Contains(item))
        {
            return;
        }
        try
        {
            if (_scheduledStore is not null && !await RemoveScheduledAsync(item.Resend).ConfigureAwait(true))
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

    private async Task<bool> RemoveScheduledAsync(ScheduledResend resend, CancellationToken cancellationToken = default)
    {
        try
        {
            return _scheduledStore is null || await _scheduledStore.TryRemoveAsync(resend, cancellationToken).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            ErrorText = $"Scheduled resends could not be saved: {exception.Message}";
            throw new IOException(ErrorText, exception);
        }
        finally
        {
            await ReportSetAsideSchedulesAsync().ConfigureAwait(true);
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
