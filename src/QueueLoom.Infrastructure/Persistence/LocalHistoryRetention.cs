using Microsoft.Extensions.Logging;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Persistence;

/// <param name="ActivityRecordsDeleted">Activity journal files removed.</param>
/// <param name="OperationsDeleted">Operation history folders removed.</param>
public sealed record RetentionResult(int ActivityRecordsDeleted, int OperationsDeleted);

/// <summary>
/// Keeps the Activity journal and the operation (resend and replay) history for <see cref="Period"/> and then deletes
/// them: at start-up, off the calling thread, and again on the first check of every new UTC day while the process runs.
/// Unfinished operations are never deleted (see <see cref="BatchReplayStore.DeleteExpired"/>). Housekeeping is best
/// effort: a failure is logged and retried on the next run, and never stops or slows the application.
/// </summary>
public sealed class LocalHistoryRetention : IDisposable
{
    /// <summary>How long the Activity journal and finished operations are kept. The single place to change it.</summary>
    public const int RetentionDays = 3;

    public static TimeSpan Period { get; } = TimeSpan.FromDays(RetentionDays);

    /// <summary>How often a running process checks whether a new day has started.</summary>
    public static TimeSpan CheckInterval { get; } = TimeSpan.FromHours(1);

    private const string ScheduledResendKind = "Scheduled resend";

    private readonly FileActivityJournal? _journal;
    private readonly BatchReplayStore? _operations;
    private readonly IScheduledResendStore? _schedules;
    private readonly TimeProvider _clock;
    private readonly ILogger? _logger;
    private readonly object _gate = new();
    private DateTime? _lastRunDay;
    private ITimer? _timer;
    private bool _disposed;
    private int _running;

    public LocalHistoryRetention(
        FileActivityJournal? journal,
        BatchReplayStore? operations,
        IScheduledResendStore? schedules = null,
        TimeProvider? clock = null,
        ILogger<LocalHistoryRetention>? logger = null)
    {
        _journal = journal;
        _operations = operations;
        _schedules = schedules;
        _clock = clock ?? TimeProvider.System;
        _logger = logger;
    }

    /// <summary>Runs once now on a worker thread and then checks every <see cref="CheckInterval"/> for a new day.</summary>
    /// <returns>The start-up run, for callers that want to wait for it; it never fails.</returns>
    public Task Start()
    {
        lock (_gate)
        {
            if (_disposed || _timer is not null) return Task.CompletedTask;
            _timer = _clock.CreateTimer(_ => RunIfNewDay(), null, CheckInterval, CheckInterval);
        }
        return Task.Run(RunIfNewDay);
    }

    /// <summary>Runs unless a run already happened on the current UTC day.</summary>
    /// <returns>True when a run took place.</returns>
    public bool RunIfNewDay()
    {
        lock (_gate)
        {
            if (_disposed || _lastRunDay == _clock.GetUtcNow().UtcDateTime.Date) return false;
        }
        return RunNow() is not null;
    }

    /// <summary>Deletes what is older than <see cref="Period"/> now. Never throws.</summary>
    /// <returns>What was deleted, or null when another run in this process is still busy.</returns>
    public RetentionResult? RunNow()
    {
        if (Interlocked.Exchange(ref _running, 1) == 1) return null;
        try
        {
            var now = _clock.GetUtcNow();
            lock (_gate) _lastRunDay = now.UtcDateTime.Date;
            var cutoff = now - Period;
            var activity = Attempt("Activity journal", () => _journal?.DeleteExpired(cutoff) ?? 0);
            var operations = Attempt("operation history", () => _operations?.DeleteExpired(cutoff, ScheduledReferences()) ?? 0);
            if (activity + operations > 0)
            {
                _logger?.LogInformation("Retention ({Days} days) removed {Activity} Activity records and {Operations} finished operations.",
                    RetentionDays, activity, operations);
            }
            return new RetentionResult(activity, operations);
        }
        finally
        {
            Volatile.Write(ref _running, 0);
        }
    }

    private int Attempt(string what, Func<int> delete)
    {
        try
        {
            return delete();
        }
        catch (Exception exception)
        {
            // Housekeeping runs on a worker or timer thread: nothing may escape and stop the process.
            _logger?.LogWarning("Retention of the {What} did not finish and is retried later: {Error}", what,
                SensitiveDataRedactor.SummarizeException(exception));
            return 0;
        }
    }

    /// <summary>
    /// A scheduled resend's snapshot is prepared just before the job is taken off the pending list. While a job of the
    /// same environment is still pending, its scheduled snapshots are kept (deliberately broad). If the pending list
    /// cannot be read, every scheduled snapshot is kept.
    /// </summary>
    private Func<ReplayPlan, bool>? ScheduledReferences()
    {
        if (_schedules is null) return null;
        HashSet<Guid> profiles;
        try
        {
            profiles = _schedules.Load().Select(job => job.ProfileId).ToHashSet();
        }
        catch (Exception exception)
        {
            _logger?.LogWarning("Scheduled resends could not be read; scheduled operation history is kept: {Error}",
                SensitiveDataRedactor.SummarizeException(exception));
            return IsScheduled;
        }
        return plan => IsScheduled(plan) && profiles.Contains(plan.ProfileId);
    }

    private static bool IsScheduled(ReplayPlan plan) => plan.RequiresScheduleActivation || plan.Kind == ScheduledResendKind;

    public void Dispose()
    {
        ITimer? timer;
        lock (_gate)
        {
            _disposed = true;
            timer = _timer;
            _timer = null;
        }
        timer?.Dispose();
    }
}
