using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Persistence;

public sealed partial class BatchReplayStore
{
    /// <summary>
    /// Deletes operations that are finished and whose last activity (the newest file written in the folder, such as a
    /// state change) is older than <paramref name="cutoff"/>, and unpublished folders (no plan.json) that old.
    /// An operation that can still matter is never deleted: see <see cref="IsFinishedState"/>. A damaged plan, a folder
    /// in use (its lock is held), a linked folder or a folder with unexpected subfolders is kept. Best effort: whatever
    /// cannot be checked or deleted now is left for the next run.
    /// </summary>
    /// <param name="cutoff">Activity at or after this instant keeps the operation.</param>
    /// <param name="isStillNeeded">True for a plan something else still refers to, such as a pending scheduled resend.</param>
    /// <returns>The number of operation folders deleted.</returns>
    public int DeleteExpired(DateTimeOffset cutoff, Func<ReplayPlan, bool>? isStillNeeded = null)
    {
        if (!Directory.Exists(RootDirectory)) return 0;
        var deleted = 0;
        foreach (var folder in Directory.GetDirectories(RootDirectory))
        {
            try
            {
                if (TryDeleteExpired(folder, cutoff, isStillNeeded)) deleted++;
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException) { }
        }
        return deleted;
    }

    /// <summary>
    /// The only item states after which nothing more can happen to an item and nobody needs to look at it:
    /// a copy that was confirmed sent, or a move whose original was then removed (Moved) or reported as not removed
    /// by the provider (SentOriginalKept: the send is confirmed and the original is still in the broker).
    /// Everything else keeps the operation: Pending (Continue), AwaitingScheduleClaim (blocked scheduled snapshot),
    /// Rejected (Retry proven failures), Sending/Uncertain (delivery unknown), Deleting/DeleteUncertain (source
    /// deletion unknown), Sent in a move (the copy is out but the original was never removed), Corrupt and anything
    /// unknown.
    /// </summary>
    internal static bool IsFinishedState(ResendMode mode, string state) => mode switch
    {
        ResendMode.Copy => state == "Sent",
        // OriginalKept is a proven untouched original. The older SentOriginalKept was also written for a failed (possibly
        // accepted) settlement, so it is kept like DeleteUncertain.
        ResendMode.Move => state is "Moved" or "OriginalKept",
        _ => false
    };

    private static bool TryDeleteExpired(string folder, DateTimeOffset cutoff, Func<ReplayPlan, bool>? isStillNeeded)
    {
        // Only folders this store creates; anything else in the directory is not ours to judge.
        if (!Guid.TryParseExact(Path.GetFileName(folder), "N", out var id)) return false;
        var directory = new DirectoryInfo(folder);
        if (directory.Attributes.HasFlag(FileAttributes.ReparsePoint) || directory.EnumerateDirectories().Any()) return false;
        if (LastActivity(directory) >= cutoff) return false;
        var planPath = Path.Combine(folder, "plan.json");
        using (var ownership = CrossProcessFileLock.TryAcquire(Path.Combine(folder, ".lock")))
        {
            // Held by a run, a recovery or a scheduled activation in this or another process: it is in use.
            if (ownership is null) return false;
            // Decide again under the lock: a state written meanwhile is new activity.
            if (LastActivity(directory) >= cutoff || directory.EnumerateDirectories().Any()) return false;
            if (File.Exists(planPath))
            {
                if (!IsFinished(folder, id, planPath, isStillNeeded)) return false;
                // Unpublish first, under the lock. A run or recovery reads plan.json under this lock, so none can start
                // afterwards; and if the rest of the delete fails, the leftovers are an unpublished folder that is never
                // listed (never shown with missing state files read as Pending) and is removed by a later run.
                File.Delete(planPath);
            }
            foreach (var file in directory.GetFiles())
            {
                if (file.Name != ".lock") file.Delete();
            }
        }
        // The lock file can only go once released; someone may just have taken it, which keeps the empty folder.
        try
        {
            File.Delete(Path.Combine(folder, ".lock"));
            Directory.Delete(folder);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException) { }
        return true;
    }

    private static bool IsFinished(string folder, Guid id, string planPath, Func<ReplayPlan, bool>? isStillNeeded)
    {
        ReplayPlan? plan;
        try
        {
            plan = JsonSerializer.Deserialize<ReplayPlan>(File.ReadAllText(planPath));
        }
        // A damaged plan cannot prove its items are finished: keep it for inspection, as List does.
        catch (Exception exception) when (exception is JsonException or ArgumentException or NotSupportedException)
        {
            return false;
        }
        if (plan is null || plan.Id != id || plan.Count is < 1 or > 1000) return false;
        if (isStillNeeded?.Invoke(plan) == true) return false;
        for (var index = 0; index < plan.Count; index++)
        {
            // A blocked scheduled snapshot shows Pending as AwaitingScheduleClaim; neither is finished.
            if (!IsFinishedState(plan.Mode, ReadState(folder, index))) return false;
        }
        return true;
    }

    /// <summary>When the operation last changed: the newest file in its folder. The lock file does not count, since
    /// opening it (as retention itself does) is not activity; an empty folder uses its own time.</summary>
    private static DateTimeOffset LastActivity(DirectoryInfo directory)
    {
        var newest = DateTime.MinValue;
        var any = false;
        foreach (var file in directory.EnumerateFiles())
        {
            if (file.Name == ".lock") continue;
            any = true;
            if (file.LastWriteTimeUtc > newest) newest = file.LastWriteTimeUtc;
        }
        if (!any)
        {
            directory.Refresh();
            newest = directory.LastWriteTimeUtc;
        }
        return new DateTimeOffset(newest, TimeSpan.Zero);
    }
}
