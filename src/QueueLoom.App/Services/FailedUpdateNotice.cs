using QueueLoom.Core.Updates;

namespace QueueLoom.App.Services;

/// <summary>
/// A release that failed to start and was rolled back by the stable launcher. The startup check shows it once,
/// with a manual way to install it anyway, and then stops offering it; a newer release is offered as usual.
/// </summary>
public static class FailedUpdateNotice
{
    /// <summary>The remembered failure of a stable installation, or null (none, or not a stable installation).</summary>
    public static FailedStartup? Read(UpdateTarget? target)
    {
        try
        {
            return target is not null && VersionInstallation.HasDescriptor(target.Executable)
                ? new VersionInstallation(target.Executable).ReadFailedStartup()
                : null;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or InvalidDataException)
        {
            return null;
        }
    }

    /// <summary>True when the offered release is the one that failed to start.</summary>
    public static bool IsFailedRelease(UpdateCheckResult update, FailedStartup? failure) =>
        failure?.FailedVersion is { } version && GitHubUpdateChecker.TryParseVersion(version, out var failed) &&
        failed == update.Version;

    /// <summary>Whether the startup check shows a dialog: always for another release, once for the failed one.</summary>
    public static bool ShouldShow(UpdateCheckResult update, FailedStartup? failure) =>
        !IsFailedRelease(update, failure) || !failure!.Noticed;

    /// <summary>Remembers that the rollback was shown. Never thrown: the hint must not interrupt the update check.</summary>
    public static void MarkNoticed(UpdateTarget? target, FailedStartup failure)
    {
        try
        {
            if (target is not null && VersionInstallation.HasDescriptor(target.Executable))
                new VersionInstallation(target.Executable).NoticeFailedStartup(failure);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or InvalidDataException)
        {
        }
    }
}
