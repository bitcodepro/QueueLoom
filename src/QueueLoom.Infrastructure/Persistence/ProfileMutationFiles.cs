namespace QueueLoom.Infrastructure.Persistence;

internal static class ProfileMutationFiles
{
    // Always acquired before .storage.lock. The vault and repository take their own
    // storage locks sequentially; this outer lock must never reuse .storage.lock.
    public static string LockPath(QueueLoomPaths paths) => Path.Combine(paths.RootDirectory, ".profile-mutations.lock");
    public static string PendingPath(QueueLoomPaths paths, Guid profileId) =>
        Path.Combine(paths.RootDirectory, $".profile-{profileId:N}.credential-pending");
}
