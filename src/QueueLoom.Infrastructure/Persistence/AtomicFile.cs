using System.Text;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;

namespace QueueLoom.Infrastructure.Persistence;

internal static class AtomicFile
{
    /// <summary>How long a replacement refused because a reader holds the file open is tried again.</summary>
    internal static readonly TimeSpan ReplaceBudget = TimeSpan.FromSeconds(2);

    /// <summary>Tests: a shorter budget for this async flow.</summary>
    internal static readonly AsyncLocal<TimeSpan?> ReplaceBudgetOverride = new();

    /// <summary>Tests: called in this async flow each time a refused replacement is about to be tried again.</summary>
    internal static readonly AsyncLocal<Action<string>?> ReplaceRetrying = new();

    /// <summary>
    /// Moves the written temporary file over <paramref name="path"/>. On Windows the move is refused while another
    /// thread or program reads the target (the operation history is read off the window's thread, and an antivirus or
    /// indexer opens files too); such a read is short, so the move is tried again within <see cref="ReplaceBudget"/>
    /// instead of failing the operation that writes the file.
    /// </summary>
    private static async Task ReplaceAsync(string temporaryPath, string path, CancellationToken cancellationToken)
    {
        var started = Environment.TickCount64;
        var budget = ReplaceBudgetOverride.Value ?? ReplaceBudget;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                File.Move(temporaryPath, path, overwrite: true);
                return;
            }
            catch (Exception exception) when (IsHeldOpen(exception) && Environment.TickCount64 - started < budget.TotalMilliseconds)
            {
                ReplaceRetrying.Value?.Invoke(path);
                await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(20 * attempt, 200)), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // ERROR_ACCESS_DENIED for a target open without delete sharing; ERROR_SHARING_VIOLATION or ERROR_LOCK_VIOLATION.
    private static bool IsHeldOpen(Exception exception) => OperatingSystem.IsWindows() &&
        (exception is UnauthorizedAccessException ||
         exception is IOException io and not FileNotFoundException and not DirectoryNotFoundException && (io.HResult & 0xFFFF) is 32 or 33);
    public static async Task WriteTextAsync(
        string path,
        string contents,
        CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(path)
            ?? throw new ArgumentException("The file must have a parent directory.", nameof(path));
        Directory.CreateDirectory(directory);

        var temporaryPath = Path.Combine(directory, $".{Path.GetFileName(path)}.{Guid.NewGuid():N}.tmp");
        try
        {
            await using (var stream = new FileStream(temporaryPath, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                64 * 1024, FileOptions.Asynchronous))
            {
                RestrictToCurrentUser(temporaryPath);
                await stream.WriteAsync(new UTF8Encoding(false).GetBytes(contents), cancellationToken).ConfigureAwait(false);
                stream.Flush(flushToDisk: true);
            }
            await ReplaceAsync(temporaryPath, path, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (File.Exists(temporaryPath))
            {
                File.Delete(temporaryPath);
            }
        }
    }

    public static async Task WriteBytesAsync(
        string path,
        ReadOnlyMemory<byte> contents,
        CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(path)
            ?? throw new ArgumentException("The file must have a parent directory.", nameof(path));
        Directory.CreateDirectory(directory);

        var temporaryPath = Path.Combine(directory, $".{Path.GetFileName(path)}.{Guid.NewGuid():N}.tmp");
        try
        {
            await File.WriteAllBytesAsync(temporaryPath, contents.ToArray(), cancellationToken)
                .ConfigureAwait(false);
            RestrictToCurrentUser(temporaryPath);
            await ReplaceAsync(temporaryPath, path, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (File.Exists(temporaryPath))
            {
                File.Delete(temporaryPath);
            }
        }
    }

    public static void RestrictToCurrentUser(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            RestrictFileOnWindows(path);
            return;
        }

        try
        {
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
        catch (PlatformNotSupportedException)
        {
        }
    }

    public static void RestrictDirectoryToCurrentUser(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            RestrictDirectoryOnWindows(path);
            return;
        }

        try
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
        catch (PlatformNotSupportedException)
        {
        }
    }

    [SupportedOSPlatform("windows")]
    private static void RestrictFileOnWindows(string path)
    {
        var currentUser = WindowsIdentity.GetCurrent().User
            ?? throw new InvalidOperationException("The current Windows user SID is unavailable.");
        var security = new FileSecurity();
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            currentUser,
            FileSystemRights.FullControl,
            AccessControlType.Allow));
        new FileInfo(path).SetAccessControl(security);
    }

    [SupportedOSPlatform("windows")]
    private static void RestrictDirectoryOnWindows(string path)
    {
        var currentUser = WindowsIdentity.GetCurrent().User
            ?? throw new InvalidOperationException("The current Windows user SID is unavailable.");
        var security = new DirectorySecurity();
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            currentUser,
            FileSystemRights.FullControl,
            InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
            PropagationFlags.None,
            AccessControlType.Allow));
        new DirectoryInfo(path).SetAccessControl(security);
    }
}
