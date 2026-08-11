using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Security;

internal static class PlatformMasterKeyStore
{
    public static IPlatformMasterKeyStore Create(QueueLoomPaths paths)
    {
        ArgumentNullException.ThrowIfNull(paths);

        if (OperatingSystem.IsWindows())
        {
            return new WindowsDpapiMasterKeyStore(paths);
        }

        if (OperatingSystem.IsMacOS())
        {
            return new MacOsKeychainMasterKeyStore();
        }

        if (OperatingSystem.IsLinux())
        {
            return new LinuxSecretServiceMasterKeyStore();
        }

        throw new PlatformNotSupportedException(
            "QueueLoom secure storage supports Windows DPAPI, macOS Keychain, and Linux Secret Service.");
    }
}
