using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

namespace QueueLoom.Infrastructure.Security;

internal sealed class MacOsKeychainMasterKeyStore : IPlatformMasterKeyStore
{
    private const string SecurityFramework = "/System/Library/Frameworks/Security.framework/Security";
    private const string CoreFoundationFramework = "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation";
    private const int Success = 0;
    private const int ItemNotFound = -25300;
    private const int DuplicateItem = -25299;
    private const int MasterKeySize = 32;
    private static readonly byte[] ServiceName = Encoding.UTF8.GetBytes("io.queueloom.master-key");

    public string BackendName => "macOS Keychain";

    public ValueTask<byte[]> GetOrCreateAsync(
        string installationId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!OperatingSystem.IsMacOS())
        {
            throw new PlatformNotSupportedException("macOS Keychain is only available on macOS.");
        }

        var account = Encoding.UTF8.GetBytes(installationId);
        var existing = Find(account);
        if (existing is not null)
        {
            return ValueTask.FromResult(existing);
        }

        var key = RandomNumberGenerator.GetBytes(MasterKeySize);
        var status = SecKeychainAddGenericPassword(
            IntPtr.Zero,
            (uint)ServiceName.Length,
            ServiceName,
            (uint)account.Length,
            account,
            (uint)key.Length,
            key,
            out var itemReference);

        if (itemReference != IntPtr.Zero)
        {
            CFRelease(itemReference);
        }

        if (status == DuplicateItem)
        {
            CryptographicOperations.ZeroMemory(key);
            return ValueTask.FromResult(
                Find(account) ?? throw new SecretVaultException("The existing QueueLoom Keychain item could not be read."));
        }

        if (status != Success)
        {
            CryptographicOperations.ZeroMemory(key);
            throw new SecureStoreUnavailableException($"macOS Keychain rejected the QueueLoom vault key (status {status}).");
        }

        return ValueTask.FromResult(key);
    }

    private static byte[]? Find(byte[] account)
    {
        var status = SecKeychainFindGenericPassword(
            IntPtr.Zero,
            (uint)ServiceName.Length,
            ServiceName,
            (uint)account.Length,
            account,
            out var passwordLength,
            out var passwordData,
            out var itemReference);

        try
        {
            if (status == ItemNotFound)
            {
                return null;
            }

            if (status != Success)
            {
                throw new SecureStoreUnavailableException($"macOS Keychain could not unlock QueueLoom (status {status}).");
            }

            if (passwordLength != MasterKeySize || passwordData == IntPtr.Zero)
            {
                throw new SecretVaultException("The QueueLoom Keychain vault key has an invalid format.");
            }

            var key = new byte[passwordLength];
            Marshal.Copy(passwordData, key, 0, key.Length);
            return key;
        }
        finally
        {
            if (passwordData != IntPtr.Zero)
            {
                SecKeychainItemFreeContent(IntPtr.Zero, passwordData);
            }

            if (itemReference != IntPtr.Zero)
            {
                CFRelease(itemReference);
            }
        }
    }

    [DllImport(SecurityFramework)]
    private static extern int SecKeychainFindGenericPassword(
        IntPtr keychainOrArray,
        uint serviceNameLength,
        byte[] serviceName,
        uint accountNameLength,
        byte[] accountName,
        out uint passwordLength,
        out IntPtr passwordData,
        out IntPtr itemReference);

    [DllImport(SecurityFramework)]
    private static extern int SecKeychainAddGenericPassword(
        IntPtr keychain,
        uint serviceNameLength,
        byte[] serviceName,
        uint accountNameLength,
        byte[] accountName,
        uint passwordLength,
        byte[] passwordData,
        out IntPtr itemReference);

    [DllImport(SecurityFramework)]
    private static extern int SecKeychainItemFreeContent(IntPtr attributeList, IntPtr data);

    [DllImport(CoreFoundationFramework)]
    private static extern void CFRelease(IntPtr value);
}
