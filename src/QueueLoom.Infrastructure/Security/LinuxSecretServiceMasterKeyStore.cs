using System.ComponentModel;
using System.Diagnostics;
using System.Security.Cryptography;

namespace QueueLoom.Infrastructure.Security;

internal sealed class LinuxSecretServiceMasterKeyStore : IPlatformMasterKeyStore
{
    private const int MasterKeySize = 32;

    public string BackendName => "Linux Secret Service";

    public async ValueTask<byte[]> GetOrCreateAsync(
        string installationId,
        CancellationToken cancellationToken = default)
    {
        if (!OperatingSystem.IsLinux())
        {
            throw new PlatformNotSupportedException("Secret Service is only available on Linux.");
        }

        var lookup = await RunSecretToolAsync(
            ["lookup", "application", "QueueLoom", "vault", installationId],
            standardInput: null,
            cancellationToken).ConfigureAwait(false);

        if (lookup.ExitCode == 0 && !string.IsNullOrWhiteSpace(lookup.Output))
        {
            try
            {
                var key = Convert.FromBase64String(lookup.Output.Trim());
                if (key.Length != MasterKeySize)
                {
                    CryptographicOperations.ZeroMemory(key);
                    throw new SecretVaultException("The QueueLoom Secret Service vault key has an invalid length.");
                }

                return key;
            }
            catch (FormatException exception)
            {
                throw new SecretVaultException("The QueueLoom Secret Service vault key is damaged.", exception);
            }
        }

        if (lookup.ExitCode is not (0 or 1))
        {
            throw new SecureStoreUnavailableException(
                $"Linux Secret Service lookup failed: {SanitizeError(lookup.Error)}");
        }

        var newKey = RandomNumberGenerator.GetBytes(MasterKeySize);
        var encoded = Convert.ToBase64String(newKey);
        var store = await RunSecretToolAsync(
            ["store", "--label=QueueLoom local vault key", "application", "QueueLoom", "vault", installationId],
            encoded,
            cancellationToken).ConfigureAwait(false);

        if (store.ExitCode != 0)
        {
            CryptographicOperations.ZeroMemory(newKey);
            throw new SecureStoreUnavailableException(
                $"Linux Secret Service could not store the QueueLoom vault key: {SanitizeError(store.Error)}");
        }

        return newKey;
    }

    private static async Task<ProcessResult> RunSecretToolAsync(
        IReadOnlyList<string> arguments,
        string? standardInput,
        CancellationToken cancellationToken)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = "secret-tool",
            UseShellExecute = false,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        try
        {
            using var process = new Process { StartInfo = startInfo };
            process.Start();

            if (standardInput is not null)
            {
                await process.StandardInput.WriteAsync(standardInput.AsMemory(), cancellationToken)
                    .ConfigureAwait(false);
            }
            process.StandardInput.Close();

            var outputTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
            var errorTask = process.StandardError.ReadToEndAsync(cancellationToken);
            await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
            return new ProcessResult(
                process.ExitCode,
                await outputTask.ConfigureAwait(false),
                await errorTask.ConfigureAwait(false));
        }
        catch (Win32Exception exception)
        {
            throw new SecureStoreUnavailableException(
                "Linux Secret Service requires the 'secret-tool' command and an unlocked desktop keyring. " +
                "Install libsecret-tools or use Microsoft Entra ID without a connection string.",
                exception);
        }
    }

    private static string SanitizeError(string value)
    {
        var sanitized = value.Replace('\r', ' ').Replace('\n', ' ').Trim();
        return string.IsNullOrEmpty(sanitized) ? "no diagnostic was provided" : sanitized;
    }

    private sealed record ProcessResult(int ExitCode, string Output, string Error);
}
