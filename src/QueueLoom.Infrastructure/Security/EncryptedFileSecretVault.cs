using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using QueueLoom.Core.Abstractions;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Security;

public sealed class EncryptedFileSecretVault : ISecretVault, IDisposable
{
    private const int NonceSize = 12;
    private const int TagSize = 16;
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly QueueLoomPaths _paths;
    private readonly InstallationIdentityProvider _identityProvider;
    private readonly IPlatformMasterKeyStore _masterKeyStore;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public EncryptedFileSecretVault(QueueLoomPaths paths)
        : this(paths, PlatformMasterKeyStore.Create(paths))
    {
    }

    internal EncryptedFileSecretVault(QueueLoomPaths paths, IPlatformMasterKeyStore masterKeyStore)
    {
        ArgumentNullException.ThrowIfNull(paths);
        ArgumentNullException.ThrowIfNull(masterKeyStore);
        _paths = paths;
        _identityProvider = new InstallationIdentityProvider(paths);
        _masterKeyStore = masterKeyStore;
        _paths.EnsureCreated();
    }

    public string BackendName => _masterKeyStore.BackendName;

    public ValueTask StoreAsync(
        ProfileSecretKey key,
        string secret,
        CancellationToken cancellationToken = default) =>
        new(StoreCoreAsync(key, secret, cancellationToken));

    public async ValueTask<string?> RetrieveAsync(
        ProfileSecretKey key,
        CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var storageLock = await CrossProcessFileLock.AcquireAsync(
                _paths.StorageLockFile,
                cancellationToken).ConfigureAwait(false);
            var document = await LoadAsync(cancellationToken).ConfigureAwait(false);
            if (!document.Entries.TryGetValue(GetEntryName(key), out var entry))
            {
                return null;
            }

            var installationId = await _identityProvider.GetOrCreateAsync(cancellationToken).ConfigureAwait(false);
            var masterKey = await _masterKeyStore.GetOrCreateAsync(installationId, cancellationToken)
                .ConfigureAwait(false);
            try
            {
                var nonce = Convert.FromBase64String(entry.Nonce);
                var ciphertext = Convert.FromBase64String(entry.Ciphertext);
                var tag = Convert.FromBase64String(entry.Tag);
                var plaintext = new byte[ciphertext.Length];
                try
                {
                    using var aes = new AesGcm(masterKey, TagSize);
                    aes.Decrypt(nonce, ciphertext, tag, plaintext, GetAdditionalData(installationId, key));
                    return Encoding.UTF8.GetString(plaintext);
                }
                catch (CryptographicException exception)
                {
                    throw new SecretVaultException(
                        "The stored connection string cannot be decrypted on this user account and computer.",
                        exception);
                }
                finally
                {
                    CryptographicOperations.ZeroMemory(nonce);
                    CryptographicOperations.ZeroMemory(ciphertext);
                    CryptographicOperations.ZeroMemory(tag);
                    CryptographicOperations.ZeroMemory(plaintext);
                }
            }
            catch (FormatException exception)
            {
                throw new SecretVaultException("The encrypted QueueLoom secret record is damaged.", exception);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(masterKey);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<bool> ExistsAsync(
        ProfileSecretKey key,
        CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var storageLock = await CrossProcessFileLock.AcquireAsync(
                _paths.StorageLockFile,
                cancellationToken).ConfigureAwait(false);
            return (await LoadAsync(cancellationToken).ConfigureAwait(false))
                .Entries.ContainsKey(GetEntryName(key));
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<bool> RemoveAsync(
        ProfileSecretKey key,
        CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var storageLock = await CrossProcessFileLock.AcquireAsync(
                _paths.StorageLockFile,
                cancellationToken).ConfigureAwait(false);
            var document = await LoadAsync(cancellationToken).ConfigureAwait(false);
            if (!document.Entries.Remove(GetEntryName(key)))
            {
                return false;
            }

            await SaveAsync(document, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task StoreCoreAsync(
        ProfileSecretKey key,
        string secret,
        CancellationToken cancellationToken)
    {
        ValidateKey(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(secret);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var storageLock = await CrossProcessFileLock.AcquireAsync(
                _paths.StorageLockFile,
                cancellationToken).ConfigureAwait(false);
            var installationId = await _identityProvider.GetOrCreateAsync(cancellationToken).ConfigureAwait(false);
            var masterKey = await _masterKeyStore.GetOrCreateAsync(installationId, cancellationToken)
                .ConfigureAwait(false);
            var plaintext = Encoding.UTF8.GetBytes(secret);
            var nonce = RandomNumberGenerator.GetBytes(NonceSize);
            var ciphertext = new byte[plaintext.Length];
            var tag = new byte[TagSize];
            try
            {
                using var aes = new AesGcm(masterKey, TagSize);
                aes.Encrypt(nonce, plaintext, ciphertext, tag, GetAdditionalData(installationId, key));

                var document = await LoadAsync(cancellationToken).ConfigureAwait(false);
                document.Entries[GetEntryName(key)] = new SecretEntry
                {
                    Version = 1,
                    Nonce = Convert.ToBase64String(nonce),
                    Ciphertext = Convert.ToBase64String(ciphertext),
                    Tag = Convert.ToBase64String(tag)
                };
                await SaveAsync(document, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(masterKey);
                CryptographicOperations.ZeroMemory(plaintext);
                CryptographicOperations.ZeroMemory(nonce);
                CryptographicOperations.ZeroMemory(ciphertext);
                CryptographicOperations.ZeroMemory(tag);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<SecretDocument> LoadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_paths.SecretsFile))
        {
            return new SecretDocument();
        }

        try
        {
            await using var stream = new FileStream(
                _paths.SecretsFile,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                16 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            var document = await JsonSerializer.DeserializeAsync<SecretDocument>(
                    stream,
                    SerializerOptions,
                    cancellationToken)
                .ConfigureAwait(false)
                ?? new SecretDocument();
            ValidateDocument(document);
            return document;
        }
        catch (JsonException exception)
        {
            throw new SecretVaultException("The encrypted QueueLoom secret store is damaged.", exception);
        }
    }

    private Task SaveAsync(SecretDocument document, CancellationToken cancellationToken)
    {
        document.SchemaVersion = 1;
        var json = JsonSerializer.Serialize(document, SerializerOptions);
        return AtomicFile.WriteTextAsync(_paths.SecretsFile, json, cancellationToken);
    }

    private static byte[] GetAdditionalData(string installationId, ProfileSecretKey key) =>
        Encoding.UTF8.GetBytes($"QueueLoom|secret-v1|{installationId}|{key.ProfileId:N}|{key.Kind}");

    private static string GetEntryName(ProfileSecretKey key) => $"{key.ProfileId:N}:{key.Kind}";

    private static void ValidateDocument(SecretDocument document)
    {
        if (document.SchemaVersion != 1 || document.Entries is null)
        {
            throw new SecretVaultException(
                "The encrypted QueueLoom secret store has an unsupported or damaged structure.");
        }

        foreach (var (name, entry) in document.Entries)
        {
            if (string.IsNullOrWhiteSpace(name) || entry is null || entry.Version != 1 ||
                string.IsNullOrWhiteSpace(entry.Nonce) ||
                string.IsNullOrWhiteSpace(entry.Ciphertext) ||
                string.IsNullOrWhiteSpace(entry.Tag))
            {
                throw new SecretVaultException("The encrypted QueueLoom secret store contains a damaged entry.");
            }

            byte[]? nonce = null;
            byte[]? ciphertext = null;
            byte[]? tag = null;
            try
            {
                nonce = Convert.FromBase64String(entry.Nonce);
                ciphertext = Convert.FromBase64String(entry.Ciphertext);
                tag = Convert.FromBase64String(entry.Tag);
                if (nonce.Length != NonceSize || ciphertext.Length == 0 || tag.Length != TagSize)
                {
                    throw new SecretVaultException("The encrypted QueueLoom secret store contains a damaged entry.");
                }
            }
            catch (FormatException exception)
            {
                throw new SecretVaultException(
                    "The encrypted QueueLoom secret store contains an invalid Base64 entry.",
                    exception);
            }
            finally
            {
                if (nonce is not null)
                {
                    CryptographicOperations.ZeroMemory(nonce);
                }
                if (ciphertext is not null)
                {
                    CryptographicOperations.ZeroMemory(ciphertext);
                }
                if (tag is not null)
                {
                    CryptographicOperations.ZeroMemory(tag);
                }
            }
        }
    }

    private static void ValidateKey(ProfileSecretKey key)
    {
        if (key.ProfileId == Guid.Empty)
        {
            throw new ArgumentException("A secret must belong to a profile.", nameof(key));
        }

        if (!Enum.IsDefined(key.Kind))
        {
            throw new ArgumentException("The secret kind is not supported.", nameof(key));
        }
    }

    public void Dispose() => _gate.Dispose();

    private sealed class SecretDocument
    {
        public int SchemaVersion { get; set; } = 1;

        public Dictionary<string, SecretEntry> Entries { get; set; } = new(StringComparer.Ordinal);
    }

    private sealed class SecretEntry
    {
        public int Version { get; set; }

        public string Nonce { get; set; } = string.Empty;

        public string Ciphertext { get; set; } = string.Empty;

        public string Tag { get; set; } = string.Empty;
    }
}
