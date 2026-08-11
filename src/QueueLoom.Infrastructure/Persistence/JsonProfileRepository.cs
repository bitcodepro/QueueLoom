using System.Text.Json;
using System.Text.Json.Serialization;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;

namespace QueueLoom.Infrastructure.Persistence;

public sealed class JsonProfileRepository : IProfileRepository, IDisposable
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly QueueLoomPaths _paths;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public JsonProfileRepository(QueueLoomPaths paths)
    {
        ArgumentNullException.ThrowIfNull(paths);
        _paths = paths;
        _paths.EnsureCreated();
    }

    public async Task<IReadOnlyList<ServiceBusProfile>> ListAsync(
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var storageLock = await CrossProcessFileLock.AcquireAsync(
                _paths.StorageLockFile,
                cancellationToken).ConfigureAwait(false);
            var document = await LoadAsync(cancellationToken).ConfigureAwait(false);
            return Array.AsReadOnly(document.Profiles
                .OrderBy(profile => profile.Environment)
                .ThenBy(profile => profile.Name, StringComparer.OrdinalIgnoreCase)
                .ToArray());
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<ServiceBusProfile?> GetAsync(
        Guid profileId,
        CancellationToken cancellationToken = default)
    {
        if (profileId == Guid.Empty)
        {
            return null;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var storageLock = await CrossProcessFileLock.AcquireAsync(
                _paths.StorageLockFile,
                cancellationToken).ConfigureAwait(false);
            var document = await LoadAsync(cancellationToken).ConfigureAwait(false);
            return document.Profiles.FirstOrDefault(profile => profile.Id == profileId);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task UpsertAsync(
        ServiceBusProfile profile,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        var validation = ProfileValidator.Validate(profile);
        if (!validation.IsValid)
        {
            throw new ArgumentException(
                string.Join(" ", validation.Errors.Select(error => error.Message)),
                nameof(profile));
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var storageLock = await CrossProcessFileLock.AcquireAsync(
                _paths.StorageLockFile,
                cancellationToken).ConfigureAwait(false);
            var document = await LoadAsync(cancellationToken).ConfigureAwait(false);
            var index = document.Profiles.FindIndex(existing => existing.Id == profile.Id);
            if (index >= 0)
            {
                document.Profiles[index] = profile;
            }
            else
            {
                document.Profiles.Add(profile);
            }

            await SaveAsync(document, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<bool> DeleteAsync(
        Guid profileId,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var storageLock = await CrossProcessFileLock.AcquireAsync(
                _paths.StorageLockFile,
                cancellationToken).ConfigureAwait(false);
            var document = await LoadAsync(cancellationToken).ConfigureAwait(false);
            var removed = document.Profiles.RemoveAll(profile => profile.Id == profileId) > 0;
            if (!removed)
            {
                return false;
            }

            if (document.SelectedProfileId == profileId)
            {
                document.SelectedProfileId = null;
            }

            await SaveAsync(document, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<Guid?> GetSelectedProfileIdAsync(
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var storageLock = await CrossProcessFileLock.AcquireAsync(
                _paths.StorageLockFile,
                cancellationToken).ConfigureAwait(false);
            return (await LoadAsync(cancellationToken).ConfigureAwait(false)).SelectedProfileId;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task SetSelectedProfileIdAsync(
        Guid? profileId,
        CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var storageLock = await CrossProcessFileLock.AcquireAsync(
                _paths.StorageLockFile,
                cancellationToken).ConfigureAwait(false);
            var document = await LoadAsync(cancellationToken).ConfigureAwait(false);
            if (profileId.HasValue && document.Profiles.All(profile => profile.Id != profileId.Value))
            {
                throw new ArgumentException("The selected profile does not exist.", nameof(profileId));
            }

            document.SelectedProfileId = profileId;
            await SaveAsync(document, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<ProfileDocument> LoadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_paths.ProfilesFile))
        {
            return new ProfileDocument();
        }

        try
        {
            await using var stream = new FileStream(
                _paths.ProfilesFile,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 16 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            var document = await JsonSerializer.DeserializeAsync<ProfileDocument>(
                    stream,
                    SerializerOptions,
                    cancellationToken)
                .ConfigureAwait(false)
                ?? new ProfileDocument();

            if (document.SchemaVersion != 1 || document.Profiles is null)
            {
                throw new InvalidDataException(
                    "QueueLoom profile metadata has an unsupported or damaged structure.");
            }

            // Treat profile metadata as untrusted local input. A manually edited or
            // older file must never persistently enable writes for Production.
            for (var index = 0; index < document.Profiles.Count; index++)
            {
                var profile = document.Profiles[index];
                if (profile is null)
                {
                    throw new InvalidDataException("QueueLoom profile metadata contains an empty profile entry.");
                }
                if (profile.Environment == EnvironmentKind.Production &&
                    profile.AccessMode != ProfileAccessMode.ReadOnly)
                {
                    document.Profiles[index] = profile with { AccessMode = ProfileAccessMode.ReadOnly };
                }

                var validation = ProfileValidator.Validate(document.Profiles[index]);
                if (!validation.IsValid)
                {
                    throw new InvalidDataException(
                        "QueueLoom profile metadata contains an invalid environment: " +
                        string.Join(" ", validation.Errors.Select(error => error.Message)));
                }
            }

            if (document.Profiles.Select(profile => profile.Id).Distinct().Count() != document.Profiles.Count)
            {
                throw new InvalidDataException("QueueLoom profile metadata contains duplicate profile identifiers.");
            }
            if (document.SelectedProfileId is { } selectedProfileId &&
                document.Profiles.All(profile => profile.Id != selectedProfileId))
            {
                throw new InvalidDataException("QueueLoom profile metadata selects an environment that does not exist.");
            }

            return document;
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("QueueLoom profile metadata is damaged or has an unsupported format.", exception);
        }
    }

    private Task SaveAsync(ProfileDocument document, CancellationToken cancellationToken)
    {
        document.SchemaVersion = 1;
        var json = JsonSerializer.Serialize(document, SerializerOptions);
        return AtomicFile.WriteTextAsync(_paths.ProfilesFile, json, cancellationToken);
    }

    public void Dispose() => _gate.Dispose();

    private sealed class ProfileDocument
    {
        public int SchemaVersion { get; set; } = 1;

        public Guid? SelectedProfileId { get; set; }

        public List<ServiceBusProfile> Profiles { get; set; } = [];
    }
}
