using System.Text.Json.Nodes;
using System.Text.Json;
using System.Text.Json.Serialization;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;

namespace QueueLoom.Infrastructure.Persistence;

public sealed class JsonProfileRepository : IAtomicProfileRepository, IProfileMutationCoordinator, IDisposable
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

    public async ValueTask<IAsyncDisposable> AcquireProfileMutationAsync(CancellationToken cancellationToken = default) =>
        await CrossProcessFileLock.AcquireAsync(ProfileMutationFiles.LockPath(_paths), cancellationToken).ConfigureAwait(false);

    public Task MarkCredentialUpdatePendingAsync(Guid profileId, CancellationToken cancellationToken = default) =>
        AtomicFile.WriteTextAsync(ProfileMutationFiles.PendingPath(_paths, profileId), "Credential update incomplete", cancellationToken);

    public void CompleteCredentialUpdate(Guid profileId) => File.Delete(ProfileMutationFiles.PendingPath(_paths, profileId));

    public bool IsCredentialUpdatePending(Guid profileId) => File.Exists(ProfileMutationFiles.PendingPath(_paths, profileId));

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

    public Task UpsertAsync(
        ServiceBusProfile profile,
        CancellationToken cancellationToken = default) =>
        UpsertCoreAsync(profile, select: false, cancellationToken);

    public Task UpsertAndSelectAsync(
        ServiceBusProfile profile,
        CancellationToken cancellationToken = default) =>
        UpsertCoreAsync(profile, select: true, cancellationToken);

    private async Task UpsertCoreAsync(
        ServiceBusProfile profile,
        bool select,
        CancellationToken cancellationToken)
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

            if (select) document.SelectedProfileId = profile.Id;

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
            using var buffer = new MemoryStream();
            await stream.CopyToAsync(buffer, cancellationToken).ConfigureAwait(false);
            // A UTF-8 byte order mark is accepted, as reading from the stream did; the span readers below would refuse it.
            var bytes = buffer.ToArray().AsMemory();
            if (bytes.Span.StartsWith((ReadOnlySpan<byte>)[0xEF, 0xBB, 0xBF]))
            {
                bytes = bytes[3..];
            }
            var document = JsonSerializer.Deserialize<ProfileDocument>(bytes.Span, SerializerOptions) ?? new ProfileDocument();
            document.ProfileExtras = ReadProfileExtras(bytes);

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

                var validation = ProfileValidator.ValidatePersistedProfile(document.Profiles[index]);
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
        var node = JsonSerializer.SerializeToNode(document, SerializerOptions)!;
        // Fields of an environment that this version does not know (written by a newer QueueLoom) are put back on it,
        // so editing any environment here does not strip them.
        if (document.ProfileExtras.Count > 0 && node["profiles"] is JsonArray profiles)
        {
            foreach (var profile in profiles.OfType<JsonObject>())
            {
                if (profile["id"]?.GetValue<Guid>() is { } id && document.ProfileExtras.TryGetValue(id, out var extras))
                {
                    foreach (var (name, value) in extras)
                    {
                        if (!profile.ContainsKey(name))
                        {
                            profile[name] = JsonNode.Parse(value.GetRawText());
                        }
                    }
                }
            }
        }
        return AtomicFile.WriteTextAsync(_paths.ProfilesFile, node.ToJsonString(SerializerOptions), cancellationToken);
    }

    /// <summary>The names this version writes for an environment; anything else on one is kept as an extra.</summary>
    private static readonly Lazy<HashSet<string>> KnownProfileFields = new(() =>
    {
        // The options get their default resolver (as serializing would give them) before their metadata is read.
        SerializerOptions.MakeReadOnly(populateMissingResolver: true);
        return new HashSet<string>(
            SerializerOptions.GetTypeInfo(typeof(ServiceBusProfile)).Properties.Select(property => property.Name),
            StringComparer.OrdinalIgnoreCase);
    });

    /// <summary>
    /// Matches the serializer: property names in any case ("id", "Id", "ID"), and a repeated property counts with its
    /// last occurrence, so a hand-edited file it accepts is read the same way here.
    /// </summary>
    private static Dictionary<Guid, Dictionary<string, JsonElement>> ReadProfileExtras(ReadOnlyMemory<byte> json)
    {
        var extras = new Dictionary<Guid, Dictionary<string, JsonElement>>();
        using var parsed = JsonDocument.Parse(json);
        if (parsed.RootElement.ValueKind != JsonValueKind.Object)
        {
            return extras;
        }
        foreach (var property in parsed.RootElement.EnumerateObject())
        {
            if (!string.Equals(property.Name, "profiles", StringComparison.OrdinalIgnoreCase) || property.Value.ValueKind != JsonValueKind.Array)
            {
                continue;
            }
            foreach (var profile in property.Value.EnumerateArray())
            {
                if (profile.ValueKind != JsonValueKind.Object ||
                    profile.EnumerateObject().LastOrDefault(field => string.Equals(field.Name, "id", StringComparison.OrdinalIgnoreCase))
                        is not { Value.ValueKind: JsonValueKind.String } id ||
                    !id.Value.TryGetGuid(out var profileId))
                {
                    continue;
                }
                var unknown = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
                foreach (var field in profile.EnumerateObject().Where(field => !KnownProfileFields.Value.Contains(field.Name)))
                {
                    // Cloned: the parsed document is disposed when this returns. A repeated field keeps its last value.
                    unknown[field.Name] = field.Value.Clone();
                }
                if (unknown.Count > 0)
                {
                    extras[profileId] = unknown;
                }
            }
        }
        return extras;
    }

    public void Dispose() => _gate.Dispose();

    private sealed class ProfileDocument
    {
        public int SchemaVersion { get; set; } = 1;

        public Guid? SelectedProfileId { get; set; }

        public List<ServiceBusProfile> Profiles { get; set; } = [];

        /// <summary>Fields this version does not know (for example from a newer QueueLoom), written back unchanged.</summary>
        [JsonExtensionData]
        public Dictionary<string, JsonElement>? AdditionalFields { get; set; }

        /// <summary>Per environment, the fields this version does not know; written back onto that environment.</summary>
        [JsonIgnore]
        public Dictionary<Guid, Dictionary<string, JsonElement>> ProfileExtras { get; set; } = [];
    }
}
