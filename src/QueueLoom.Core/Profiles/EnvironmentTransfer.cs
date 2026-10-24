using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using QueueLoom.Core.Validation;

namespace QueueLoom.Core.Profiles;

/// <summary>What an import found: the environments to add, and why others were left out.</summary>
/// <param name="NeedSecrets">Imported environments that sign in with a password, key or connection string, which must be entered again.</param>
public sealed record EnvironmentImport(IReadOnlyList<ServiceBusProfile> Profiles, IReadOnlyList<string> Skipped, int NeedSecrets);

/// <summary>
/// Environments in a file to share with a colleague or move to another computer. Only the settings travel:
/// passwords, keys and connection strings stay in this computer's vault, and imported environments start
/// read-only.
/// </summary>
public static class EnvironmentTransfer
{
    public const string Format = "queueloom-environments";
    public const int Version = 1;
    public const int MaximumEnvironments = 500;

    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    private static readonly string[] ComputedProperties = ["canWrite", "endpointDisplay", "environmentDisplayName", "authenticationDisplayName"];

    public static string Export(IEnumerable<ServiceBusProfile> profiles)
    {
        ArgumentNullException.ThrowIfNull(profiles);
        var list = profiles as IReadOnlyCollection<ServiceBusProfile> ?? profiles.ToList();
        // Name the environments at fault (never the URL, which may hold the credentials), so the user knows which to edit.
        var unsafeRegistry = list.Where(profile => profile.Kafka?.SchemaRegistryUrl is { } registry &&
                (!Uri.TryCreate(registry, UriKind.Absolute, out var uri) ||
                 !string.IsNullOrEmpty(uri.UserInfo) || !string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment)))
            .Select(profile => $"\"{profile.Name}\"").ToList();
        if (unsafeRegistry.Count > 0)
        {
            const int shown = 5;
            var names = string.Join(", ", unsafeRegistry.Take(shown)) +
                (unsafeRegistry.Count > shown ? $" and {unsafeRegistry.Count - shown} more" : "");
            throw new InvalidOperationException(
                $"Remove credentials, query and fragment from the Schema Registry URL of {names} before exporting environments.");
        }
        var environments = new JsonArray();
        foreach (var profile in list)
        {
            var node = JsonSerializer.SerializeToNode(profile, Options)!.AsObject();
            // The id belongs to this computer (and its vault entries); write access and permission to delete
            // queues are local decisions.
            node.Remove("id");
            node.Remove("configurationRevision");
            node.Remove("accessMode");
            node.Remove("allowQueueManagement");
            // Computed from the settings, not settings themselves: written, they could contradict the importer's own
            // decisions (canWrite, while the access mode is left out on purpose).
            foreach (var computed in ComputedProperties) node.Remove(computed);
            (node["rabbitMq"] as JsonObject)?.Remove("managementUri");
            (node["kafka"] as JsonObject)?.Remove("effectiveDeadLetterSuffixes");
            environments.Add(node);
        }
        var document = new JsonObject
        {
            ["format"] = Format,
            ["version"] = Version,
            ["exportedAt"] = DateTimeOffset.UtcNow,
            ["note"] = "QueueLoom environments without passwords, keys or connection strings.",
            ["environments"] = environments
        };
        return document.ToJsonString(Options);
    }

    public static EnvironmentImport Import(string json, IReadOnlyCollection<ServiceBusProfile> existing)
    {
        ArgumentNullException.ThrowIfNull(existing);
        JsonNode? root;
        try
        {
            root = JsonNode.Parse(json);
        }
        catch (JsonException exception)
        {
            throw new InvalidOperationException($"The file is not a QueueLoom environments file: {exception.Message}", exception);
        }
        // Read without throwing: a value of the wrong JSON type is a malformed file, not the JSON library's own error.
        if (root is not JsonObject document ||
            document["format"] is not JsonValue format || !format.TryGetValue<string>(out var formatName) || formatName != Format ||
            document["environments"] is not JsonArray environments)
        {
            throw new InvalidOperationException("The file is not a QueueLoom environments file. Export one from Environments → Export.");
        }
        if (document["version"] is not JsonValue versionValue || !versionValue.TryGetValue<int>(out var version) || version < 1)
        {
            throw new InvalidOperationException(
                "The file has no valid version (a whole number from 1). Export the environments from QueueLoom again and import that file.");
        }
        if (version > Version)
        {
            throw new InvalidOperationException("The file was written by a newer QueueLoom. Update QueueLoom and import it again.");
        }
        if (version != Version)
        {
            throw new InvalidOperationException($"The file has version {version}, which this QueueLoom does not read. Export the environments again.");
        }
        if (environments.Count > MaximumEnvironments)
        {
            throw new InvalidOperationException($"The file lists more than {MaximumEnvironments} environments.");
        }

        var names = existing.Select(profile => profile.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var imported = new List<ServiceBusProfile>();
        var skipped = new List<string>();
        foreach (var node in environments.OfType<JsonObject>())
        {
            var copy = node.DeepClone().AsObject();
            copy["id"] = Guid.NewGuid();
            copy["accessMode"] = nameof(ProfileAccessMode.ReadOnly);
            ServiceBusProfile? profile;
            try
            {
                profile = copy.Deserialize<ServiceBusProfile>(Options);
            }
            catch (JsonException exception)
            {
                skipped.Add($"{node["name"]?.ToString() ?? "An environment"}: {exception.Message}");
                continue;
            }
            if (profile is null)
            {
                continue;
            }

            // JsonObject member names are case-sensitive but Web deserialization is not.
            // Normalize after deserialization so an alternate casing cannot reuse a local
            // vault identity or override the read-only import boundary.
            profile = profile with
            {
                Id = Guid.NewGuid(),
                AccessMode = ProfileAccessMode.ReadOnly,
                // Creating, changing and deleting queues is turned on per computer, never by a file from elsewhere.
                AllowQueueManagement = false,
                ConfigurationRevision = Guid.Empty,
                Name = UniqueName(profile.Name, names)
            };
            var validation = ProfileValidator.Validate(profile);
            if (!validation.IsValid)
            {
                skipped.Add($"{profile.Name}: {string.Join(" ", validation.Errors.Select(error => error.Message))}");
                continue;
            }
            names.Add(profile.Name);
            imported.Add(profile);
        }

        return new EnvironmentImport(imported, skipped,
            imported.Count(profile => profile.Authentication.Kind.UsesStoredSecret() || profile.Kafka?.SchemaRegistryUserName is not null));
    }

    /// <summary>"Staging", or "Staging (2)" when that name is taken.</summary>
    private static string UniqueName(string name, IReadOnlySet<string> taken)
    {
        var trimmed = string.IsNullOrWhiteSpace(name) ? "Imported environment" : name.Trim();
        if (trimmed.Length > ProfileValidator.MaxNameLength || !taken.Contains(trimmed))
        {
            return trimmed;
        }
        for (var number = 2; ; number++)
        {
            var suffix = $" ({number})";
            var candidate = TextLimits.Head(trimmed, ProfileValidator.MaxNameLength - suffix.Length) + suffix;
            if (!taken.Contains(candidate))
            {
                return candidate;
            }
        }
    }
}
