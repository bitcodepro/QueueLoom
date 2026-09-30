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

    public static string Export(IEnumerable<ServiceBusProfile> profiles)
    {
        ArgumentNullException.ThrowIfNull(profiles);
        var environments = new JsonArray();
        foreach (var profile in profiles)
        {
            var node = JsonSerializer.SerializeToNode(profile, Options)!.AsObject();
            // The id belongs to this computer (and its vault entries); write access is a local decision.
            node.Remove("id");
            node.Remove("accessMode");
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
        if (root?["format"]?.GetValue<string>() != Format || root["environments"] is not JsonArray environments)
        {
            throw new InvalidOperationException("The file is not a QueueLoom environments file. Export one from Environments → Export.");
        }
        if (root["version"]?.GetValue<int>() is not Version)
        {
            throw new InvalidOperationException("The file was written by a newer QueueLoom. Update QueueLoom and import it again.");
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

            profile = profile with { Name = UniqueName(profile.Name, names) };
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
        if (!taken.Contains(trimmed))
        {
            return trimmed;
        }
        for (var number = 2; ; number++)
        {
            var candidate = $"{trimmed} ({number})";
            if (!taken.Contains(candidate))
            {
                return candidate;
            }
        }
    }
}
