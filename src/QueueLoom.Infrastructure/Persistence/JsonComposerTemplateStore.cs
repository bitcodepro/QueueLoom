using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Persistence;

/// <summary>
/// Composer templates, one JSON file each in the data folder's <c>templates</c> folder. One file per template means a
/// save from one window never overwrites another window's templates; the file name comes from the name, without its
/// case, so saving under an existing name replaces that template.
/// </summary>
public sealed class JsonComposerTemplateStore(string directory) : IComposerTemplateStore
{
    /// <summary>A template with a larger body is refused: templates are for messages typed by hand.</summary>
    public const int MaximumTemplateBytes = 1024 * 1024;

    public const int MaximumTemplates = 200;

    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public JsonComposerTemplateStore(QueueLoomPaths paths) : this(Path.Combine(paths.RootDirectory, "templates"))
    {
    }

    public async Task<IReadOnlyList<ComposerTemplate>> ListAsync(CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(directory))
        {
            return [];
        }
        var templates = new List<ComposerTemplate>();
        foreach (var file in Directory.EnumerateFiles(directory, "*.json"))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await TryReadAsync(file, cancellationToken).ConfigureAwait(false) is { } template)
            {
                templates.Add(template);
            }
        }
        return templates.OrderBy(template => template.Name, StringComparer.CurrentCultureIgnoreCase).ToArray();
    }

    public async Task SaveAsync(ComposerTemplate template, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(template);
        template = template with { Name = ComposerTemplate.NormalizeName(template.Name) };
        var bytes = JsonSerializer.SerializeToUtf8Bytes(template, Options);
        if (bytes.Length > MaximumTemplateBytes)
        {
            throw new InvalidOperationException(
                $"A template can hold at most {MaximumTemplateBytes / (1024 * 1024)} MiB. Shorten the body before saving it.");
        }
        Directory.CreateDirectory(directory);
        var path = PathFor(template.Name);
        if (!File.Exists(path) && Directory.EnumerateFiles(directory, "*.json").Count() >= MaximumTemplates)
        {
            throw new InvalidOperationException($"At most {MaximumTemplates} templates can be saved. Delete one first.");
        }
        await AtomicFile.WriteBytesAsync(path, bytes, cancellationToken).ConfigureAwait(false);
    }

    public Task DeleteAsync(string name, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        File.Delete(PathFor(ComposerTemplate.NormalizeName(name)));
        return Task.CompletedTask;
    }

    private string PathFor(string name) => Path.Combine(directory,
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name.ToUpperInvariant())))[..32] + ".json");

    /// <summary>A damaged, oversized or foreign file is skipped: one bad template must not hide the others.</summary>
    private async Task<ComposerTemplate?> TryReadAsync(string file, CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            if (stream.Length > MaximumTemplateBytes)
            {
                return null;
            }
            var template = await JsonSerializer.DeserializeAsync<ComposerTemplate>(stream, Options, cancellationToken).ConfigureAwait(false);
            if (template is null || template.Body is null || !Enum.IsDefined(template.BodyFormat))
            {
                return null;
            }
            var name = ComposerTemplate.NormalizeName(template.Name);
            // Only a file at the place its own name gives counts, so a copied file cannot shadow another template.
            return string.Equals(Path.GetFullPath(file), Path.GetFullPath(PathFor(name)), StringComparison.OrdinalIgnoreCase)
                ? template with { Name = name }
                : null;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or JsonException
                                              or InvalidOperationException or NotSupportedException)
        {
            return null;
        }
    }
}
