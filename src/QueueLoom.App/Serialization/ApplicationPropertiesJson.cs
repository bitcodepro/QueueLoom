using System.Text.Json;
using System.Text.Json.Serialization;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.Serialization;

internal static class ApplicationPropertiesJson
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public static string Serialize(IEnumerable<MessageApplicationProperty> properties)
    {
        var values = properties.ToDictionary(
            property => property.Name,
            property => new PropertyValue(property.Type, property.Value),
            StringComparer.Ordinal);
        return JsonSerializer.Serialize(values, Options);
    }

    public static IReadOnlyList<MessageApplicationProperty> Deserialize(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return [];
        }

        var values = JsonSerializer.Deserialize<Dictionary<string, PropertyValue>>(json, Options)
            ?? [];
        return values.Select(pair => new MessageApplicationProperty(
                pair.Key,
                pair.Value.Type,
                pair.Value.Value ?? string.Empty))
            .ToArray();
    }

    private sealed record PropertyValue(ApplicationPropertyType Type, string? Value);
}
