using System.Collections.Concurrent;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Kafka;

/// <summary>
/// Reads schemas by id from a Confluent-compatible Schema Registry (GET /schemas/ids/{id}). Schemas never change
/// once registered, so each id is fetched once per connection; an id the registry cannot give is not asked again.
/// </summary>
internal sealed class SchemaRegistryClient : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private readonly HttpClient _http;
    private readonly ConcurrentDictionary<int, MessageSchema?> _schemas = new();

    public SchemaRegistryClient(string url, string? userName, string? password, HttpMessageHandler? handler = null)
    {
        _http = handler is null ? new HttpClient() : new HttpClient(handler, disposeHandler: false);
        _http.BaseAddress = new Uri(url.TrimEnd('/') + "/");
        _http.Timeout = Timeout;
        _http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.schemaregistry.v1+json"));
        _http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        if (!string.IsNullOrEmpty(userName))
        {
            _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{userName}:{password}")));
        }
    }

    public async Task<MessageSchema?> GetAsync(int id, CancellationToken cancellationToken)
    {
        if (_schemas.TryGetValue(id, out var cached))
        {
            return cached;
        }

        MessageSchema? schema = null;
        try
        {
            using var response = await _http.GetAsync($"schemas/ids/{id.ToString(CultureInfo.InvariantCulture)}", cancellationToken)
                .ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
            {
                await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
                schema = Parse(id, document.RootElement);
            }
        }
        catch (Exception exception) when (exception is HttpRequestException or JsonException
                                              || exception is TaskCanceledException && !cancellationToken.IsCancellationRequested)
        {
            // The body is then shown without its schema; the registry being down must not stop reading messages.
        }

        _schemas[id] = schema;
        return schema;
    }

    internal static MessageSchema? Parse(int id, JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("schema", out var text) || text.ValueKind != JsonValueKind.String)
        {
            return null;
        }
        var type = root.TryGetProperty("schemaType", out var kind) && kind.ValueKind == JsonValueKind.String
            ? kind.GetString()?.ToUpperInvariant() switch
            {
                "PROTOBUF" => MessageSchemaType.Protobuf,
                "JSON" => MessageSchemaType.Json,
                _ => MessageSchemaType.Avro
            }
            : MessageSchemaType.Avro;
        return new MessageSchema(id, type, text.GetString()!);
    }

    public void Dispose() => _http.Dispose();
}
