using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Kafka;

/// <summary>
/// Reads schemas by id from a Confluent-compatible Schema Registry (GET /schemas/ids/{id}). Schemas never change
/// once registered, so each id is fetched once per connection; an id the registry does not have is not asked again.
/// A failure that can pass (5xx, 429, timeout) is not asked again for <see cref="FailureBackoff"/>, so an outage costs
/// one lookup per browse instead of one per record; an unreachable registry is skipped for every id meanwhile.
/// </summary>
internal sealed class SchemaRegistryClient : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private readonly HttpClient _http;
    internal static readonly TimeSpan FailureBackoff = TimeSpan.FromSeconds(30);
    private readonly ConcurrentDictionary<int, MessageSchema?> _schemas = new();
    private readonly ConcurrentDictionary<int, DateTimeOffset> _failedUntil = new();
    private readonly TimeProvider _time;
    private long _unreachableUntilTicks;

    public SchemaRegistryClient(string url, string? userName, string? password, HttpMessageHandler? handler = null,
        TimeProvider? time = null)
    {
        _time = time ?? TimeProvider.System;
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
        var now = _time.GetUtcNow();
        if (now.UtcTicks < Interlocked.Read(ref _unreachableUntilTicks)
            || _failedUntil.TryGetValue(id, out var until) && now < until)
        {
            return null;
        }

        MessageSchema? schema = null;
        var definitive = false;
        try
        {
            // Headers first, then at most MaximumResponseBytes of the body, read within the same time limit: a wrong URL
            // (a large download, an endless stream) is not read whole into memory.
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(Timeout);
            using var response = await _http.GetAsync($"schemas/ids/{id.ToString(CultureInfo.InvariantCulture)}",
                    HttpCompletionOption.ResponseHeadersRead, deadline.Token)
                .ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
            {
                await using var stream = await response.Content.ReadAsStreamAsync(deadline.Token).ConfigureAwait(false);
                var body = await ReadBoundedAsync(stream, deadline.Token).ConfigureAwait(false);
                if (body is null)
                {
                    // Not a schema: asked again only after the backoff, like a temporary failure.
                    _failedUntil[id] = now + FailureBackoff;
                }
                else
                {
                    using var document = JsonDocument.Parse(body);
                    schema = Parse(id, document.RootElement);
                    definitive = true;
                }
            }
            else
            {
                // Only a 404 is a lasting answer; 5xx, 401/403 and 429 can change, so they are asked again later.
                definitive = response.StatusCode == HttpStatusCode.NotFound;
                if (!definitive)
                {
                    _failedUntil[id] = now + FailureBackoff;
                }
            }
        }
        catch (JsonException)
        {
            _failedUntil[id] = now + FailureBackoff;
        }
        // The body is read as a stream (see above), so its failures arrive unwrapped: a connection dropped mid-body is
        // an IOException, and the deadline expiring during the read an OperationCanceledException. Both are the
        // registry failing, like an HttpRequestException; only the caller's own cancellation goes on.
        catch (Exception exception) when (exception is HttpRequestException or IOException
                                              || exception is OperationCanceledException && !cancellationToken.IsCancellationRequested)
        {
            Interlocked.Exchange(ref _unreachableUntilTicks, (now + FailureBackoff).UtcTicks);
            // The body is then shown without its schema; the registry being down must not stop reading messages.
        }

        if (definitive)
        {
            _failedUntil.TryRemove(id, out _);
            _schemas[id] = schema;
        }
        return schema;
    }

    /// <summary>A schema is a few kilobytes; a registry answer larger than this is not one.</summary>
    internal const int MaximumResponseBytes = 4 * 1024 * 1024;

    private static async Task<byte[]?> ReadBoundedAsync(Stream stream, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[81_920];
        int read;
        while ((read = await stream.ReadAsync(chunk.AsMemory(0, (int)Math.Min(chunk.Length, MaximumResponseBytes + 1L - buffer.Length)),
                   cancellationToken).ConfigureAwait(false)) > 0)
        {
            buffer.Write(chunk, 0, read);
            if (buffer.Length > MaximumResponseBytes)
            {
                return null;
            }
        }
        return buffer.ToArray();
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
