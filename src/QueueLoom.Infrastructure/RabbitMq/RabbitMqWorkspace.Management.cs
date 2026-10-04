using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Infrastructure.RabbitMq;

/// <summary>
/// Creating and deleting queues through the management API. New queues are durable quorum queues; their dead
/// letters go to "&lt;name&gt;.dlq" through the default exchange. RabbitMQ does not change a queue's arguments later.
/// </summary>
public sealed partial class RabbitMqWorkspace
{
    public override QueueManagementCapabilities? QueueManagement => new(
        "queue",
        QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount,
        QueueSettingFlags.None,
        CanCreateDeadLetterQueue: true,
        UpdateNote: "RabbitMQ does not change a queue's settings after it is created. Use a policy in the RabbitMQ management UI instead.");

    public override async Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default)
    {
        using var operation = await EnterReadOperationAsync(cancellationToken).ConfigureAwait(false);
        var management = _management ?? throw new InvalidOperationException("Connect to the environment first.");
        using var response = await management.GetAsync($"api/queues/{Escape(_virtualHost)}/{Uri.EscapeDataString(queue)}", cancellationToken)
            .ConfigureAwait(false);
        await EnsureSuccessAsync(response, "the management API").ConfigureAwait(false);
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false));
        var info = RabbitQueueInfo.From(document.RootElement);
        long? ttl = document.RootElement.TryGetProperty("arguments", out var arguments) &&
                    arguments.TryGetProperty("x-message-ttl", out var value) && value.TryGetInt64(out var milliseconds)
            ? milliseconds
            : null;
        return new QueueSettings(ttl is null ? null : BrokerClock.FromMilliseconds(ttl.Value), (int?)info.DeliveryLimit);
    }

    public override Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            ArgumentNullException.ThrowIfNull(definition);
            var arguments = new Dictionary<string, object> { ["x-queue-type"] = "quorum" };
            if (definition.Settings.MessageTimeToLive is { } timeToLive)
            {
                arguments["x-message-ttl"] = (long)timeToLive.TotalMilliseconds;
            }
            if (definition.Settings.MaxDeliveryCount is { } limit)
            {
                arguments["x-delivery-limit"] = limit;
            }
            string? createdDeadLetter = null;
            if (definition.CreateDeadLetterQueue)
            {
                var deadLetter = definition.Name + ".dlq";
                // Refuse a taken name before anything is created, so a refusal leaves nothing behind.
                await EnsureQueueMissingAsync(definition.Name, token).ConfigureAwait(false);
                await PutQueueAsync(deadLetter, new Dictionary<string, object> { ["x-queue-type"] = "quorum" }, token).ConfigureAwait(false);
                createdDeadLetter = deadLetter;
                arguments["x-dead-letter-exchange"] = string.Empty;
                arguments["x-dead-letter-routing-key"] = deadLetter;
            }
            var outcome = new PutOutcome();
            try
            {
                await PutQueueAsync(definition.Name, arguments, token, outcome).ConfigureAwait(false);
            }
            catch when (createdDeadLetter is not null && outcome.Refused)
            {
                // Only a confirmed refusal with the main queue still absent removes the dead-letter queue this call
                // created; otherwise it would block every retry with "already exists". It stays when the main queue
                // exists (another creator won the race and may route to it) or its state is unknown (a timeout, a
                // cancellation, a lost response, a failed lookup).
                if (await QueueExistsAsync(definition.Name).ConfigureAwait(false) == false)
                {
                    var management = _management ?? throw new InvalidOperationException("Connect to the environment first.");
                    using var _ = await management.DeleteAsync(
                            $"api/queues/{Escape(_virtualHost)}/{Uri.EscapeDataString(createdDeadLetter)}?if-empty=true", CancellationToken.None)
                        .ConfigureAwait(false);
                }
                throw;
            }
        }, cancellationToken);

    public override Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(QueueManagement!.UpdateNote);

    public override Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            var management = _management ?? throw new InvalidOperationException("Connect to the environment first.");
            using var response = await management.DeleteAsync($"api/queues/{Escape(_virtualHost)}/{Uri.EscapeDataString(queue)}", token)
                .ConfigureAwait(false);
            await EnsureSuccessAsync(response, "the management API").ConfigureAwait(false);
        }, cancellationToken);

    private async Task EnsureQueueMissingAsync(string name, CancellationToken cancellationToken)
    {
        var management = _management ?? throw new InvalidOperationException("Connect to the environment first.");
        using var existing = await management.GetAsync($"api/queues/{Escape(_virtualHost)}/{Uri.EscapeDataString(name)}", cancellationToken)
            .ConfigureAwait(false);
        if (existing.StatusCode != HttpStatusCode.NotFound)
        {
            throw new InvalidOperationException($"A queue named '{name}' already exists.");
        }
    }

    /// <summary>Whether a failed PUT certainly created nothing: it was never sent, or the broker answered 4xx.</summary>
    private sealed class PutOutcome
    {
        public bool Refused { get; set; } = true;
    }

    /// <summary>True or false when the broker answers clearly; null when the lookup itself fails.</summary>
    private async Task<bool?> QueueExistsAsync(string name)
    {
        try
        {
            var management = _management ?? throw new InvalidOperationException("Connect to the environment first.");
            using var response = await management.GetAsync(
                    $"api/queues/{Escape(_virtualHost)}/{Uri.EscapeDataString(name)}", CancellationToken.None)
                .ConfigureAwait(false);
            return response.StatusCode == HttpStatusCode.NotFound ? false : response.IsSuccessStatusCode ? true : null;
        }
        catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException or InvalidOperationException)
        {
            return null;
        }
    }

    private async Task PutQueueAsync(string name, Dictionary<string, object> arguments, CancellationToken cancellationToken,
        PutOutcome? outcome = null)
    {
        var management = _management ?? throw new InvalidOperationException("Connect to the environment first.");
        var path = $"api/queues/{Escape(_virtualHost)}/{Uri.EscapeDataString(name)}";
        await EnsureQueueMissingAsync(name, cancellationToken).ConfigureAwait(false);

        var body = JsonSerializer.Serialize(new { durable = true, auto_delete = false, arguments });
        if (outcome is not null)
        {
            outcome.Refused = false;
        }
        using var response = await management.PutAsync(path, new StringContent(body, Encoding.UTF8, "application/json"), cancellationToken)
            .ConfigureAwait(false);
        if (outcome is not null && (int)response.StatusCode is >= 400 and < 500)
        {
            outcome.Refused = true;
        }
        await EnsureSuccessAsync(response, "the management API").ConfigureAwait(false);
        // RabbitMQ answers 201 when it creates the queue and 204 when an equivalent one already exists: another
        // creator won the race after the lookup above, and that queue is not ours to configure or clean up.
        if (response.StatusCode == HttpStatusCode.NoContent)
        {
            throw new InvalidOperationException($"A queue named '{name}' already exists.");
        }
    }
}
