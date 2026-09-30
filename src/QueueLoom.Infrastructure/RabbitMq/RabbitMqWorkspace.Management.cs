using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;

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
        return new QueueSettings(ttl is null ? null : TimeSpan.FromMilliseconds(ttl.Value), (int?)info.DeliveryLimit);
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
            if (definition.CreateDeadLetterQueue)
            {
                var deadLetter = definition.Name + ".dlq";
                await PutQueueAsync(deadLetter, new Dictionary<string, object> { ["x-queue-type"] = "quorum" }, token).ConfigureAwait(false);
                arguments["x-dead-letter-exchange"] = string.Empty;
                arguments["x-dead-letter-routing-key"] = deadLetter;
            }
            await PutQueueAsync(definition.Name, arguments, token).ConfigureAwait(false);
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

    private async Task PutQueueAsync(string name, Dictionary<string, object> arguments, CancellationToken cancellationToken)
    {
        var management = _management ?? throw new InvalidOperationException("Connect to the environment first.");
        var path = $"api/queues/{Escape(_virtualHost)}/{Uri.EscapeDataString(name)}";
        using (var existing = await management.GetAsync(path, cancellationToken).ConfigureAwait(false))
        {
            if (existing.StatusCode != HttpStatusCode.NotFound)
            {
                throw new InvalidOperationException($"A queue named '{name}' already exists.");
            }
        }

        var body = JsonSerializer.Serialize(new { durable = true, auto_delete = false, arguments });
        using var response = await management.PutAsync(path, new StringContent(body, Encoding.UTF8, "application/json"), cancellationToken)
            .ConfigureAwait(false);
        await EnsureSuccessAsync(response, "the management API").ConfigureAwait(false);
    }
}
