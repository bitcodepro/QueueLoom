using System.Globalization;
using System.Text.Json;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.RabbitMq;

/// <summary>A queue as the management API reports it, with its dead-letter settings from arguments or policy.</summary>
internal sealed record RabbitQueueInfo(
    string Name,
    string Type,
    long Ready,
    long Unacknowledged,
    string? DeadLetterExchange,
    string? DeadLetterRoutingKey,
    long? DeliveryLimit,
    string State)
{
    /// <summary>Connected consumers; null when the management API leaves the field out.</summary>
    public int? Consumers { get; init; }

    public bool IsStream => Type == "stream";

    public bool IsQuorum => Type == "quorum";

    public static RabbitQueueInfo From(JsonElement queue)
    {
        string? Setting(string argument, string policyKey) =>
            queue.TryGetProperty("arguments", out var arguments) && arguments.ValueKind == JsonValueKind.Object &&
            arguments.TryGetProperty(argument, out var value) && value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : queue.TryGetProperty("effective_policy_definition", out var policy) && policy.ValueKind == JsonValueKind.Object &&
                  policy.TryGetProperty(policyKey, out var fromPolicy) && fromPolicy.ValueKind == JsonValueKind.String
                    ? fromPolicy.GetString()
                    : null;

        long? Limit()
        {
            foreach (var (container, key) in new[] { ("arguments", "x-delivery-limit"), ("effective_policy_definition", "delivery-limit") })
            {
                if (queue.TryGetProperty(container, out var settings) && settings.ValueKind == JsonValueKind.Object &&
                    settings.TryGetProperty(key, out var value) && value.TryGetInt64(out var limit))
                {
                    return limit;
                }
            }
            return null;
        }

        return new RabbitQueueInfo(
            queue.GetProperty("name").GetString()!,
            queue.TryGetProperty("type", out var type) ? type.GetString() ?? "classic" : "classic",
            ReadLong(queue, "messages_ready"),
            ReadLong(queue, "messages_unacknowledged"),
            Setting("x-dead-letter-exchange", "dead-letter-exchange"),
            Setting("x-dead-letter-routing-key", "dead-letter-routing-key"),
            Limit(),
            queue.TryGetProperty("state", out var state) ? state.GetString() ?? "running" : "running")
        {
            Consumers = queue.TryGetProperty("consumers", out var consumers) && consumers.TryGetInt32(out var count) ? count : null
        };
    }

    private static long ReadLong(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.TryGetInt64(out var number) && number > 0 ? number : 0;
}

internal sealed record RabbitExchangeInfo(string Name, string Type)
{
    /// <summary>The default exchange and the built-in amq.* exchanges are not shown as topics.</summary>
    public bool IsUserExchange => Name.Length > 0 && !Name.StartsWith("amq.", StringComparison.Ordinal);
}

internal sealed record RabbitBindingInfo(string Exchange, string Queue, string RoutingKey);

/// <summary>Where each queue's dead letters end up, and the conversion to QueueLoom's model.</summary>
internal sealed class RabbitMqTopologyIndex
{
    private readonly Dictionary<string, RabbitQueueInfo> _queues;
    private readonly Dictionary<string, string> _deadLetterQueues = new(StringComparer.Ordinal);

    public RabbitMqTopologyIndex(
        IEnumerable<RabbitQueueInfo> queues,
        IEnumerable<RabbitExchangeInfo> exchanges,
        IEnumerable<RabbitBindingInfo> bindings)
    {
        Queues = queues.OrderBy(queue => queue.Name, StringComparer.Ordinal).ToArray();
        Exchanges = exchanges.OrderBy(exchange => exchange.Name, StringComparer.Ordinal).ToArray();
        Bindings = bindings.ToArray();
        _queues = Queues.ToDictionary(queue => queue.Name, StringComparer.Ordinal);
        foreach (var queue in Queues)
        {
            if (ResolveDeadLetterQueue(queue) is { } target)
            {
                _deadLetterQueues[queue.Name] = target;
            }
        }
    }

    public static RabbitMqTopologyIndex Empty { get; } = new([], [], []);

    public IReadOnlyList<RabbitQueueInfo> Queues { get; }

    public IReadOnlyList<RabbitExchangeInfo> Exchanges { get; }

    public IReadOnlyList<RabbitBindingInfo> Bindings { get; }

    public RabbitQueueInfo? FindQueue(string name) => _queues.GetValueOrDefault(name);

    public string? DeadLetterQueueOf(string queue) => _deadLetterQueues.GetValueOrDefault(queue);

    /// <summary>True when several queues dead-letter into the same queue, so reads must filter by the x-death header.</summary>
    public bool IsSharedDeadLetterQueue(string deadLetterQueue) =>
        _deadLetterQueues.Values.Count(target => target == deadLetterQueue) > 1;

    public bool IsExchange(string name) => Exchanges.Any(exchange => exchange.Name == name);

    public ServiceBusTopology ToTopology(DateTimeOffset fetchedAt)
    {
        var users = _deadLetterQueues
            .GroupBy(pair => pair.Value, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Select(pair => pair.Key).Order(StringComparer.Ordinal).ToArray(), StringComparer.Ordinal);

        var queues = Queues.Select(queue =>
        {
            var deadLetter = DeadLetterQueueOf(queue.Name) is { } target ? FindQueue(target) : null;
            var notes = new List<string>();
            if (users.TryGetValue(queue.Name, out var sources))
            {
                notes.Add($"Dead-letter queue of {string.Join(", ", sources)}");
            }
            if (queue.DeadLetterExchange is not null && deadLetter is null)
            {
                notes.Add(queue.DeadLetterExchange.Length == 0
                    ? "Its dead-letter routing key matches no queue"
                    : $"Dead-letter exchange {queue.DeadLetterExchange} routes to no queue");
            }
            else if (deadLetter is not null && users.TryGetValue(deadLetter.Name, out var sharing) && sharing.Length > 1)
            {
                notes.Add($"Shares dead-letter queue {deadLetter.Name} with {sharing.Length - 1} other queue(s); its count covers all of them");
            }
            if (queue.IsQuorum)
            {
                notes.Add(queue.DeliveryLimit is { } limit and > 0
                    ? $"Quorum queue, delivery limit {limit.ToString(CultureInfo.CurrentCulture)}"
                    : "Quorum queue");
            }
            if (queue.IsStream)
            {
                notes.Add("Stream: reading is not supported");
            }
            if (queue.Unacknowledged > 0)
            {
                notes.Add($"{queue.Unacknowledged.ToString("N0", CultureInfo.CurrentCulture)} being processed");
            }

            var runtime = new ServiceBusEntityRuntime(new ServiceBusMessageCounts(
                active: queue.Ready,
                deadLetter: deadLetter?.Ready ?? 0))
            { HasTransferDeadLetterCount = false };
            return new ServiceBusQueue(queue.Name, runtime,
                queue.State is "running" or "idle" or "live" ? ServiceBusEntityStatus.Active : ServiceBusEntityStatus.Unknown)
            {
                HasDeadLetterQueue = deadLetter is not null,
                Note = notes.Count == 0 ? null : string.Join(" · ", notes),
                Consumers = queue.Consumers is { } consumers ? ConsumerActivity.Connected(consumers) : null
            };
        });

        var deadLetterExchanges = Queues
            .Where(queue => !string.IsNullOrEmpty(queue.DeadLetterExchange))
            .Select(queue => queue.DeadLetterExchange!)
            .ToHashSet(StringComparer.Ordinal);
        var topics = Exchanges.Where(exchange => exchange.IsUserExchange).Select(exchange =>
        {
            var targets = Bindings.Where(binding => binding.Exchange == exchange.Name).Select(binding => binding.Queue)
                .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
            var note = $"{exchange.Type} exchange" +
                       (deadLetterExchanges.Contains(exchange.Name) ? " for dead letters" : string.Empty) +
                       (targets.Length == 0 ? " · routes to no queue" : $" · routes to {string.Join(", ", targets.Take(4))}" +
                           (targets.Length > 4 ? $" and {targets.Length - 4} more" : string.Empty));
            return new ServiceBusTopic(
                exchange.Name,
                new ServiceBusEntityRuntime(ServiceBusMessageCounts.Empty) { HasTransferDeadLetterCount = false },
                [],
                ServiceBusEntityStatus.Active) { Note = note };
        });

        return new ServiceBusTopology(fetchedAt, queues, topics) { SupportsTransferDeadLetter = false, TopicKindName = "exchange" };
    }

    /// <summary>
    /// Follows the queue's dead-letter exchange and routing key to the queue that receives its dead letters:
    /// the default exchange routes by queue name; direct and topic exchanges by their bindings; fanout and headers
    /// exchanges to every bound queue (the first one is used).
    /// </summary>
    private string? ResolveDeadLetterQueue(RabbitQueueInfo queue)
    {
        if (queue.DeadLetterExchange is null)
        {
            return null;
        }

        var routingKey = queue.DeadLetterRoutingKey ?? queue.Name;
        if (queue.DeadLetterExchange.Length == 0)
        {
            return routingKey != queue.Name && _queues.ContainsKey(routingKey) ? routingKey : null;
        }

        var exchange = Exchanges.FirstOrDefault(item => item.Name == queue.DeadLetterExchange);
        if (exchange is null)
        {
            return null;
        }

        return Bindings
            .Where(binding => binding.Exchange == exchange.Name && binding.Queue != queue.Name && _queues.ContainsKey(binding.Queue))
            .Where(binding => exchange.Type switch
            {
                "direct" => binding.RoutingKey == routingKey,
                "topic" => TopicMatches(binding.RoutingKey, routingKey),
                _ => true
            })
            .Select(binding => binding.Queue)
            .Order(StringComparer.Ordinal)
            .FirstOrDefault();
    }

    /// <summary>AMQP topic matching: '*' is one word, '#' is zero or more words.</summary>
    internal static bool TopicMatches(string pattern, string routingKey) => RabbitBindings.TopicMatches(pattern, routingKey);
}
