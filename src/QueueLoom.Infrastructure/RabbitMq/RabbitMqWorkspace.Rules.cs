using System.Text;
using System.Text.Json;
using QueueLoom.Core.Routing;

namespace QueueLoom.Infrastructure.RabbitMq;

/// <summary>
/// An exchange's bindings as rules: each queue or exchange bound to it is a destination, each binding a rule named
/// by RabbitMQ's properties key. Presence bindings are created over AMQP because HTTP rejects null arguments;
/// a binding cannot be changed in place, so a change adds the new binding and then removes the old one.
/// </summary>
public sealed partial class RabbitMqWorkspace
{
    public override bool SupportsSubscriptionRules => true;

    public override RoutingService RoutingService => RoutingService.RabbitMq;

    public override Task<IReadOnlyList<SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default) =>
        ReadRulesAsync<IReadOnlyList<SubscriptionRules>>(async token =>
        {
            var vhost = Escape(_virtualHost);
            var exchangePath = $"api/exchanges/{vhost}/{Uri.EscapeDataString(topic)}";
            var exchange = await GetObjectAsync(exchangePath, token).ConfigureAwait(false);
            var type = exchange.TryGetProperty("type", out var typeValue) ? typeValue.GetString() ?? "direct" : "direct";
            var bindings = await GetArrayAsync($"{exchangePath}/bindings/source", token).ConfigureAwait(false);
            var result = bindings
                .GroupBy(binding => (Destination: binding.GetProperty("destination").GetString() ?? string.Empty,
                    IsExchange: binding.GetProperty("destination_type").GetString() == "exchange"))
                .OrderBy(group => group.Key.Destination, StringComparer.Ordinal)
                .Select(group => new SubscriptionRules(group.Key.Destination,
                    group.Select((binding, index) => ToRule(type, binding, index, group.Count()) with { ToExchange = group.Key.IsExchange }).ToArray())
                {
                    Service = RoutingService.RabbitMq,
                    IsExchange = group.Key.IsExchange,
                    Note = group.Key.IsExchange ? "Exchange: passes the message on through its own bindings" : null
                })
                .ToList();
            if (exchange.TryGetProperty("arguments", out var arguments) && arguments.ValueKind == JsonValueKind.Object &&
                arguments.TryGetProperty("alternate-exchange", out var alternate) && alternate.GetString() is { Length: > 0 } alternateName)
            {
                result.Add(new SubscriptionRules(alternateName, [])
                {
                    Service = RoutingService.RabbitMq,
                    IsExchange = true,
                    IsFallback = true,
                    Note = "Alternate exchange: gets what no binding takes"
                });
            }
            return result;
        }, cancellationToken);

    public override Task SaveSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule, bool replace,
        CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            ArgumentNullException.ThrowIfNull(rule);
            var path = BindingPath(topic, subscription, rule.ToExchange);
            var arguments = rule.Kind is RuleFilterKind.HeadersBinding or RuleFilterKind.OtherBinding
                ? rule.Arguments.ToDictionary(pair => pair.Key, pair => pair.Value)
                : new Dictionary<string, object?>();
            if (arguments.Values.Any(value => value is null))
            {
                await SavePresenceBindingAsync(topic, subscription, path, rule, arguments, replace, token).ConfigureAwait(false);
                return;
            }
            var body = JsonSerializer.Serialize(new { routing_key = rule.Expression ?? string.Empty, arguments });
            using var response = await Management.PostAsync(path, new StringContent(body, Encoding.UTF8, "application/json"), token)
                .ConfigureAwait(false);
            await EnsureSuccessAsync(response, "the management API").ConfigureAwait(false);
            // RabbitMQ answers with the new binding's location, which ends in its properties key.
            var created = response.Headers.Location is { } location
                ? Uri.UnescapeDataString(location.OriginalString.TrimEnd('/').Split('/')[^1])
                : null;
            if (replace && !string.Equals(created, rule.Name, StringComparison.Ordinal))
            {
                await DeleteBindingAsync(path, rule.Name, token).ConfigureAwait(false);
            }
        }, cancellationToken);

    private async Task SavePresenceBindingAsync(string topic, string destination, string path, SubscriptionRule rule,
        Dictionary<string, object?> arguments, bool replace, CancellationToken cancellationToken)
    {
        var key = rule.Expression ?? string.Empty;
        var expected = JsonSerializer.SerializeToElement(arguments);
        bool Matches(JsonElement binding) => binding.GetProperty("routing_key").GetString() == key &&
            JsonElement.DeepEquals(binding.GetProperty("arguments"), expected);

        // An unchanged save must keep the broker's identity and original AMQP field types, which HTTP JSON
        // cannot fully describe. This also avoids unnecessary native binds for existing presence conditions.
        if (replace)
        {
            var before = await GetArrayAsync(path, cancellationToken).ConfigureAwait(false);
            if (before.Any(binding => binding.GetProperty("properties_key").GetString() == rule.Name && Matches(binding))) return;
        }

        // RabbitMQ.Client encodes null as AMQP void; the management API's JSON conversion throws
        // null_not_allowed. Native binds wait for bind-ok and happen before any deletion of the old binding.
        await using var channel = await Connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        if (BindingDestinationIsExchange(destination, rule.ToExchange))
            await channel.ExchangeBindAsync(destination, topic, key, arguments, cancellationToken: cancellationToken).ConfigureAwait(false);
        else
            await channel.QueueBindAsync(destination, topic, key, arguments, cancellationToken: cancellationToken).ConfigureAwait(false);

        // Native bind-ok does not carry a properties_key. Read the destination-specific binding collection;
        // if identity cannot be confirmed uniquely, retain the old binding rather than risk losing its route.
        var after = await GetArrayAsync(path, cancellationToken).ConfigureAwait(false);
        var matches = after.Where(Matches).ToArray();
        if (matches.Length != 1 || !matches[0].TryGetProperty("properties_key", out var property) ||
            property.GetString() is not { Length: > 0 } created)
            throw new InvalidOperationException("The presence binding was added, but its identity could not be confirmed. " +
                "The previous binding was retained; refresh the bindings before trying again.");
        if (replace && !string.Equals(created, rule.Name, StringComparison.Ordinal))
            await DeleteBindingAsync(path, rule.Name, cancellationToken).ConfigureAwait(false);
    }

    public override Task DeleteSubscriptionRuleAsync(string topic, string subscription, string rule, CancellationToken cancellationToken = default) =>
        ManageAsync(token => DeleteBindingAsync(BindingPath(topic, subscription, null), rule, token), cancellationToken);

    public override Task DeleteSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule,
        CancellationToken cancellationToken = default) =>
        ManageAsync(token => DeleteBindingAsync(BindingPath(topic, subscription, rule.ToExchange), rule.Name, token), cancellationToken);

    private HttpClient Management => _management ?? throw new InvalidOperationException("Connect to the environment first.");

    /// <summary>
    /// api/bindings/vhost/e/exchange/q/queue, or …/e/exchange when the destination is an exchange. A queue and an
    /// exchange can share a name, so the caller says which one it means; without that, a shared name is refused
    /// rather than guessed, since a guess could change another destination's binding.
    /// </summary>
    private string BindingPath(string exchange, string destination, bool? toExchange) =>
        $"api/bindings/{Escape(_virtualHost)}/e/{Uri.EscapeDataString(exchange)}/{(BindingDestinationIsExchange(destination, toExchange) ? "e" : "q")}/{Uri.EscapeDataString(destination)}";

    private bool BindingDestinationIsExchange(string destination, bool? toExchange)
    {
        var isQueue = _index.FindQueue(destination) is not null;
        var isExchange = _index.IsExchange(destination);
        var kind = toExchange switch
        {
            true when isExchange => "e",
            false when isQueue => "q",
            null when isQueue && isExchange => throw new InvalidOperationException(
                $"'{destination}' is the name of both a queue and an exchange; say which one the binding leads to."),
            null when isQueue => "q",
            null when isExchange => "e",
            _ => null
        };
        return kind is null
            ? throw new InvalidOperationException(
                $"{(toExchange == true ? "Exchange" : toExchange == false ? "Queue" : "Destination")} '{destination}' was not found. Refresh and try again.")
            : kind == "e";
    }

    private async Task DeleteBindingAsync(string path, string propertiesKey, CancellationToken cancellationToken)
    {
        using var response = await Management.DeleteAsync($"{path}/{Uri.EscapeDataString(propertiesKey)}", cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, "the management API").ConfigureAwait(false);
    }

    private async Task<JsonElement> GetObjectAsync(string path, CancellationToken cancellationToken)
    {
        using var response = await Management.GetAsync(path, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, "the management API").ConfigureAwait(false);
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false));
        return document.RootElement.Clone();
    }

    internal static SubscriptionRule ToRule(string exchangeType, JsonElement binding, int index, int count)
    {
        var key = binding.TryGetProperty("routing_key", out var routingKey) ? routingKey.GetString() ?? string.Empty : string.Empty;
        var propertiesKey = binding.TryGetProperty("properties_key", out var properties) ? properties.GetString() ?? key : key;
        var arguments = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (binding.TryGetProperty("arguments", out var values) && values.ValueKind == JsonValueKind.Object)
        {
            foreach (var argument in values.EnumerateObject())
            {
                arguments[argument.Name] = ToValue(argument.Value);
            }
        }
        var (kind, title) = exchangeType switch
        {
            "direct" => (RuleFilterKind.DirectBinding, $"'{key}'"),
            "topic" => (RuleFilterKind.TopicBinding, $"'{key}'"),
            "fanout" => (RuleFilterKind.FanoutBinding, "fanout"),
            "headers" => (RuleFilterKind.HeadersBinding, count > 1 ? $"headers {index + 1}" : "headers"),
            _ => (RuleFilterKind.OtherBinding, key.Length > 0 ? $"'{key}'" : $"{exchangeType} binding")
        };
        return new SubscriptionRule(propertiesKey, kind) { Expression = key, Arguments = arguments, Title = title };
    }

    private static object? ToValue(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.String => value.GetString(),
        JsonValueKind.Number => value.TryGetInt64(out var whole) ? (object)whole : value.GetDouble(),
        JsonValueKind.True or JsonValueKind.False => value.GetBoolean(),
        JsonValueKind.Null => null,
        _ => value.GetRawText()
    };
}
