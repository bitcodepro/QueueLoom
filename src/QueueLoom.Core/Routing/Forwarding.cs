using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Routing;

/// <summary>
/// Azure Service Bus auto-forwarding: a queue or subscription that forwards keeps nothing, every message moves on to
/// another queue or topic, and a topic copies it to its subscriptions, which may forward again. Service Bus moves a
/// message across at most 4 forwards and dead-letters it after that, so a loop or a long chain ends in a
/// dead-letter queue; a forward to an entity that does not exist loses nothing only because Service Bus refuses it.
/// </summary>
public static class Forwarding
{
    /// <summary>Service Bus moves a message through at most this many forwards (its transfer hop count).</summary>
    public const int MaximumHops = 4;

    /// <summary>The entity name a ForwardTo value points at; Service Bus also accepts a full sb:// or https:// address.</summary>
    public static string? TargetName(string? forwardTo)
    {
        if (string.IsNullOrWhiteSpace(forwardTo))
        {
            return null;
        }
        var value = forwardTo.Trim();
        return Uri.TryCreate(value, UriKind.Absolute, out var uri) && uri.Scheme is "sb" or "https" or "http"
            ? uri.AbsolutePath.Trim('/')
            : value.Trim('/');
    }

    /// <summary>Where a message sent to (or forwarded into) <paramref name="entity"/> can end up, followed hop by hop.</summary>
    public static ForwardingReport Follow(ServiceBusTopology topology, string entity)
    {
        ArgumentNullException.ThrowIfNull(topology);
        var map = new Map(topology);
        var paths = new List<IReadOnlyList<string>>();
        var loops = new List<IReadOnlyList<string>>();
        var missing = new List<IReadOnlyList<string>>();
        Walk(map, entity, [], paths, loops, missing);
        return new ForwardingReport(entity, paths, loops, missing);
    }

    /// <summary>
    /// Adds to every queue and subscription what forwarding means for it: where its messages or dead letters go,
    /// where they come from, and whether a chain loops, is too long or points at nothing.
    /// </summary>
    public static ServiceBusTopology Annotate(ServiceBusTopology topology)
    {
        ArgumentNullException.ThrowIfNull(topology);
        var map = new Map(topology);
        if (!map.HasForwarding)
        {
            return topology;
        }

        string? Note(string path, string? forwardTo, string? deadLettersTo, string? existing)
        {
            var notes = new List<string>();
            if (TargetName(forwardTo) is { } target)
            {
                var report = Follow(topology, path);
                notes.Add(report.Describe(target));
            }
            if (TargetName(deadLettersTo) is { } deadLetterTarget)
            {
                notes.Add(map.Exists(deadLetterTarget)
                    ? $"Dead letters are forwarded to {deadLetterTarget}"
                    : $"Dead letters are forwarded to {deadLetterTarget}, which does not exist");
            }
            if (map.Sources(path) is { Count: > 0 } sources)
            {
                notes.Add($"Gets forwarded messages from {string.Join(", ", sources.Take(3))}" + (sources.Count > 3 ? $" and {sources.Count - 3} more" : string.Empty));
            }
            if (notes.Count == 0)
            {
                return existing;
            }
            return existing is null ? string.Join(" · ", notes) : $"{existing} · {string.Join(" · ", notes)}";
        }

        var queues = topology.Queues.Select(queue => queue with { Note = Note(queue.Name, queue.ForwardTo, queue.ForwardDeadLettersTo, queue.Note) });
        var topics = topology.Topics.Select(topic => new ServiceBusTopic(topic.Name, topic.Runtime,
            topic.Subscriptions.Select(subscription => subscription with
            {
                Note = Note($"{topic.Name}/{subscription.Name}", subscription.ForwardTo, subscription.ForwardDeadLettersTo, subscription.Note)
            }), topic.Status)
        {
            Note = Note(topic.Name, null, null, topic.Note)
        });
        return new ServiceBusTopology(topology.FetchedAt, queues, topics)
        {
            UsesSampledCounts = topology.UsesSampledCounts,
            QueueKindName = topology.QueueKindName,
            TopicKindName = topology.TopicKindName,
            SupportsTransferDeadLetter = topology.SupportsTransferDeadLetter,
            CanDeleteSelectedMessages = topology.CanDeleteSelectedMessages,
            HasMessageCounts = topology.HasMessageCounts
        };
    }

    private static void Walk(Map map, string entity, List<string> path, List<IReadOnlyList<string>> paths,
        List<IReadOnlyList<string>> loops, List<IReadOnlyList<string>> missing)
    {
        if (path.Contains(entity, StringComparer.OrdinalIgnoreCase))
        {
            loops.Add([.. path, entity]);
            return;
        }
        path.Add(entity);
        try
        {
            if (path.Count > 3 * Forwarding.MaximumHops)
            {
                // Far past what Service Bus forwards; the chain is reported as too long without following it further.
                paths.Add([.. path]);
                return;
            }
            if (!map.Exists(entity))
            {
                missing.Add([.. path]);
                return;
            }
            var next = map.Next(entity);
            if (next.Count == 0)
            {
                paths.Add([.. path]);
                return;
            }
            foreach (var target in next)
            {
                Walk(map, target, path, paths, loops, missing);
            }
        }
        finally
        {
            path.RemoveAt(path.Count - 1);
        }
    }

    /// <summary>Entity names (case-insensitive, as in Service Bus) and the forwards between them.</summary>
    private sealed class Map
    {
        private readonly Dictionary<string, string?> _queues = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, List<(string Path, string? ForwardTo)>> _topics = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, List<string>> _sources = new(StringComparer.OrdinalIgnoreCase);

        public Map(ServiceBusTopology topology)
        {
            foreach (var queue in topology.Queues)
            {
                _queues[queue.Name] = TargetName(queue.ForwardTo);
                AddSource(TargetName(queue.ForwardTo), queue.Name);
                AddSource(TargetName(queue.ForwardDeadLettersTo), $"{queue.Name} (dead letters)");
                HasForwarding |= queue.ForwardTo is not null || queue.ForwardDeadLettersTo is not null;
            }
            foreach (var topic in topology.Topics)
            {
                var subscriptions = new List<(string, string?)>();
                foreach (var subscription in topic.Subscriptions)
                {
                    var path = $"{topic.Name}/{subscription.Name}";
                    subscriptions.Add((path, TargetName(subscription.ForwardTo)));
                    AddSource(TargetName(subscription.ForwardTo), path);
                    AddSource(TargetName(subscription.ForwardDeadLettersTo), $"{path} (dead letters)");
                    HasForwarding |= subscription.ForwardTo is not null || subscription.ForwardDeadLettersTo is not null;
                }
                _topics[topic.Name] = subscriptions;
            }
        }

        public bool HasForwarding { get; }

        public bool Exists(string entity) =>
            _queues.ContainsKey(entity) || _topics.ContainsKey(entity) || IsSubscription(entity);

        /// <summary>A queue or subscription goes on to its forward target; a topic to each subscription.</summary>
        public IReadOnlyList<string> Next(string entity)
        {
            if (_queues.TryGetValue(entity, out var forwardTo))
            {
                return forwardTo is null ? [] : [forwardTo];
            }
            if (_topics.TryGetValue(entity, out var subscriptions))
            {
                return subscriptions.Select(subscription => subscription.Path).ToArray();
            }
            var forward = Subscription(entity)?.ForwardTo;
            return forward is null ? [] : [forward];
        }

        public IReadOnlyList<string> Sources(string entity) => _sources.TryGetValue(entity, out var sources) ? sources : [];

        private void AddSource(string? target, string source)
        {
            if (target is null)
            {
                return;
            }
            if (!_sources.TryGetValue(target, out var list))
            {
                _sources[target] = list = [];
            }
            list.Add(source);
        }

        private bool IsSubscription(string entity) => Subscription(entity) is not null;

        private (string Path, string? ForwardTo)? Subscription(string entity)
        {
            var slash = entity.IndexOf('/', StringComparison.Ordinal);
            if (slash <= 0 || !_topics.TryGetValue(entity[..slash], out var subscriptions))
            {
                return null;
            }
            foreach (var subscription in subscriptions)
            {
                if (string.Equals(subscription.Path, entity, StringComparison.OrdinalIgnoreCase))
                {
                    return subscription;
                }
            }
            return null;
        }
    }
}

/// <summary>Every way a message can travel from <see cref="Start"/>, as lists of entities, the first being the start.</summary>
public sealed record ForwardingReport(
    string Start,
    IReadOnlyList<IReadOnlyList<string>> Paths,
    IReadOnlyList<IReadOnlyList<string>> Loops,
    IReadOnlyList<IReadOnlyList<string>> Missing)
{
    /// <summary>The most forwards any message makes; a topic copying to a subscription is not a forward.</summary>
    public int LongestChain => Paths.Concat(Loops).Concat(Missing).Select(Hops).DefaultIfEmpty(0).Max();

    public bool IsTooLong => LongestChain > Forwarding.MaximumHops;

    /// <summary>Where messages finally stay: the last entity of each path.</summary>
    public IReadOnlyList<string> Destinations => Paths.Select(path => path[^1]).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

    /// <summary>One line for an entity that forwards to <paramref name="target"/>.</summary>
    public string Describe(string target)
    {
        if (Loops.Count > 0)
        {
            return $"Forwarding loop: {string.Join(" → ", Loops[0])}; Service Bus dead-letters these messages after {Forwarding.MaximumHops} forwards";
        }
        if (Missing.Count > 0)
        {
            return $"Forwards to {string.Join(" → ", Missing[0].Skip(1))}, which does not exist";
        }
        var chain = Paths.Count == 1 && Paths[0].Count > 2
            ? $"Forwards to {string.Join(" → ", Paths[0].Skip(1))}"
            : Destinations.Count > 1 || Destinations.Count == 1 && !string.Equals(Destinations[0], target, StringComparison.OrdinalIgnoreCase)
                ? $"Forwards to {target}; messages end up in {string.Join(", ", Destinations.Take(3))}" + (Destinations.Count > 3 ? $" and {Destinations.Count - 3} more" : string.Empty)
                : $"Forwards to {target}";
        return IsTooLong
            ? $"{chain}; {LongestChain} forwards is more than Service Bus allows ({Forwarding.MaximumHops}), so messages are dead-lettered on the way"
            : chain;
    }

    /// <summary>Forwards along a path: every step except a topic copying to one of its subscriptions.</summary>
    private static int Hops(IReadOnlyList<string> path)
    {
        var hops = 0;
        for (var index = 1; index < path.Count; index++)
        {
            var isCopy = path[index].StartsWith(path[index - 1] + "/", StringComparison.OrdinalIgnoreCase);
            if (!isCopy)
            {
                hops++;
            }
        }
        return hops;
    }
}
