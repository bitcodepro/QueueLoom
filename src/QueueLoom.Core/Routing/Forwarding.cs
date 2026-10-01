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

    /// <summary>
    /// Where a message sent to (or forwarded into) <paramref name="entity"/> can end up, followed hop by hop. The name
    /// is looked up as a queue, then a topic, then a subscription ("topic/subscription"; topic names may hold '/').
    /// </summary>
    public static ForwardingReport Follow(ServiceBusTopology topology, string entity)
    {
        ArgumentNullException.ThrowIfNull(topology);
        var map = new Map(topology);
        return Follow(map, entity, map.Start(entity));
    }

    private static ForwardingReport Follow(Map map, string entity, Node start)
    {
        var walk = new WalkResult();
        Walk(map, start, [], 0, walk);
        return new ForwardingReport(entity, walk.Paths, walk.Loops, walk.Missing) { LongestChain = walk.LongestChain };
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

        string? Note(string path, EntityKind kind, string? forwardTo, string? deadLettersTo, string? existing)
        {
            var notes = new List<string>();
            if (TargetName(forwardTo) is { } target)
            {
                var report = Follow(map, path, new Node(path, kind));
                notes.Add(report.Describe(target));
            }
            if (TargetName(deadLettersTo) is { } deadLetterTarget)
            {
                notes.Add(map.Target(deadLetterTarget).Kind != EntityKind.Missing
                    ? $"Dead letters are forwarded to {deadLetterTarget}"
                    : $"Dead letters are forwarded to {deadLetterTarget}, which does not exist");
            }
            // Only queues and topics can be forwarded to; a subscription's path is never a target.
            if (kind != EntityKind.Subscription && map.Sources(path) is { Count: > 0 } sources)
            {
                notes.Add($"Gets forwarded messages from {string.Join(", ", sources.Take(3))}" + (sources.Count > 3 ? $" and {sources.Count - 3} more" : string.Empty));
            }
            if (notes.Count == 0)
            {
                return existing;
            }
            return existing is null ? string.Join(" · ", notes) : $"{existing} · {string.Join(" · ", notes)}";
        }

        var queues = topology.Queues.Select(queue => queue with { Note = Note(queue.Name, EntityKind.Queue, queue.ForwardTo, queue.ForwardDeadLettersTo, queue.Note) });
        var topics = topology.Topics.Select(topic => new ServiceBusTopic(topic.Name, topic.Runtime,
            topic.Subscriptions.Select(subscription => subscription with
            {
                Note = Note($"{topic.Name}/{subscription.Name}", EntityKind.Subscription, subscription.ForwardTo, subscription.ForwardDeadLettersTo, subscription.Note)
            }), topic.Status)
        {
            Note = Note(topic.Name, EntityKind.Topic, null, null, topic.Note)
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

    private enum EntityKind
    {
        Queue,
        Topic,
        Subscription,
        Missing
    }

    /// <summary>An entity with its kind: a queue and a subscription path may be spelled alike.</summary>
    private readonly record struct Node(string Name, EntityKind Kind)
    {
        public bool Is(Node other) => Kind == other.Kind && string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class WalkResult
    {
        public List<IReadOnlyList<string>> Paths { get; } = [];

        public List<IReadOnlyList<string>> Loops { get; } = [];

        public List<IReadOnlyList<string>> Missing { get; } = [];

        public int LongestChain { get; set; }
    }

    /// <param name="hops">Forwards so far; a topic copying to its subscription is not one.</param>
    private static void Walk(Map map, Node node, List<Node> path, int hops, WalkResult result)
    {
        result.LongestChain = Math.Max(result.LongestChain, hops);
        if (path.Any(item => item.Is(node)))
        {
            result.Loops.Add([.. path.Select(item => item.Name), node.Name]);
            return;
        }
        path.Add(node);
        try
        {
            var names = path.Select(item => item.Name).ToArray();
            if (hops > 3 * MaximumHops)
            {
                // Far past what Service Bus forwards; the chain is reported as too long without following it further.
                result.Paths.Add(names);
                return;
            }
            if (node.Kind == EntityKind.Missing)
            {
                result.Missing.Add(names);
                return;
            }
            var next = map.Next(node);
            if (next.Count == 0)
            {
                result.Paths.Add(names);
                return;
            }
            foreach (var target in next)
            {
                var isCopy = node.Kind == EntityKind.Topic && target.Kind == EntityKind.Subscription;
                Walk(map, target, path, isCopy ? hops : hops + 1, result);
            }
        }
        finally
        {
            path.RemoveAt(path.Count - 1);
        }
    }

    /// <summary>Entities by name (case-insensitive, as in Service Bus) and the forwards between them.</summary>
    private sealed class Map
    {
        private readonly Dictionary<string, string?> _queues = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, List<string>> _topics = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, string?> _subscriptions = new(StringComparer.OrdinalIgnoreCase);
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
                var paths = new List<string>();
                foreach (var subscription in topic.Subscriptions)
                {
                    // The full path is the key, so a topic named "orders/eu" keeps its subscriptions apart from "orders".
                    var path = $"{topic.Name}/{subscription.Name}";
                    paths.Add(path);
                    _subscriptions[path] = TargetName(subscription.ForwardTo);
                    AddSource(TargetName(subscription.ForwardTo), path);
                    AddSource(TargetName(subscription.ForwardDeadLettersTo), $"{path} (dead letters)");
                    HasForwarding |= subscription.ForwardTo is not null || subscription.ForwardDeadLettersTo is not null;
                }
                _topics[topic.Name] = paths;
            }
        }

        public bool HasForwarding { get; }

        /// <summary>Where a walk starts: a queue, else a topic, else a subscription.</summary>
        public Node Start(string entity) =>
            _queues.ContainsKey(entity) ? new Node(entity, EntityKind.Queue)
            : _topics.ContainsKey(entity) ? new Node(entity, EntityKind.Topic)
            : _subscriptions.ContainsKey(entity) ? new Node(entity, EntityKind.Subscription)
            : new Node(entity, EntityKind.Missing);

        /// <summary>A forward target: Service Bus forwards only to queues and topics.</summary>
        public Node Target(string entity) =>
            _queues.ContainsKey(entity) ? new Node(entity, EntityKind.Queue)
            : _topics.ContainsKey(entity) ? new Node(entity, EntityKind.Topic)
            : new Node(entity, EntityKind.Missing);

        /// <summary>A queue or subscription goes on to its forward target; a topic copies to each subscription.</summary>
        public IReadOnlyList<Node> Next(Node node) => node.Kind switch
        {
            EntityKind.Queue => _queues[node.Name] is { } forward ? [Target(forward)] : [],
            EntityKind.Subscription => _subscriptions[node.Name] is { } forward ? [Target(forward)] : [],
            EntityKind.Topic => _topics[node.Name].Select(path => new Node(path, EntityKind.Subscription)).ToArray(),
            _ => []
        };

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
    public int LongestChain { get; init; }

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
}
