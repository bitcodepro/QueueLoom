using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace QueueLoom.Infrastructure.RabbitMq;

/// <summary>
/// Queues and exchanges of one RabbitMQ virtual host. The management plugin (HTTP) lists them with their counts;
/// messages are read over AMQP with basic.get and returned with a requeue, which RabbitMQ allows without changing
/// them. A queue's dead letters are read from the queue its dead-letter exchange routes to.
/// </summary>
public sealed partial class RabbitMqWorkspace : LeasedMessagingWorkspace
{
    private const int MaximumBatch = 100;

    private readonly ISecretVault _secretVault;
    private readonly HttpMessageHandler? _httpHandler;
    private HttpClient? _management;
    private IConnection? _connection;
    private string _virtualHost = "/";
    private RabbitMqTopologyIndex _index = RabbitMqTopologyIndex.Empty;
    private readonly Func<ConnectionFactory, CancellationToken, Task<IConnection>>? _openBindingConnection;

    public RabbitMqWorkspace(
        ISecretVault secretVault,
        TimeProvider? timeProvider = null,
        DeadLetterJsonBackupStore? backupStore = null,
        HttpMessageHandler? httpHandler = null)
        : base(backupStore, timeProvider)
    {
        ArgumentNullException.ThrowIfNull(secretVault);
        _secretVault = secretVault;
        _httpHandler = httpHandler;
    }

    public override MessagingProvider Provider => MessagingProvider.RabbitMq;

    internal RabbitMqWorkspace(ISecretVault secretVault, Func<ConnectionFactory, CancellationToken, Task<IConnection>> openBindingConnection)
        : this(secretVault) => _openBindingConnection = openBindingConnection;

    private async Task<IConnection> OpenBindingConnectionAsync(CancellationToken cancellationToken)
    {
        var profile = GetConnectedProfile();
        var settings = profile.RabbitMq ?? throw new InvalidOperationException("The RabbitMQ settings are missing.");
        var password = await _secretVault.RetrieveForProfileAsync(profile, ProfileSecretKind.ConnectionString, cancellationToken)
            .ConfigureAwait(false) ?? throw new InvalidOperationException("The RabbitMQ password is missing. Edit the environment and enter it again.");
        var factory = CreateAmqpFactory(settings, password);
        // These mutations are durable broker topology, not resources owned by the long-lived client.
        // HTTP deletion cannot remove autorecovery records. Keep mutation records off the main connection.
        factory.AutomaticRecoveryEnabled = false;
        factory.TopologyRecoveryEnabled = false;
        factory.ClientProvidedName = "QueueLoom binding mutation";
        return _openBindingConnection is { } open
            ? await open(factory, cancellationToken).ConfigureAwait(false)
            : await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
    }

    private static ConnectionFactory CreateAmqpFactory(RabbitMqSettings settings, string password) => new()
    {
        HostName = settings.Host, Port = settings.AmqpPort, UserName = settings.UserName, Password = password,
        VirtualHost = settings.VirtualHost, ClientProvidedName = "QueueLoom",
        Ssl = new SslOption { Enabled = settings.UseTls, ServerName = settings.Host },
        RequestedConnectionTimeout = TimeSpan.FromSeconds(15)
    };

    private IConnection Connection => _connection ?? throw new InvalidOperationException("Connect to the environment first.");

    protected override async Task OpenAsync(ServiceBusProfile profile, CancellationToken cancellationToken)
    {
        var settings = profile.RabbitMq ?? throw new InvalidOperationException("The RabbitMQ settings are missing.");
        var password = await _secretVault.RetrieveForProfileAsync(profile, ProfileSecretKind.ConnectionString, cancellationToken)
                           .ConfigureAwait(false)
                       ?? throw new InvalidOperationException("The RabbitMQ password is missing. Edit the environment and enter it again.");
        _virtualHost = settings.VirtualHost;

        var management = _httpHandler is null ? new HttpClient() : new HttpClient(_httpHandler, disposeHandler: false);
        management.BaseAddress = settings.ManagementUri;
        management.Timeout = TimeSpan.FromSeconds(30);
        management.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{settings.UserName}:{password}")));
        _management = management;
        try
        {
            // Proves the management plugin, the credentials and access to the virtual host.
            using var response = await management.GetAsync($"api/vhosts/{Escape(_virtualHost)}", cancellationToken).ConfigureAwait(false);
            await EnsureSuccessAsync(response, "the management API").ConfigureAwait(false);

            var factory = CreateAmqpFactory(settings, password);
            _connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (BrokerUnreachableException exception)
        {
            await CloseAsync().ConfigureAwait(false);
            throw new InvalidOperationException(
                $"RabbitMQ at {settings.Host}:{settings.AmqpPort} did not accept the AMQP connection: {exception.GetBaseException().Message}",
                exception);
        }
        catch
        {
            await CloseAsync().ConfigureAwait(false);
            throw;
        }
    }

    protected override async ValueTask CloseAsync()
    {
        var connection = _connection;
        var management = _management;
        _connection = null;
        _management = null;
        _index = RabbitMqTopologyIndex.Empty;
        try
        {
            if (connection is not null)
            {
                try
                {
                    await connection.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OutOfMemoryException)
                {
                    // Closing is best effort (timeouts, socket errors, ...); the connection is disposed regardless.
                }
                finally
                {
                    try { connection.Dispose(); }
                    catch (Exception exception) when (exception is not OutOfMemoryException) { }
                }
            }
        }
        finally
        {
            management?.Dispose();
        }
    }

    protected override async Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken cancellationToken)
    {
        var vhost = Escape(_virtualHost);
        var queues = await GetArrayAsync($"api/queues/{vhost}", cancellationToken).ConfigureAwait(false);
        var exchanges = await GetArrayAsync($"api/exchanges/{vhost}", cancellationToken).ConfigureAwait(false);
        var bindings = await GetArrayAsync($"api/bindings/{vhost}", cancellationToken).ConfigureAwait(false);
        _index = new RabbitMqTopologyIndex(
            queues.Select(RabbitQueueInfo.From),
            exchanges.Select(exchange => new RabbitExchangeInfo(
                exchange.GetProperty("name").GetString() ?? string.Empty,
                exchange.GetProperty("type").GetString() ?? "direct")),
            bindings
                .Where(binding => binding.GetProperty("destination_type").GetString() == "queue")
                .Select(binding => new RabbitBindingInfo(
                    binding.GetProperty("source").GetString() ?? string.Empty,
                    binding.GetProperty("destination").GetString() ?? string.Empty,
                    binding.GetProperty("routing_key").GetString() ?? string.Empty)));
        return _index.ToTopology(TimeProvider.GetUtcNow());
    }

    protected override ILeasedMessageChannel OpenChannel(
        ServiceBusTopology topology,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue)
    {
        if (source.Kind != ServiceBusEntityKind.Queue)
        {
            throw new InvalidOperationException("RabbitMQ keeps messages in queues. Pick a queue.");
        }
        if (subQueue == ServiceBusSubQueue.TransferDeadLetter)
        {
            throw new InvalidOperationException("RabbitMQ has no transfer dead-letter queues.");
        }

        var queue = _index.FindQueue(source.Name)
                    ?? throw new InvalidOperationException($"Queue '{source.Name}' was not found. Refresh and try again.");
        if (subQueue == ServiceBusSubQueue.Active)
        {
            return Readable(queue, source, subQueue, belongsTo: null);
        }

        var deadLetterQueue = _index.DeadLetterQueueOf(queue.Name) is { } name ? _index.FindQueue(name) : null;
        if (deadLetterQueue is null)
        {
            throw new InvalidOperationException(
                $"Queue '{queue.Name}' has no dead-letter queue. Set a dead-letter exchange on it (x-dead-letter-exchange or a policy) " +
                "and bind a queue to that exchange.");
        }
        // Retained dead letters outlive their source queue and its current dead-letter configuration.
        // Current topology cannot prove exclusive ownership of the physical dead-letter queue.
        return Readable(deadLetterQueue, source, subQueue, belongsTo: queue.Name);
    }

    private RabbitChannel Readable(RabbitQueueInfo queue, ServiceBusEntityReference source, ServiceBusSubQueue subQueue, string? belongsTo) =>
        queue.IsStream
            ? throw new InvalidOperationException($"'{queue.Name}' is a stream. Streams are read by offset, which QueueLoom does not support yet.")
            : new RabbitChannel(this, queue.Name, source, subQueue, belongsTo, BrokerOwnedHeaders(queue));

    private IReadOnlySet<string> BrokerOwnedHeaders(RabbitQueueInfo queue) => BrokerOwnedHeaders(queue.IsQuorum, ServerVersion());

    /// <summary>
    /// The headers the broker writes itself on messages read from a queue, as far as it is established: a quorum
    /// queue keeps its delivery count in x-delivery-count, and from RabbitMQ 4.3 its delivery path also counts
    /// acquisitions in x-acquired-count. Classic queues (any version), and queues whose type is not known, pass a
    /// producer's headers through unchanged, so nothing is broker-owned there and every header stays part of the
    /// message's identity.
    /// </summary>
    internal static IReadOnlySet<string> BrokerOwnedHeaders(bool isQuorum, Version? serverVersion)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        if (!isQuorum)
        {
            return names;
        }
        names.Add("x-delivery-count");
        if (serverVersion is { } version && version >= new Version(4, 3))
        {
            names.Add("x-acquired-count");
        }
        return names;
    }

    private Version? ServerVersion()
    {
        try
        {
            return _connection?.ServerProperties is { } properties && properties.TryGetValue("version", out var value) &&
                   (value is byte[] bytes ? System.Text.Encoding.UTF8.GetString(bytes) : value as string) is { } text &&
                   Version.TryParse(new string(text.TakeWhile(character => char.IsDigit(character) || character == '.').ToArray()), out var parsed)
                ? parsed
                : null;
        }
        catch (Exception exception) when (exception is ObjectDisposedException or InvalidOperationException)
        {
            return null;
        }
    }

    protected override async Task SendCoreAsync(
        ServiceBusTopology topology,
        ServiceBusEntityReference destination,
        MessageDraft message,
        CancellationToken cancellationToken)
    {
        if (message.Properties.ScheduledEnqueueTime is not null)
        {
            throw new InvalidOperationException("RabbitMQ cannot schedule messages without a plugin. Clear the scheduled time.");
        }

        var (exchange, routingKey) = destination.Kind switch
        {
            // The default exchange delivers to the queue with the routing key's name.
            ServiceBusEntityKind.Queue => (string.Empty, destination.Name),
            ServiceBusEntityKind.Topic => (destination.Name, message.Properties.Subject ?? string.Empty),
            _ => throw new InvalidOperationException("Send to a queue, or to an exchange with the routing key as Subject.")
        };

        await using var channel = await Connection.CreateChannelAsync(
                new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
                cancellationToken)
            .ConfigureAwait(false);
        try
        {
            await channel.BasicPublishAsync(exchange, routingKey, mandatory: true, RabbitMqMessageMapper.ToAmqp(message),
                    message.Body.GetBytes(), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (PublishException exception) when (exception.IsReturn)
        {
            // One mandatory publish on this channel was explicitly returned as unroutable.
            // Connection loss, cancellation, timeout and a generic nack never enter this proof path.
            throw new DeliveryRejectedException(
                $"Exchange '{exchange}' has no queue bound for routing key '{routingKey}', so the message was not delivered. " +
                "Set the routing key in Subject.", exception);
        }
    }

    private async Task<IReadOnlyList<JsonElement>> GetArrayAsync(string path, CancellationToken cancellationToken)
    {
        var management = _management ?? throw new InvalidOperationException("Connect to the environment first.");
        using var response = await management.GetAsync(path, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, "the management API").ConfigureAwait(false);
        await using var content = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using var document = await JsonDocument.ParseAsync(content, cancellationToken: cancellationToken).ConfigureAwait(false);
        return document.RootElement.EnumerateArray().Select(item => item.Clone()).ToArray();
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, string what)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        throw response.StatusCode switch
        {
            HttpStatusCode.Unauthorized => new InvalidOperationException(
                "RabbitMQ rejected the user name or password, or the user has no management tag (monitoring is enough)."),
            HttpStatusCode.NotFound => new InvalidOperationException(
                "The virtual host was not found, or the user has no permissions on it."),
            _ => new InvalidOperationException(
                $"RabbitMQ {what} answered {(int)response.StatusCode} {response.ReasonPhrase}: " +
                $"{(await response.Content.ReadAsStringAsync().ConfigureAwait(false)).Trim()}")
        };
    }

    private static string Escape(string virtualHost) => Uri.EscapeDataString(virtualHost);

    /// <summary>
    /// Reads one queue with basic.get on its own AMQP channel. Held messages stay unacknowledged; releasing them
    /// requeues them in place. The channel closes at operation cleanup, returning any deliveries not yet released.
    /// </summary>
    private sealed class RabbitChannel(
        RabbitMqWorkspace owner,
        string queue,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        string? belongsTo,
        IReadOnlySet<string>? brokerOwnedHeaders = null) : ILeasedMessageChannel, IAsyncDisposable
    {
        private readonly HashSet<ulong> _held = [];
        private IChannel? _channel;
        private readonly string _identity = Guid.NewGuid().ToString("N");

        public string PhysicalName => queue;

        public int MaximumBatchSize => MaximumBatch;

        public async Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken)
        {
            _channel ??= await owner.Connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            var messages = new List<LeasedMessage>();
            while (messages.Count < Math.Clamp(maxMessages, 1, MaximumBatch))
            {
                var result = await _channel.BasicGetAsync(queue, autoAck: false, cancellationToken).ConfigureAwait(false);
                if (result is null)
                {
                    break;
                }

                _held.Add(result.DeliveryTag);
                // Ownership must be known before deriving the no-ID selection key, as well as the fingerprint.
                var message = RabbitMqMessageMapper.FromAmqp(result.Body, result.BasicProperties, result.RoutingKey, source, subQueue, brokerOwnedHeaders);
                var belongs = belongsTo is null || RabbitMqMessageMapper.DeadLetteredFrom(result.BasicProperties) == belongsTo;
                messages.Add(new LeasedMessage(message, result.DeliveryTag.ToString(System.Globalization.CultureInfo.InvariantCulture), belongs)
                    { DeliveryIdentity = $"{_identity}:{result.DeliveryTag}" });
            }
            return messages;
        }

        public async Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken)
        {
            if (_channel is null)
            {
                return;
            }
            foreach (var tag in Tags(messages))
            {
                if (!_held.Contains(tag)) continue;
                // The message comes back with redelivered set. Quorum queues count this requeue toward their
                // delivery-limit up to RabbitMQ 4.2 ("every requeued message incremented its delivery-count by 1,
                // regardless of the reason"); from 4.3 basic.nack raises only acquired-count, which the limit ignores
                // (RabbitMQ blog, "RabbitMQ 4.3 release"; docs, Quorum Queues, poison message handling).
                await _channel.BasicNackAsync(tag, multiple: false, requeue: true, cancellationToken).ConfigureAwait(false);
                _held.Remove(tag);
            }
        }

        public async Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken)
        {
            var failed = new List<LeasedMessage>();
            foreach (var message in messages)
            {
                var tag = ulong.Parse(message.LeaseHandle, System.Globalization.CultureInfo.InvariantCulture);
                try
                {
                    await _channel!.BasicAckAsync(tag, multiple: false, cancellationToken).ConfigureAwait(false);
                    _held.Remove(tag);
                }
                catch (Exception exception) when (exception is AlreadyClosedException or OperationInterruptedException)
                {
                    failed.Add(message);
                }
            }
            return failed;
        }

        private static IEnumerable<ulong> Tags(IEnumerable<LeasedMessage> messages) =>
            messages.Select(message => ulong.Parse(message.LeaseHandle, System.Globalization.CultureInfo.InvariantCulture));

        public async ValueTask DisposeAsync()
        {
            if (_channel is null)
            {
                return;
            }
            var channel = _channel;
            _channel = null;
            try
            {
                await channel.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is AlreadyClosedException or OperationInterruptedException)
            {
            }
            finally
            {
                _held.Clear();
                try { channel.Dispose(); }
                catch (Exception exception) when (exception is not OutOfMemoryException) { }
            }
        }
    }
}
