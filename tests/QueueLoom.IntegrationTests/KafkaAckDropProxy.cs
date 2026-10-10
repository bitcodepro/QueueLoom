using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace QueueLoom.IntegrationTests;

/// <summary>
/// A TCP proxy in front of one Kafka broker that understands just enough of the protocol to lose an acknowledgement:
/// it forwards a produce request to the broker, which writes the record, and then closes the client's connection
/// instead of passing the response on. The broker's own address in metadata answers is rewritten to the proxy, so the
/// client reconnects (and retries) through it.
/// </summary>
internal sealed class KafkaAckDropProxy : IAsyncDisposable
{
    private const short ProduceApi = 0;
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly string _upstreamHost;
    private readonly int _upstreamPort;
    private readonly CancellationTokenSource _stopping = new();
    private readonly List<Task> _connections = [];
    private readonly Task _accepting;
    private int _dropNextProduceResponse;

    public KafkaAckDropProxy(string upstream)
    {
        var separator = upstream.LastIndexOf(':');
        _upstreamHost = upstream[..separator];
        _upstreamPort = int.Parse(upstream[(separator + 1)..], System.Globalization.CultureInfo.InvariantCulture);
        // The advertised host is replaced in place, so the replacement must have the same length.
        if (Encoding.ASCII.GetByteCount(_upstreamHost) != Encoding.ASCII.GetByteCount(ProxyHost))
            throw new InvalidOperationException($"The broker must advertise a 9-character host such as localhost, not '{_upstreamHost}'.");
        _listener.Start();
        _accepting = AcceptAsync();
    }

    private const string ProxyHost = "127.0.0.1";

    public string BootstrapServers => $"{ProxyHost}:{((IPEndPoint)_listener.LocalEndpoint).Port}";

    /// <summary>The broker still receives and applies the next produce request; its response is never delivered.</summary>
    public void DropNextProduceResponse() => Interlocked.Exchange(ref _dropNextProduceResponse, 1);

    public bool ProduceResponseDropped => Volatile.Read(ref _dropNextProduceResponse) == 2;

    private async Task AcceptAsync()
    {
        while (!_stopping.IsCancellationRequested)
        {
            TcpClient client;
            try { client = await _listener.AcceptTcpClientAsync(_stopping.Token); }
            catch (Exception) when (_stopping.IsCancellationRequested) { return; }
            lock (_connections) _connections.Add(RelayAsync(client));
        }
    }

    private async Task RelayAsync(TcpClient client)
    {
        using var _ = client;
        using var upstream = new TcpClient();
        try
        {
            await upstream.ConnectAsync(_upstreamHost, _upstreamPort, _stopping.Token);
            var apiKeys = new ConcurrentDictionary<int, short>();
            using var closing = CancellationTokenSource.CreateLinkedTokenSource(_stopping.Token);
            var requests = PumpAsync(client.GetStream(), upstream.GetStream(), frame =>
            {
                // Request header: api_key int16, api_version int16, correlation_id int32.
                apiKeys[BinaryPrimitives.ReadInt32BigEndian(frame.AsSpan(4))] = BinaryPrimitives.ReadInt16BigEndian(frame);
                return frame;
            }, closing.Token);
            var responses = PumpAsync(upstream.GetStream(), client.GetStream(), frame =>
            {
                apiKeys.TryRemove(BinaryPrimitives.ReadInt32BigEndian(frame), out var apiKey);
                if (apiKey == ProduceApi && Interlocked.CompareExchange(ref _dropNextProduceResponse, 2, 1) == 1) return null;
                return apiKey == ProduceApi ? frame : RewriteBrokerAddress(frame);
            }, closing.Token);
            await Task.WhenAny(requests, responses);
            await closing.CancelAsync();
        }
        catch (Exception) when (_stopping.IsCancellationRequested) { }
        catch (IOException) { }
        catch (SocketException) { }
    }

    // Copies whole frames; a null from the transform closes the connection without sending that frame.
    private static async Task PumpAsync(Stream from, Stream to, Func<byte[], byte[]?> transform, CancellationToken cancellationToken)
    {
        var size = new byte[4];
        try
        {
            while (true)
            {
                await from.ReadExactlyAsync(size, cancellationToken);
                var frame = new byte[BinaryPrimitives.ReadInt32BigEndian(size)];
                await from.ReadExactlyAsync(frame, cancellationToken);
                var forwarded = transform(frame);
                if (forwarded is null) return;
                await to.WriteAsync(size, cancellationToken);
                await to.WriteAsync(forwarded, cancellationToken);
            }
        }
        catch (Exception exception) when (exception is IOException or EndOfStreamException or OperationCanceledException or ObjectDisposedException) { }
    }

    // The broker's host followed by its port (int32), as metadata and coordinator answers carry it, becomes the proxy's.
    private byte[] RewriteBrokerAddress(byte[] frame)
    {
        var pattern = new byte[_upstreamHost.Length + 4];
        Encoding.ASCII.GetBytes(_upstreamHost, pattern);
        BinaryPrimitives.WriteInt32BigEndian(pattern.AsSpan(_upstreamHost.Length), _upstreamPort);
        var replacement = new byte[pattern.Length];
        Encoding.ASCII.GetBytes(ProxyHost, replacement);
        BinaryPrimitives.WriteInt32BigEndian(replacement.AsSpan(ProxyHost.Length), ((IPEndPoint)_listener.LocalEndpoint).Port);
        for (var index = frame.AsSpan().IndexOf(pattern); index >= 0;)
        {
            replacement.CopyTo(frame, index);
            var next = frame.AsSpan(index + pattern.Length).IndexOf(pattern);
            index = next < 0 ? -1 : index + pattern.Length + next;
        }
        return frame;
    }

    public async ValueTask DisposeAsync()
    {
        await _stopping.CancelAsync();
        _listener.Stop();
        await _accepting;
        Task[] connections;
        lock (_connections) connections = [.. _connections];
        await Task.WhenAll(connections).WaitAsync(TimeSpan.FromSeconds(10));
        _stopping.Dispose();
    }
}
