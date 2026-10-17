using System.Net;
using System.Reflection;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

public sealed class RabbitManagementResponseTests
{
    // The queue list asks only for the fields QueueLoom reads; without a column list the management API returns every
    // queue's full statistics, which on thousands of queues is heavy for the broker and for each refresh.
    [Fact]
    public async Task TheQueueListAsksOnlyForTheColumnsItUses()
    {
        var handler = new Recording(HttpStatusCode.OK, "[]");

        await ReadTopologyAsync(handler);

        var queues = Assert.Single(handler.Requests, uri => uri.AbsolutePath.StartsWith("/api/queues/", StringComparison.Ordinal));
        var columns = System.Web.HttpUtility.ParseQueryString(queues.Query)["columns"]!.Split(',');
        Assert.Equal(["arguments", "consumers", "effective_policy_definition", "messages_ready", "messages_unacknowledged", "name", "state", "type"],
            columns.Order());
    }

    // A proxy in front of the management API answers with a large HTML page: the error says what happened in one
    // bounded line instead of carrying the whole page into the status line and Activity.
    [Fact]
    public async Task AnErrorPageIsCutToOneShortLine()
    {
        var page = "<html>\n<body>\n" + string.Concat(Enumerable.Repeat("<p>Bad gateway</p>\n", 50_000)) + "</body></html>";
        var handler = new Recording(HttpStatusCode.BadGateway, page);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ReadTopologyAsync(handler));

        Assert.StartsWith("RabbitMQ the management API answered 502", error.Message, StringComparison.Ordinal);
        Assert.True(error.Message.Length < 1_200, $"{error.Message.Length} characters.");
        Assert.DoesNotContain('\n', error.Message);
        Assert.EndsWith("…", error.Message, StringComparison.Ordinal);
    }

    // Exchanges came with their message rates and bindings with every field: only what the topology reads is asked for.
    [Fact]
    public async Task ExchangesAndBindingsAskOnlyForTheColumnsTheyUse()
    {
        var handler = new Recording(HttpStatusCode.OK, "[]");

        await ReadTopologyAsync(handler);

        string[] Columns(string path) => System.Web.HttpUtility.ParseQueryString(Assert.Single(handler.Requests,
            uri => uri.AbsolutePath.StartsWith(path, StringComparison.Ordinal)).Query)["columns"]!.Split(',').Order().ToArray();
        Assert.Equal(["name", "type"], Columns("/api/exchanges/"));
        Assert.Equal(["destination", "destination_type", "routing_key", "source"], Columns("/api/bindings/"));
    }

    // The error text was read whole before it was cut: an endless error body (a misbehaving proxy) was buffered
    // without end. Only the start is read now.
    [Fact]
    public async Task AnEndlessErrorBodyIsReadOnlyAsFarAsItIsShown()
    {
        var stream = new EndlessStream();
        var handler = new Recording(HttpStatusCode.BadGateway, stream);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => ReadTopologyAsync(handler).WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.StartsWith("RabbitMQ the management API answered 502", error.Message, StringComparison.Ordinal);
        Assert.EndsWith("…", error.Message, StringComparison.Ordinal);
        Assert.True(stream.Served < 1024 * 1024, $"{stream.Served:N0} bytes were read.");
    }

    // With headers read first, HttpClient.Timeout no longer covered the body: a 200 whose body stalls hung the refresh
    // past the client's timeout. The whole request, body included, keeps that deadline.
    [Fact]
    public async Task ASuccessfulResponseWhoseBodyStallsStillEndsAtTheClientTimeout()
    {
        var handler = new Recording(HttpStatusCode.OK, new StalledStream());
        var elapsed = System.Diagnostics.Stopwatch.StartNew();

        var error = await Assert.ThrowsAsync<TimeoutException>(() =>
            ReadTopologyAsync(handler, timeout: TimeSpan.FromSeconds(1)).WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.Contains("did not answer within", error.Message, StringComparison.Ordinal);
        Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(15), $"{elapsed.Elapsed} elapsed.");
    }

    // The error text had only its own ten-second deadline: cancelling during a stalled error body waited for it and
    // ended as an error instead of a cancellation.
    [Fact]
    public async Task CancellingWhileAnErrorBodyStallsEndsAsACancellation()
    {
        var handler = new Recording(HttpStatusCode.BadGateway, new StalledStream());
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var elapsed = System.Diagnostics.Stopwatch.StartNew();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ReadTopologyAsync(handler, cancellation: cancellation.Token).WaitAsync(TimeSpan.FromSeconds(30)));

        Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(5), $"{elapsed.Elapsed} elapsed.");
    }

    private sealed class StalledStream : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException("Read asynchronously.");
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return 0;
        }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class EndlessStream : Stream
    {
        public long Served { get; private set; }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => Served; set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count)
        {
            buffer.AsSpan(offset, count).Fill((byte)'x');
            Served += count;
            return count;
        }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private static async Task ReadTopologyAsync(HttpMessageHandler handler, TimeSpan? timeout = null, CancellationToken cancellation = default)
    {
        await using var workspace = new RabbitMqWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(RabbitMqWorkspace).GetField("_management", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(workspace, new HttpClient(handler) { BaseAddress = new Uri("http://broker.invalid/"), Timeout = timeout ?? TimeSpan.FromSeconds(100) });
        var read = typeof(RabbitMqWorkspace).GetMethod("ReadTopologyAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<ServiceBusTopology>)read.Invoke(workspace, [cancellation])!;
    }

    private sealed class Recording : HttpMessageHandler
    {
        private readonly HttpStatusCode _status;
        private readonly Func<HttpContent> _content;
        public Recording(HttpStatusCode status, string body) => (_status, _content) = (status, () => new StringContent(body));
        public Recording(HttpStatusCode status, Stream body) => (_status, _content) = (status, () => new StreamContent(body));
        public List<Uri> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request.RequestUri!);
            return Task.FromResult(new HttpResponseMessage(_status) { Content = _content() });
        }
    }
}
