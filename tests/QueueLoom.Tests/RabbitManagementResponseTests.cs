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
    // past the client's timeout. The whole request, body included, keeps that deadline. The deadline is fired by the
    // test, after the body read has begun, so the sequence does not depend on the runner's speed.
    [Fact]
    public async Task ASuccessfulResponseWhoseBodyStallsStillEndsAtTheClientTimeout()
    {
        var stream = new StalledStream();
        var clock = new ManualTimers();
        var reading = ReadTopologyAsync(new Recording(HttpStatusCode.OK, stream), clock);
        try
        {
            await stream.Entered.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.False(reading.IsCompleted);
            clock.FireAll();

            var error = await Assert.ThrowsAsync<TimeoutException>(() => reading.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Contains("did not answer within", error.Message, StringComparison.Ordinal);
        }
        finally
        {
            stream.Stop();
            await reading.ContinueWith(_ => { }, TaskScheduler.Default);
        }
    }

    // The error text had only its own ten-second deadline: cancelling during a stalled error body waited for it and
    // ended as an error instead of a cancellation. The operation is cancelled once the error body is being read.
    [Fact]
    public async Task CancellingWhileAnErrorBodyStallsEndsAsACancellation()
    {
        var stream = new StalledStream();
        using var cancellation = new CancellationTokenSource();
        var reading = ReadTopologyAsync(new Recording(HttpStatusCode.BadGateway, stream), cancellation: cancellation.Token);
        try
        {
            await stream.Entered.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.False(reading.IsCompleted);
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reading.WaitAsync(TimeSpan.FromSeconds(30)));
        }
        finally
        {
            stream.Stop();
            await reading.ContinueWith(_ => { }, TaskScheduler.Default);
        }
    }

    /// <summary>A body whose read never returns data: it ends when the reader cancels or the test stops it.</summary>
    private sealed class StalledStream : Stream
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationTokenSource _stop = new();
        public Task Entered => _entered.Task;
        public void Stop() => _stop.Cancel();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException("Read asynchronously.");
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _entered.TrySetResult();
            using var either = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stop.Token);
            await Task.Delay(Timeout.Infinite, either.Token);
            return 0;
        }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>A clock whose timers fire only when the test says so.</summary>
    private sealed class ManualTimers : TimeProvider
    {
        private readonly List<Timer> _timers = [];
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new Timer(callback, state);
            lock (_timers) _timers.Add(timer);
            return timer;
        }
        public void FireAll()
        {
            Timer[] timers;
            lock (_timers) timers = [.. _timers];
            foreach (var timer in timers) timer.Fire();
        }
        private sealed class Timer(TimerCallback callback, object? state) : ITimer
        {
            private bool _disposed;
            public void Fire() { if (!_disposed) callback(state); }
            public bool Change(TimeSpan dueTime, TimeSpan period) => !_disposed;
            public void Dispose() => _disposed = true;
            public ValueTask DisposeAsync() { _disposed = true; return ValueTask.CompletedTask; }
        }
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

    private static async Task ReadTopologyAsync(HttpMessageHandler handler, TimeProvider? clock = null, CancellationToken cancellation = default)
    {
        await using var workspace = new RabbitMqWorkspace(new DeepAuditCloudTests.EmptyVault(), clock);
        typeof(RabbitMqWorkspace).GetField("_management", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(workspace, new HttpClient(handler) { BaseAddress = new Uri("http://broker.invalid/") });
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
