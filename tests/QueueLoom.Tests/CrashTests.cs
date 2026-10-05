using System.Collections;
using System.Reflection;
using QueueLoom.App.Commands;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

/// <summary>Faults and hangs that used to terminate or freeze the desktop application.</summary>
public sealed class CrashTests
{
    /// <summary>Records what async void posts back, the way Avalonia's dispatcher would receive it.</summary>
    private sealed class RecordingContext : SynchronizationContext
    {
        private readonly Queue<(SendOrPostCallback Callback, object? State)> _queue = new();
        public List<Exception> Crashes { get; } = [];

        public override void Post(SendOrPostCallback d, object? state) { lock (_queue) _queue.Enqueue((d, state)); }

        public void Drain()
        {
            while (true)
            {
                (SendOrPostCallback Callback, object? State) item;
                lock (_queue) { if (!_queue.TryDequeue(out item)) return; }
                try { item.Callback(item.State); }
                catch (Exception error) { Crashes.Add(error); }
            }
        }
    }

    private static List<Exception> RunExecute(Action execute, Func<Task> completion)
    {
        var previous = SynchronizationContext.Current;
        var context = new RecordingContext();
        SynchronizationContext.SetSynchronizationContext(context);
        try
        {
            execute();
            for (var round = 0; round < 1000 && (!completion().IsCompleted || round < 3); round++) context.Drain();
            context.Drain();
        }
        finally { SynchronizationContext.SetSynchronizationContext(previous); }
        return context.Crashes;
    }

    [Fact]
    public void AFailingCommandClickedInTheUiDoesNotCrashTheDispatcher()
    {
        var command = new AsyncRelayCommand(_ => throw new InvalidOperationException("broker said no"));
        var crashes = RunExecute(() => command.Execute(null), () => command.Completion);
        Assert.Empty(crashes);
        Assert.True(command.Completion.IsFaulted);
    }

    [Fact]
    public void AFailingRowCommandClickedInTheUiDoesNotCrashTheDispatcher()
    {
        var command = new AsyncRelayCommand<string>((_, _) => Task.FromException(new InvalidOperationException("rule gone")));
        var crashes = RunExecute(() => command.Execute("rule"), () => command.Completion);
        Assert.Empty(crashes);
        Assert.True(command.Completion.IsFaulted);
    }

    [Fact]
    public void RegistryProtobufSchemasDoNotAccumulateForTheLifeOfTheProcess()
    {
        byte[] body = [0, 0, 0, 0, 1, 0, 0x08, 0x01];
        for (var index = 0; index < 1000; index++)
        {
            var schema = new MessageSchema(1, MessageSchemaType.Protobuf, $"syntax = \"proto3\"; message M{index} {{ int32 a = 1; }}");
            BodyDecoder.Decode(body, schema: schema);
        }

        var cache = (ICollection)typeof(BodyDecoder).GetField("RegistryProtos", BindingFlags.NonPublic | BindingFlags.Static)!.GetValue(null)!;
        Assert.InRange(cache.Count, 1, 256);
    }
}
