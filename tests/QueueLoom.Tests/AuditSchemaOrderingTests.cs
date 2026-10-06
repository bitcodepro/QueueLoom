using QueueLoom.Core.ServiceBus;
using QueueLoom.App.ViewModels;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Audit_SlowStartupSchemaCannotOverwriteExplicitSelection(bool startupFails)
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var a = ProtoSchemaSet.FromProtoFiles([("a.proto", "message Startup { string old_name = 1; }")]);
        var b = ProtoSchemaSet.FromProtoFiles([("b.proto", "message Selected { string chosen_name = 1; }")]);
        vm.ProtobufSchemaLoader = path =>
        {
            if (path == "B") return b;
            started.TrySetResult();
            release.Task.GetAwaiter().GetResult();
            if (startupFails) throw new IOException("startup failed");
            return a;
        };
        // Observe the startup task itself, rather than guessing when its continuation has finished.
        var startup = vm.LoadProtobufSchemasAsync("A");
        var message = new MessageItemViewModel(new BrowsedMessage(ServiceBusEntityReference.Queue("orders"),
            ServiceBusSubQueue.Active, 1, new byte[] { 10, 3, 97, 98, 99 }, new EditableMessageProperties(ContentType: "application/x-protobuf")));
        vm.Messages.Add(message);
        _ = message.DecodedText;
        var persistedPath = "";
        vm.PropertyChanged += (_, args) =>
        {
            if (args.PropertyName == nameof(vm.ProtobufSchemaPath)) persistedPath = vm.ProtobufSchemaPath;
        };
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await vm.LoadProtobufSchemasAsync("B");
            var selectedStatus = vm.ProtobufSchemaStatus;
            Assert.Contains("chosen_name", message.DecodedText, StringComparison.Ordinal);
            release.TrySetResult();
            await startup;
            Assert.Same(b, ProtoSchemaCatalog.Current);
            Assert.Equal("B", vm.ProtobufSchemaPath);
            Assert.Equal("B", persistedPath);
            Assert.Equal(selectedStatus, vm.ProtobufSchemaStatus);
            Assert.Contains("chosen_name", message.DecodedText, StringComparison.Ordinal);
        }
        finally { release.TrySetResult(); await startup; vm.ClearProtobufSchemasCommand.Execute(null); }
    }

    [Theory]
    [InlineData("clear")]
    [InlineData("cancel")]
    [InlineData("dispose")]
    public async Task Audit_SlowSchemaLoadCannotPublishAfterClearCancellationOrClose(string action)
    {
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace());
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var baseline = ProtoSchemaSet.FromProtoFiles([("b.proto", "message Previous { string value = 1; }")]);
        vm.ProtobufSchemaLoader = _ => baseline;
        await vm.LoadProtobufSchemasAsync("previous");
        vm.ProtobufSchemaLoader = _ =>
        {
            started.TrySetResult(); release.Task.GetAwaiter().GetResult();
            return ProtoSchemaSet.FromProtoFiles([("a.proto", "message Late { string value = 1; }")]);
        };
        using var stop = new CancellationTokenSource();
        var pending = vm.LoadProtobufSchemasAsync("late", stop.Token);
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
            if (action == "clear") vm.ClearProtobufSchemasCommand.Execute(null);
            else if (action == "cancel") stop.Cancel();
            else await vm.DisposeAsync();
            var status = vm.ProtobufSchemaStatus;
            release.TrySetResult();
            await pending;
            Assert.Same(action == "clear" ? ProtoSchemaSet.Empty : baseline, ProtoSchemaCatalog.Current);
            Assert.Equal(action == "clear" ? "" : "previous", vm.ProtobufSchemaPath);
            Assert.Equal(status, vm.ProtobufSchemaStatus);
        }
        finally { release.TrySetResult(); await pending; ProtoSchemaCatalog.Current = ProtoSchemaSet.Empty; }
    }
}
