using QueueLoom.App.Commands;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>.proto files or descriptor sets that give Protobuf bodies their field names.</summary>
public sealed partial class MainWindowViewModel
{
    private string _protobufSchemaPath = string.Empty;
    private string _protobufSchemaStatus = "No .proto files loaded: Protobuf fields are shown by number.";

    internal Func<string, ProtoSchemaSet> ProtobufSchemaLoader { get; set; } = ProtoSchemaSet.Load;
    private long _protobufLoadRequest;

    /// <summary>The .proto file, descriptor set or folder last loaded; persisted by the shell.</summary>
    public string ProtobufSchemaPath
    {
        get => _protobufSchemaPath;
        private set => SetProperty(ref _protobufSchemaPath, value ?? string.Empty);
    }

    public string ProtobufSchemaStatus
    {
        get => _protobufSchemaStatus;
        private set => SetProperty(ref _protobufSchemaStatus, value);
    }

    public AsyncRelayCommand LoadProtobufSchemasCommand { get; private set; } = null!;

    public RelayCommand ClearProtobufSchemasCommand { get; private set; } = null!;

    private void InitializeProtobuf()
    {
        LoadProtobufSchemasCommand = _commands.Create(ChooseProtobufSchemasAsync, () => !IsBusy);
        ClearProtobufSchemasCommand = new RelayCommand(() =>
        {
            Interlocked.Increment(ref _protobufLoadRequest);
            ProtoSchemaCatalog.Current = ProtoSchemaSet.Empty;
            ProtobufSchemaPath = string.Empty;
            ProtobufSchemaStatus = "No .proto files loaded: Protobuf fields are shown by number.";
            RefreshDecodedBodies();
        });
    }

    /// <summary>
    /// A picked .proto file loads every .proto of its folder and subfolders, so imports resolve; a descriptor set
    /// (protoc --descriptor_set_out) is loaded on its own.
    /// </summary>
    private async Task ChooseProtobufSchemasAsync(CancellationToken cancellationToken)
    {
        var file = await _dialogs.ChooseOpenFileAsync("Load Protobuf schemas",
            [("Protobuf schemas", "*.proto;*.desc;*.pb;*.protoset;*.binpb"), ("All files", "*")], cancellationToken).ConfigureAwait(true);
        if (string.IsNullOrWhiteSpace(file))
        {
            return;
        }
        var path = Path.GetExtension(file).Equals(".proto", StringComparison.OrdinalIgnoreCase)
            ? Path.GetDirectoryName(file) ?? file
            : file;
        await LoadProtobufSchemasAsync(path, cancellationToken).ConfigureAwait(true);
    }

    /// <summary>Loads the schemas at <paramref name="path"/>; on failure the ones loaded before stay.</summary>
    public async Task LoadProtobufSchemasAsync(string path, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return;
        }
        var request = Interlocked.Increment(ref _protobufLoadRequest);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCancellation.Token);
        try
        {
            var schemas = await Task.Run(() => ProtobufSchemaLoader(path), cancellation.Token).ConfigureAwait(true);
            if (request != Volatile.Read(ref _protobufLoadRequest) || cancellation.IsCancellationRequested || _isDisposed) return;
            ProtoSchemaCatalog.Current = schemas;
            ProtobufSchemaPath = path;
            ProtobufSchemaStatus = $"{schemas.Messages.Count:N0} message type(s) from {schemas.Sources.Count:N0} file(s) in {path}";
            StatusText = $"Loaded {schemas.Messages.Count:N0} Protobuf message type(s)";
            RefreshDecodedBodies();
        }
        catch (Exception exception) when (exception is ProtoSchemaException or IOException or UnauthorizedAccessException)
        {
            if (request != Volatile.Read(ref _protobufLoadRequest) || cancellation.IsCancellationRequested || _isDisposed) return;
            ProtobufSchemaStatus = $"Could not load {path}: {exception.Message}";
            StatusText = ProtobufSchemaStatus;
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
    }

    private void RefreshDecodedBodies()
    {
        foreach (var message in Messages)
        {
            message.RefreshDecoded();
        }
    }
}
