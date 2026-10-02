using System.IO.Compression;
using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("Connecting")]
    [InlineData("Updating environment")]
    [InlineData("Deleting environment")]
    public async Task Diagnostics_ProfileOperationTargetsSelectedBrokerWithoutConnectedEntity(string operation)
    {
        var a = CreateProfile("PRIVATE_A", EnvironmentKind.Test);
        var b = CreateProfile("PRIVATE_B", EnvironmentKind.Test) with { Provider = MessagingProvider.RabbitMq };
        var workspace = new FakeWorkspace();
        var dialogs = new DiagnosticDialogs { Confirm = false };
        await using var vm = CreateViewModel(new FakeProfileRepository([a, b], a.Id), workspace, dialogs);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        vm.SelectedEntity = new EntityItemViewModel(ServiceBusEntityReference.Queue("PRIVATE_A_QUEUE"), ServiceBusEntityRuntime.Empty, ServiceBusEntityStatus.Active, false, 0);
        vm.SelectedProfile = vm.Profiles.Single(p => p.Id == b.Id);
        await vm.RefreshTopologyCommand.ExecuteAsync();
        workspace.FailNextConnection = true;
        if (operation == "Connecting") await vm.ConnectCommand.ExecuteAsync();
        else if (operation == "Updating environment") await vm.EditEnvironmentCommand.ExecuteAsync();
        else await vm.DeleteEnvironmentCommand.ExecuteAsync();
        using var json = JsonDocument.Parse(vm.Diagnostics.Capture().Json);
        var events = json.RootElement.GetProperty("Events").EnumerateArray().ToArray();
        var latest = events.Last(e => e.GetProperty("Kind").GetString() == operation);
        var refresh = events.Last(e => e.GetProperty("Kind").GetString() == "Refreshing topology");
        Assert.Equal("AzureServiceBus", refresh.GetProperty("Broker").GetString());
        Assert.Equal(events.First(e => e.GetProperty("Kind").GetString() == "Connecting").GetProperty("Address").GetString(),
            refresh.GetProperty("Address").GetString());
        Assert.Equal("RabbitMq", latest.GetProperty("Broker").GetString());
        Assert.Equal(JsonValueKind.Null, latest.GetProperty("Entity").ValueKind);
        Assert.NotEqual(events.First(e => e.GetProperty("Kind").GetString() == "Connecting").GetProperty("Address").GetString(),
            latest.GetProperty("Address").GetString());
        if (operation == "Connecting") Assert.Equal("Failed", latest.GetProperty("Stage").GetString());
    }

    [Fact]
    public async Task Diagnostics_ProfileTargetRemainsStableWhileSelectionChangesDuringConnection()
    {
        var a = CreateProfile("PRIVATE_A", EnvironmentKind.Test);
        var b = CreateProfile("PRIVATE_B", EnvironmentKind.Test) with { Provider = MessagingProvider.RabbitMq };
        var workspace = new FakeWorkspace { ConnectionRelease = new(), FailNextConnection = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([a, b], b.Id), workspace, new DiagnosticDialogs());
        await vm.InitializeAsync();
        var connecting = vm.ConnectCommand.ExecuteAsync();
        Assert.Equal(1, workspace.ConnectCalls);
        vm.SelectedProfile = vm.Profiles.Single(p => p.Id == a.Id);
        workspace.ConnectionRelease.SetResult(true); await connecting;
        using var json = JsonDocument.Parse(vm.Diagnostics.Capture().Json);
        var latest = json.RootElement.GetProperty("Events").EnumerateArray().Last(e => e.GetProperty("Kind").GetString() == "Connecting");
        Assert.Equal("RabbitMq", latest.GetProperty("Broker").GetString());
        Assert.Equal("Failed", latest.GetProperty("Stage").GetString());
    }

    [Fact]
    public async Task Diagnostics_ProfilesWithoutEndpointDoNotHashPresentationFallback()
    {
        var a = CreateProfile("PRIVATE_A", EnvironmentKind.Test) with { FullyQualifiedNamespace = null };
        var b = a with { Id = Guid.NewGuid(), Name = "PRIVATE_B" };
        var workspace = new FakeWorkspace { FailNextConnection = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([a, b], a.Id), workspace, new DiagnosticDialogs());
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        vm.SelectedProfile = vm.Profiles.Single(p => p.Id == b.Id);
        workspace.FailNextConnection = true; await vm.ConnectCommand.ExecuteAsync();
        using var json = JsonDocument.Parse(vm.Diagnostics.Capture().Json);
        var events = json.RootElement.GetProperty("Events").EnumerateArray().Where(e => e.GetProperty("Kind").GetString() == "Connecting").ToArray();
        Assert.Equal(2, events.Select(e => e.GetProperty("Operation").GetString()).Distinct().Count());
        Assert.All(events, e => Assert.Equal(JsonValueKind.Null, e.GetProperty("Address").ValueKind));
    }

    [Fact]
    public async Task Diagnostics_RepeatedClicksFreezeOnePreview_AndExportOnlyChosenNewFile()
    {
        using var directory = new TemporaryDirectory();
        var dialogs = new DiagnosticDialogs { Destination = Path.Combine(directory.Path, "chosen.zip"), Gate = new() };
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace(), dialogs);
        vm.DraftBody = "BODY_SENTINEL"; vm.DraftApplicationProperties = "HEADER_SENTINEL";
        vm.Activity.Add(new(DateTimeOffset.UtcNow, "Error", "TOKEN_SENTINEL", "ACTIVITY_SENTINEL", ServiceBusEntityReference.Queue("QUEUE_SENTINEL")));
        vm.Diagnostics.Begin("Connecting", address: "ADDRESS_SENTINEL", entity: "QUEUE_SENTINEL");
        var first = vm.ExportDiagnosticsCommand.ExecuteAsync();
        Assert.False(vm.ExportDiagnosticsCommand.CanExecute(null));
        await vm.ExportDiagnosticsCommand.ExecuteAsync();
        vm.Diagnostics.Begin("Disconnecting");
        dialogs.Gate.SetResult(); await first;
        Assert.Equal(1, dialogs.Previews); Assert.Equal(1, dialogs.Pickers);
        Assert.Single(Directory.GetFiles(directory.Path));
        using var zip = ZipFile.OpenRead(dialogs.Destination);
        using var reader = new StreamReader(zip.GetEntry("diagnostics.json")!.Open());
        var json = await reader.ReadToEndAsync();
        Assert.Equal(dialogs.Preview!.Json, json);
        foreach (var sentinel in new[] { "BODY_SENTINEL", "HEADER_SENTINEL", "TOKEN_SENTINEL", "ACTIVITY_SENTINEL", "QUEUE_SENTINEL", "ADDRESS_SENTINEL" })
            Assert.DoesNotContain(sentinel, json, StringComparison.Ordinal);
        Assert.DoesNotContain("Disconnecting", json, StringComparison.Ordinal);
        Assert.True(vm.ExportDiagnosticsCommand.CanExecute(null));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Diagnostics_PreviewDismissalSaveDismissalAndCancellation_WriteNothing(bool approved, bool cancel)
    {
        using var directory = new TemporaryDirectory();
        var dialogs = new DiagnosticDialogs { Approve = approved, Destination = cancel ? Path.Combine(directory.Path, "cancel.zip") : null,
            Gate = cancel ? new() : null };
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace(), dialogs);
        var export = vm.ExportDiagnosticsCommand.ExecuteAsync();
        if (cancel) vm.ExportDiagnosticsCommand.Cancel();
        await export;
        Assert.Empty(Directory.GetFiles(directory.Path));
        Assert.Equal(approved && !cancel ? 1 : 0, dialogs.Pickers);
        Assert.True(vm.ExportDiagnosticsCommand.CanExecute(null));
    }

    [Fact]
    public async Task Diagnostics_SaveFailurePreservesFilesAndShowsOnlySafeGuidance()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "PERSONAL_PATH_SECRET.zip"); File.WriteAllText(path, "unrelated");
        var dialogs = new DiagnosticDialogs { Destination = path };
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace(), dialogs);
        await vm.ExportDiagnosticsCommand.ExecuteAsync();
        Assert.Equal("unrelated", File.ReadAllText(path));
        Assert.Single(Directory.GetFiles(directory.Path));
        Assert.True(dialogs.Error);
        Assert.DoesNotContain("PERSONAL_PATH_SECRET", dialogs.Message, StringComparison.Ordinal);
        Assert.Empty(vm.ErrorText);
    }

    [Fact]
    public async Task Diagnostics_OperationFailureAndDismissedSendRemainUnknown_ConfirmedSendIsRecorded()
    {
        var profile = CreateProfile("PRIVATE_NAME", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace { FailNextConnection = true };
        var dialogs = new DiagnosticDialogs();
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
        Assert.True(vm.HasError);
        Assert.Contains("InvalidOperation", vm.Diagnostics.Capture().Json, StringComparison.Ordinal);
        await vm.ConnectCommand.ExecuteAsync();
        vm.NewMessageCommand.Execute(null);
        vm.SelectedDestination = new DestinationItemViewModel(ServiceBusEntityReference.Queue("PRIVATE_QUEUE"));
        vm.DraftBody = "{\"body\":\"BODY_SECRET\"}";
        dialogs.Confirm = false;
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.DoesNotContain("Confirmed", vm.Diagnostics.Capture().Json, StringComparison.Ordinal);
        dialogs.Confirm = true;
        Assert.True(vm.SendDraftCommand.CanExecute(null), $"Send disabled: connected={vm.IsConnected}, writes={vm.CanWrite}, mismatch={vm.HasDraftEnvironmentMismatch}");
        await vm.SendDraftCommand.ExecuteAsync();
        Assert.True(string.IsNullOrEmpty(vm.ErrorText), vm.ErrorText);
        Assert.Single(workspace.SentMessages);
        var json = vm.Diagnostics.Capture().Json;
        Assert.Contains("Confirmed", json, StringComparison.Ordinal);
        Assert.DoesNotContain("BODY_SECRET", json, StringComparison.Ordinal);
        Assert.DoesNotContain("PRIVATE_QUEUE", json, StringComparison.Ordinal);
        workspace.OnSend = () => throw new DeliveryRejectedException("PRIVATE_REJECTION_SECRET");
        vm.NewMessageCommand.Execute(null);
        vm.SelectedDestination = new DestinationItemViewModel(ServiceBusEntityReference.Queue("PRIVATE_QUEUE"));
        await vm.SendDraftCommand.ExecuteAsync();
        var rejected = vm.Diagnostics.Capture().Json;
        Assert.Contains("Rejected", rejected, StringComparison.Ordinal);
        Assert.DoesNotContain("PRIVATE_REJECTION_SECRET", rejected, StringComparison.Ordinal);
    }

    private sealed class DiagnosticDialogs : IUserDialogService
    {
        public string? Destination { get; set; }
        public bool Approve { get; set; } = true;
        public bool Confirm { get; set; } = true;
        public TaskCompletionSource? Gate { get; set; }
        public int Previews { get; private set; }
        public int Pickers { get; private set; }
        public DiagnosticsPreview? Preview { get; private set; }
        public bool Error { get; private set; }
        public string Message { get; private set; } = "";
        public async Task<bool> PreviewDiagnosticsAsync(DiagnosticsPreview preview, CancellationToken cancellationToken = default)
        { Previews++; Preview = preview; if (Gate is not null) await Gate.Task.WaitAsync(cancellationToken); return Approve; }
        public Task<string?> ChooseSaveFileAsync(string title, string suggestedFileName, IReadOnlyList<(string Name, string Pattern)> fileTypes, CancellationToken cancellationToken = default)
        { Pickers++; return Task.FromResult(Destination); }
        public Task<ProfileEditorResult?> EditProfileAsync(ServiceBusProfile? profile, CancellationToken cancellationToken = default) => Task.FromResult<ProfileEditorResult?>(null);
        public Task<bool> ConfirmAsync(string title, string message, bool isDangerous = false, string? requiredText = null, CancellationToken cancellationToken = default) => Task.FromResult(Confirm);
        public Task ShowMessageAsync(string title, string message, bool isError = false, CancellationToken cancellationToken = default)
        { Message = message; Error = isError; return Task.CompletedTask; }
    }
}
