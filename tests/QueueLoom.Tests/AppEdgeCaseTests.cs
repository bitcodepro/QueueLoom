using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // ---- Loading a Google service account key file ------------------------------------------------------------------

    // A picker may hand out a stream that cannot tell its length (Length throws). The key still loads; before, the
    // NotSupportedException escaped the async void click handler and ended the application.
    [Fact]
    public async Task AKeyFromAStreamWithoutALengthLoads()
    {
        var editor = new ProfileEditorViewModel(null);
        const string key = """{"type":"service_account","project_id":"orders-prod","client_email":"queueloom@orders-prod.iam.gserviceaccount.com","private_key":"x"}""";

        await editor.LoadGoogleServiceAccountKeyAsync(() => Task.FromResult<Stream>(new UnseekableStream(System.Text.Encoding.UTF8.GetBytes(key))));

        Assert.Equal(key, editor.GoogleServiceAccountKey);
        Assert.False(editor.HasError);
    }

    // A file larger than any key is refused with a reason, instead of being ignored silently.
    [Fact]
    public async Task AFileLargerThanAKeyIsRefusedWithAReason()
    {
        var editor = new ProfileEditorViewModel(null);

        await editor.LoadGoogleServiceAccountKeyAsync(() =>
            Task.FromResult<Stream>(new UnseekableStream(new byte[ProfileEditorViewModel.MaximumKeyFileBytes + 1])));

        Assert.Equal(string.Empty, editor.GoogleServiceAccountKey);
        Assert.Contains("larger than a service account key", editor.Error, StringComparison.Ordinal);
    }

    // Any failure while opening or reading the file is shown in the editor, never thrown out of the click handler.
    [Theory]
    [InlineData("open")]
    [InlineData("read")]
    public async Task AKeyFileThatCannotBeReadIsReportedNotThrown(string failure)
    {
        var editor = new ProfileEditorViewModel(null);

        await editor.LoadGoogleServiceAccountKeyAsync(() => failure == "open"
            ? Task.FromException<Stream>(new InvalidOperationException("The portal refused the file."))
            : Task.FromResult<Stream>(new UnseekableStream([], fail: true)));

        Assert.Contains("The key file could not be read", editor.Error, StringComparison.Ordinal);
    }

    private sealed class UnseekableStream(byte[] content, bool fail = false) : Stream
    {
        private int _position;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (fail) throw new NotSupportedException("This stream cannot be read.");
            var read = Math.Min(count, content.Length - _position);
            Array.Copy(content, _position, buffer, offset, read);
            _position += read;
            return read;
        }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    // ---- Monitor alerts ---------------------------------------------------------------------------------------------

    // A hanging webhook: at most MaximumAlertsInFlight deliveries run at once; further alerts are skipped and listed
    // instead of starting one more stuck delivery each. Each delivery gets a cancellable, bounded token.
    [Fact]
    public async Task AlertsDoNotPileUpBehindAHangingWebhook()
    {
        var alerts = new HangingAlerts();
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace(), alerts: alerts);
        vm.SystemNotifications = false;
        vm.AlertWebhookUrl = "https://hooks.slack.com/services/T/B/X";

        for (var index = 0; index < 10; index++)
        {
            vm.RaiseMonitorAlert("Production", $"orders-{index} (DLQ)", index + 1, null);
        }
        await WaitUntilAsync(() => alerts.Started == MainWindowViewModel.MaximumAlertsInFlight);

        Assert.Equal(MainWindowViewModel.MaximumAlertsInFlight, vm.AlertsInFlight);
        Assert.Equal(10 - MainWindowViewModel.MaximumAlertsInFlight,
            vm.Activity.Count(item => item.Action == "Alert not delivered" && item.Details.Contains("was skipped", StringComparison.Ordinal)));
        Assert.All(alerts.Tokens, token => Assert.True(token.CanBeCanceled));

        alerts.Release.SetResult();
        await WaitUntilAsync(() => vm.AlertsInFlight == 0);
        vm.RaiseMonitorAlert("Production", "orders-after (DLQ)", 1, null);
        await WaitUntilAsync(() => alerts.Started == MainWindowViewModel.MaximumAlertsInFlight + 1);
    }

    private sealed class HangingAlerts : IMonitorAlertService
    {
        private int _started;
        public int Started => Volatile.Read(ref _started);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public System.Collections.Concurrent.ConcurrentBag<CancellationToken> Tokens { get; } = [];

        public Task<bool> ShowSystemNotificationAsync(MonitorAlert alert, bool evenWhenActive = false) => Task.FromResult(false);

        public async Task PostWebhookAsync(string webhookUrl, MonitorAlert alert, CancellationToken cancellationToken = default)
        {
            Tokens.Add(cancellationToken);
            Interlocked.Increment(ref _started);
            await Release.Task.WaitAsync(cancellationToken);
        }
    }
}

public sealed class ProfileExtraFieldTests
{
    // A profiles file saved with a UTF-8 byte order mark, an environment whose id is spelled "ID", and an unknown field
    // that appears twice (hand edited): the file loads as before, the environment's unknown field is kept with its last
    // value, and saving works.
    [Fact]
    public async Task HandEditedProfilesFilesStillLoadAndKeepTheirExtras()
    {
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        var paths = QueueLoom.Infrastructure.Persistence.QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var existing = ViewModelStateTests.CreateProfile("dev", QueueLoom.Core.Profiles.EnvironmentKind.Development);
        using (var repository = new QueueLoom.Infrastructure.Persistence.JsonProfileRepository(paths))
        {
            await repository.UpsertAsync(existing);
        }
        var json = await File.ReadAllTextAsync(paths.ProfilesFile);
        var idText = $"\"id\": \"{existing.Id}\"";
        Assert.Contains(idText, json, StringComparison.Ordinal);
        json = json.Replace(idText, $"\"ID\": \"{existing.Id}\", \"futureFolder\": \"old\", \"futureFolder\": \"payments/ops\"", StringComparison.Ordinal);
        await File.WriteAllBytesAsync(paths.ProfilesFile, [0xEF, 0xBB, 0xBF, .. System.Text.Encoding.UTF8.GetBytes(json)]);

        using (var repository = new QueueLoom.Infrastructure.Persistence.JsonProfileRepository(paths))
        {
            Assert.Equal(existing.Id, Assert.Single(await repository.ListAsync()).Id);
            await repository.UpsertAsync(existing with { Name = "dev renamed" });
        }

        var saved = System.Text.Json.Nodes.JsonNode.Parse(await File.ReadAllTextAsync(paths.ProfilesFile))!;
        var profile = Assert.Single(saved["profiles"]!.AsArray())!;
        Assert.Equal("dev renamed", profile["name"]!.GetValue<string>());
        Assert.Equal("payments/ops", profile["futureFolder"]!.GetValue<string>());
    }

    // An environment written by a newer QueueLoom carries a field this version does not know. Saving any environment
    // (here: adding another one, then editing that one) keeps the field on its environment instead of stripping it.
    [Fact]
    public async Task UnknownFieldsOfAnEnvironmentSurviveARewrite()
    {
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        var paths = QueueLoom.Infrastructure.Persistence.QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var existing = ViewModelStateTests.CreateProfile("dev", QueueLoom.Core.Profiles.EnvironmentKind.Development);
        using (var repository = new QueueLoom.Infrastructure.Persistence.JsonProfileRepository(paths))
        {
            await repository.UpsertAsync(existing);
        }
        var stored = System.Text.Json.Nodes.JsonNode.Parse(await File.ReadAllTextAsync(paths.ProfilesFile))!;
        stored["profiles"]![0]!["futureFolder"] = "payments/ops";
        await File.WriteAllTextAsync(paths.ProfilesFile, stored.ToJsonString());

        using (var repository = new QueueLoom.Infrastructure.Persistence.JsonProfileRepository(paths))
        {
            await repository.UpsertAsync(ViewModelStateTests.CreateProfile("test", QueueLoom.Core.Profiles.EnvironmentKind.Test));
            await repository.UpsertAsync(existing with { Name = "dev renamed" });
            Assert.Equal(2, (await repository.ListAsync()).Count);
        }

        var json = System.Text.Json.Nodes.JsonNode.Parse(await File.ReadAllTextAsync(paths.ProfilesFile))!;
        var renamed = json["profiles"]!.AsArray().Single(profile => profile!["id"]!.GetValue<Guid>() == existing.Id)!;
        Assert.Equal("dev renamed", renamed["name"]!.GetValue<string>());
        Assert.Equal("payments/ops", renamed["futureFolder"]!.GetValue<string>());
        Assert.All(json["profiles"]!.AsArray().Where(profile => profile!["id"]!.GetValue<Guid>() != existing.Id),
            profile => Assert.Null(profile!["futureFolder"]));
    }
}

public sealed class ProtoBestFitTests
{
    // With hundreds of message types and no type hint, only the types that declare every top-level field of the body
    // (with a matching wire type) are decoded in full; the result is the same as before.
    [Fact]
    public void BestFitDecodesOnlyTypesThatCanFit()
    {
        var proto = new System.Text.StringBuilder("syntax = \"proto3\";\npackage many;\n");
        for (var index = 1; index <= 300; index++)
        {
            proto.Append(System.Globalization.CultureInfo.InvariantCulture, $"message M{index} {{ string f = {index}; }}\n");
        }
        proto.Append("message Target { string name = 1; int32 count = 2; }\n");
        var schemas = QueueLoom.Core.ServiceBus.ProtoSchemaSet.FromProtoFiles([("many.proto", proto.ToString())]);
        byte[] body = [0x0A, 0x01, (byte)'a', 0x10, 0x05];
        var attempts = typeof(QueueLoom.Core.ServiceBus.ProtoDecoder).GetField("DecodeAttempts",
            System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;
        attempts.SetValue(null, 0);

        var decoded = QueueLoom.Core.ServiceBus.ProtoDecoder.DecodeBestFit(body, schemas);

        Assert.Equal("many.Target", decoded!.Value.Type.FullName);
        Assert.Contains("\"count\": 5", decoded.Value.Json, StringComparison.Ordinal);
        Assert.Equal(1, (int)attempts.GetValue(null)!);
    }

    // Bytes that are not Protobuf at all are refused after the single scan, without trying any type.
    [Fact]
    public void BestFitRefusesNonProtobufWithoutDecoding()
    {
        var schemas = QueueLoom.Core.ServiceBus.ProtoSchemaSet.FromProtoFiles([("one.proto", "syntax = \"proto3\";\nmessage A { string a = 1; }\n")]);
        var attempts = typeof(QueueLoom.Core.ServiceBus.ProtoDecoder).GetField("DecodeAttempts",
            System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;
        attempts.SetValue(null, 0);

        Assert.Null(QueueLoom.Core.ServiceBus.ProtoDecoder.DecodeBestFit("{\"not\":\"protobuf\"}"u8, schemas));
        Assert.Equal(0, (int)attempts.GetValue(null)!);
    }
}
