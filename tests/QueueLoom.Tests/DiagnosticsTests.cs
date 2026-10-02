using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using QueueLoom.App.Services;
using QueueLoom.Core.Profiles;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class DiagnosticsTests
{
    private static readonly string[] Sentinels =
    [
        "SEED_TOKEN_xyz_123", "SharedAccessKey=SEED_CONNECTION_987", "BODY_SENTINEL_456", "HEADER_SENTINEL_567",
        "https://SEED_USER:SEED_PASSWORD@private.example.test/SEED_QUEUE?token=SEED_TOKEN_xyz_123",
        "C:\\Users\\SEED_PERSON\\private\\SEED_FILE.txt", "/home/SEED_PERSON/private/SEED_FILE.txt", "SEED_QUEUE"
    ];

    [Fact]
    public async Task Archive_ContainsOnlyPreviewedAllowlistedData_EvenWithNestedEncodedAndMalformedSecrets()
    {
        using var directory = new TemporaryDirectory();
        var journal = new DiagnosticsJournal();
        var variants = Sentinels.SelectMany(s => new[] { s, Uri.EscapeDataString(s), Convert.ToBase64String(Encoding.UTF8.GetBytes(s)),
            Convert.ToHexString(Encoding.UTF8.GetBytes(s)), s.Replace("_", "_\r\n") }).ToArray();
        var prose = string.Join("\r\n", variants) + "\0\ud800";
        var operation = journal.Begin(prose, MessagingProvider.RabbitMq, prose, prose);
        var nested = new AggregateException(prose,
            new ServiceBusException(prose, ServiceBusFailureReason.ServiceTimeout),
            new InvalidOperationException(prose, new TimeoutException(prose)));
        nested.Data["body"] = prose;
        journal.Record(operation, DiagnosticStage.Failed, error: nested);
        journal.Record(operation, DiagnosticStage.Completed, DiagnosticOutcome.Confirmed);
        journal.Record(operation, DiagnosticStage.Failed, error: new HostileException());
        var preview = journal.Capture();
        var destination = Path.Combine(directory.Path, "safe.zip");
        await preview.SaveAsync(destination);
        var bytes = await File.ReadAllBytesAsync(destination);
        var contents = new List<string> { preview.Report, preview.Json, Encoding.UTF8.GetString(bytes), Encoding.Unicode.GetString(bytes) };
        using var archive = ZipFile.OpenRead(destination);
        Assert.Equal(new[] { "report.txt", "diagnostics.json" }, archive.Entries.Select(e => e.FullName));
        foreach (var entry in archive.Entries)
        {
            Assert.Equal(2000, entry.LastWriteTime.Year);
            Assert.Equal(0, entry.ExternalAttributes);
            using var reader = new StreamReader(entry.Open());
            var content = await reader.ReadToEndAsync();
            Assert.Equal(entry.Name == "report.txt" ? preview.Report : preview.Json, content);
            contents.Add(entry.FullName); contents.Add(content);
        }
        foreach (var text in contents)
            foreach (var sentinel in variants) Assert.DoesNotContain(sentinel, text, StringComparison.Ordinal);
        using var json = JsonDocument.Parse(preview.Json);
        var events = json.RootElement.GetProperty("Events").EnumerateArray().ToArray();
        Assert.Equal("Other operation", events[0].GetProperty("Kind").GetString());
        Assert.Contains(events[1].GetProperty("Errors").EnumerateArray(), e => e.GetProperty("Code").GetString() == "ServiceTimeout");
        Assert.Contains(events[1].GetProperty("Errors").EnumerateArray(), e => e.GetProperty("Category").GetString() == "Timeout");
        Assert.Equal("Unknown", events[1].GetProperty("Outcome").GetString());
        Assert.Equal("Confirmed", events[2].GetProperty("Outcome").GetString());
        Assert.Equal(JsonValueKind.Null, events[0].GetProperty("Address").ValueKind);
    }

    [Fact]
    public void MalformedIdentity_IsOmittedRatherThanConflatedWithDifferentMalformedIdentity()
    {
        var journal = new DiagnosticsJournal();
        journal.Begin("Connecting", address: "\ud800", entity: "\ud801");
        journal.Begin("Connecting", address: "\ud801", entity: "\ud800");
        using var json = JsonDocument.Parse(journal.Capture().Json);
        Assert.All(json.RootElement.GetProperty("Events").EnumerateArray(), e =>
        {
            Assert.Equal(JsonValueKind.Null, e.GetProperty("Address").ValueKind);
            Assert.Equal(JsonValueKind.Null, e.GetProperty("Entity").ValueKind);
        });
    }

    private sealed class HostileException : Exception
    {
        public override string Message => throw new InvalidOperationException("Never read Message");
        public override string? StackTrace => throw new InvalidOperationException("Never read StackTrace");
        public override string ToString() => throw new InvalidOperationException("Never read ToString");
    }

    [Fact]
    public void MalformedEnumsNullLabelsAndLargeInputs_FailClosed_AndRelationshipsAreReportLocal()
    {
        var journal = new DiagnosticsJournal();
        var first = journal.Begin(null, (MessagingProvider)int.MaxValue, "private-address", "private-entity");
        journal.Record(first, (DiagnosticStage)999, (DiagnosticOutcome)999, updateStage: (UpdatePhase)999,
            checksum: (DiagnosticCheck)999, restart: (DiagnosticRecovery)999);
        journal.Begin("Connecting", MessagingProvider.Kafka, "private-address", "private-entity");
        journal.Begin("Connecting", MessagingProvider.Kafka, "other-address", "private-entity");
        journal.Begin("Connecting", entity: new string('s', 100_000));
        using var json = JsonDocument.Parse(journal.Capture().Json);
        var events = json.RootElement.GetProperty("Events").EnumerateArray().ToArray();
        Assert.Equal("Unknown", events[0].GetProperty("Broker").GetString());
        Assert.Equal("Unknown", events[1].GetProperty("Stage").GetString());
        Assert.Equal("Unknown", events[1].GetProperty("Outcome").GetString());
        Assert.Equal(JsonValueKind.Null, events[1].GetProperty("UpdateStage").ValueKind);
        Assert.Equal(events[0].GetProperty("Address").GetString(), events[2].GetProperty("Address").GetString());
        Assert.Equal(events[0].GetProperty("Entity").GetString(), events[2].GetProperty("Entity").GetString());
        Assert.NotEqual(events[2].GetProperty("Entity").GetString(), events[3].GetProperty("Entity").GetString());
        Assert.Equal(JsonValueKind.Null, events[4].GetProperty("Entity").ValueKind);
    }

    [Fact]
    public void RecentEventsAndExceptionTraversalAndTotalOutput_AreBounded()
    {
        var journal = new DiagnosticsJournal();
        Exception error;
        try { AppUpdater.TargetFor("win-x64", "PRIVATE_PATH\0PRIVATE_TOKEN"); throw new Exception(); }
        catch (Exception captured) { error = captured; }
        var aggregate = new AggregateException(Enumerable.Repeat(new AggregateException(Enumerable.Repeat(error, 1000)), 1000));
        for (var i = 0; i < 200; i++)
        {
            var operation = journal.Begin("Update");
            journal.Record(operation, DiagnosticStage.Failed, error: aggregate);
        }
        var preview = journal.Capture();
        Assert.True(Encoding.UTF8.GetByteCount(preview.Report) + Encoding.UTF8.GetByteCount(preview.Json) <= DiagnosticsPreview.MaximumOutputBytes);
        using var json = JsonDocument.Parse(preview.Json);
        var events = json.RootElement.GetProperty("Events").EnumerateArray().ToArray();
        Assert.Equal(DiagnosticsJournal.MaximumEvents, events.Length);
        Assert.Equal(336, json.RootElement.GetProperty("DiscardedEvents").GetInt64());
        Assert.All(events, e => Assert.True(e.GetProperty("Errors").GetArrayLength() <= 4));
        Assert.Contains("AppUpdater.TargetFor", preview.Json, StringComparison.Ordinal);
        Assert.DoesNotContain("PRIVATE_PATH", preview.Json, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FrozenPreview_SaveNeverReplacesExistingFiles_AndCleansOwnedStaging()
    {
        using var directory = new TemporaryDirectory();
        var journal = new DiagnosticsJournal();
        journal.Begin("Connecting");
        var preview = journal.Capture();
        journal.Begin("Disconnecting");
        var existing = Path.Combine(directory.Path, "unrelated.zip");
        await File.WriteAllTextAsync(existing, "unrelated content");
        await Assert.ThrowsAnyAsync<IOException>(() => preview.SaveAsync(existing));
        Assert.Equal("unrelated content", await File.ReadAllTextAsync(existing));
        Assert.Single(Directory.GetFiles(directory.Path));
        var folder = Path.Combine(directory.Path, "folder.zip"); Directory.CreateDirectory(folder);
        await Assert.ThrowsAnyAsync<IOException>(() => preview.SaveAsync(folder));
        Assert.Empty(Directory.GetFiles(directory.Path, "*.tmp"));
        var destination = Path.Combine(directory.Path, "new.zip");
        using var cancelled = new CancellationTokenSource(); cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => preview.SaveAsync(destination, cancelled.Token));
        Assert.False(File.Exists(destination));
        await preview.SaveAsync(destination);
        using var archive = ZipFile.OpenRead(destination);
        using var reader = new StreamReader(archive.GetEntry("diagnostics.json")!.Open());
        Assert.Equal(preview.Json, await reader.ReadToEndAsync());
        Assert.DoesNotContain("Disconnecting", preview.Json, StringComparison.Ordinal);
        await Assert.ThrowsAnyAsync<IOException>(() => preview.SaveAsync(Path.Combine(directory.Path, "missing", "new.zip")));
        await Assert.ThrowsAsync<InvalidDataException>(() => preview.SaveAsync(Path.Combine(directory.Path, "notes.txt")));
    }

    [Fact]
    public async Task ConcurrentExports_UseIsolatedDestinations_AndNeverOverwriteARacingExport()
    {
        using var directory = new TemporaryDirectory();
        var preview = new DiagnosticsJournal().Capture();
        var first = Path.Combine(directory.Path, "first.zip");
        var second = Path.Combine(directory.Path, "second.zip");
        await Task.WhenAll(preview.SaveAsync(first), preview.SaveAsync(second));
        Assert.Equal(2, Directory.GetFiles(directory.Path).Length);
        var collision = Path.Combine(directory.Path, "collision.zip");
        var saves = new[] { preview.SaveAsync(collision), preview.SaveAsync(collision) };
        await Assert.ThrowsAnyAsync<IOException>(() => Task.WhenAll(saves));
        _ = Assert.Single(saves, t => t.IsCompletedSuccessfully);
        Assert.Empty(Directory.GetFiles(directory.Path, "*.tmp"));
        using var zip = ZipFile.OpenRead(collision);
        Assert.Equal(2, zip.Entries.Count);
    }

    [Fact]
    public async Task WindowsNetworkDestination_IsRejectedBeforeAnyFileIsCreated()
    {
        if (!OperatingSystem.IsWindows()) return;
        await Assert.ThrowsAsync<InvalidDataException>(() => new DiagnosticsJournal().Capture().SaveAsync("\\\\localhost\\PRIVATE_SHARE\\report.zip"));
    }
}
