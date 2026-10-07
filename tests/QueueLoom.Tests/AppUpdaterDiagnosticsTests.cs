using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using System.IO.Compression;

namespace QueueLoom.Tests;

[CollectionDefinition("UpdateRestart recovery events", DisableParallelization = true)]
public sealed class UpdateRestartRecoveryEventsCollection;

// RecoveryRecorded is process-global and carries no receipt identity. These observers must not overlap
// another test collection's Restore calls; the production event and other collections remain unchanged.
[Collection("UpdateRestart recovery events")]
public sealed partial class AppUpdaterTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Diagnostics_SharedDialogJournalRetainsChecksumAfterLargeDownload(bool valid)
    {
        using var buffer = new MemoryStream();
        using (var zip = new ZipArchive(buffer, ZipArchiveMode.Create, leaveOpen: true))
        using (var entry = zip.CreateEntry("QueueLoom.exe", CompressionLevel.NoCompression).Open())
            entry.Write(new byte[81_920 * (DiagnosticsJournal.MaximumEvents + 8)]);
        var package = buffer.ToArray();
        var journal = new DiagnosticsJournal();
        var updater = new AppUpdater(Serve(package, valid ? Sha(package) : new string('0', 64)),
            Path.Combine(_root, "download"), journal);
        var callbacks = 0;
        var previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(new InlineDiagnosticContext());
        try
        {
            var dialog = new UpdateDialogViewModel("1.5.7", Update, async (progress, token) =>
            {
                await updater.DownloadAsync(Update, Target("win-x64"), new DiagnosticProgress(progress, () => callbacks++), token);
            }, diagnostics: journal);
            await dialog.InstallAsync();
            Assert.True(callbacks > DiagnosticsJournal.MaximumEvents);
            Assert.Equal(valid ? UpdateDialogStage.Ready : UpdateDialogStage.Failed, dialog.Stage);
        }
        finally { SynchronizationContext.SetSynchronizationContext(previous); }
        using var json = JsonDocument.Parse(journal.Capture().Json);
        var events = json.RootElement.GetProperty("Events").EnumerateArray().ToArray();
        Assert.InRange(events.Length, 1, DiagnosticsJournal.MaximumEvents);
        Assert.True(events.Length < 20, "Byte progress must not flood the technical journal.");
        Assert.Contains(events, e => e.GetProperty("Checksum").GetString() == (valid ? "Verified" : "Mismatch"));
        Assert.Contains(events, e => e.GetProperty("Stage").GetString() == (valid ? "Completed" : "Failed"));
        if (!valid) Assert.Contains(events, e => e.GetProperty("Errors").GetArrayLength() > 0);
    }

    private sealed class InlineDiagnosticContext : SynchronizationContext
    {
        public override void Post(SendOrPostCallback callback, object? state) => callback(state);
    }
    private sealed class DiagnosticProgress(IProgress<UpdateProgress> target, Action count) : IProgress<UpdateProgress>
    {
        public void Report(UpdateProgress value) { count(); target.Report(value); }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Diagnostics_DownloadRecordsActualChecksumResultWithoutPathsOrContents(bool valid)
    {
        var package = Zip(("QueueLoom.exe", "BODY_UPDATE_SENTINEL"));
        var journal = new DiagnosticsJournal();
        var updater = new AppUpdater(Serve(package, valid ? Sha(package) : new string('0', 64)),
            Path.Combine(_root, "PRIVATE_DOWNLOAD_PATH"), journal);
        if (valid) await updater.DownloadAsync(Update, Target("win-x64"), null, CancellationToken.None);
        else await Assert.ThrowsAsync<UpdateStageException>(() => updater.DownloadAsync(Update, Target("win-x64"), null, CancellationToken.None));
        var preview = journal.Capture();
        using var json = JsonDocument.Parse(preview.Json);
        Assert.Contains(json.RootElement.GetProperty("Events").EnumerateArray(),
            e => e.GetProperty("Checksum").GetString() == (valid ? "Verified" : "Mismatch"));
        Assert.DoesNotContain("PRIVATE_DOWNLOAD_PATH", preview.Json, StringComparison.Ordinal);
        Assert.DoesNotContain("BODY_UPDATE_SENTINEL", preview.Json, StringComparison.Ordinal);
        Assert.DoesNotContain(Sha(package), preview.Json, StringComparison.Ordinal);
        Assert.DoesNotContain("StartupAcknowledged", preview.Json, StringComparison.Ordinal);
    }

    [Fact]
    public void Diagnostics_RollbackRecordedOnlyAfterRestoredExecutableWasVerified()
    {
        var facts = new List<UpdateRestart.RecordedRecovery>();
        void Record(UpdateRestart.RecordedRecovery fact) => facts.Add(fact);
        UpdateRestart.RecoveryRecorded += Record;
        try
        {
            var target = Target("win-x64");
            File.WriteAllText(target.Executable, "old");
            AppUpdater.Install(target, Staging(("QueueLoom.exe", "new")));
            var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(UpdateRestart.ReceiptPath(target)))!;
            var backup = receipt.Entries.Single().Backup!;
            var previous = File.ReadAllText(backup); File.Delete(backup);
            Assert.ThrowsAny<IOException>(() => UpdateRestart.Restore(receipt));
            Assert.Empty(facts);
            File.WriteAllText(backup, previous);
            UpdateRestart.Restore(receipt);
            AssertSingleRestoration(facts);
        }
        finally { UpdateRestart.RecoveryRecorded -= Record; }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public void Diagnostics_RollbackAssertionRejectsMissingOrDuplicateOwnEvents(int successfulRestores)
    {
        var target = Target("win-x64");
        File.WriteAllText(target.Executable, "old");
        AppUpdater.Install(target, Staging(("QueueLoom.exe", "new")));
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(UpdateRestart.ReceiptPath(target)))!;
        var facts = new List<UpdateRestart.RecordedRecovery>();
        void Record(UpdateRestart.RecordedRecovery fact) => facts.Add(fact);
        UpdateRestart.RecoveryRecorded += Record;
        try
        {
            for (var i = 0; i < successfulRestores; i++) UpdateRestart.Restore(receipt);
            Assert.Equal(successfulRestores, facts.Count);
            // Use the same strict assertion as the real rollback test; isolation must not hide missing
            // callbacks or duplicate callbacks for this receipt's own successful Restore operations.
            Assert.Throws<Xunit.Sdk.EqualException>(() => AssertSingleRestoration(facts));
        }
        finally { UpdateRestart.RecoveryRecorded -= Record; }
    }

    private static void AssertSingleRestoration(IEnumerable<UpdateRestart.RecordedRecovery> facts) =>
        Assert.Equal(new[] { UpdateRestart.RecordedRecovery.Restored }, facts);
}
