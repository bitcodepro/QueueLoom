using System.Text.Json;
using QueueLoom.App.Services;

namespace QueueLoom.Tests;

public sealed partial class AppUpdaterTests
{
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
            Assert.Equal(new[] { UpdateRestart.RecordedRecovery.Restored }, facts);
        }
        finally { UpdateRestart.RecoveryRecorded -= Record; }
    }
}
