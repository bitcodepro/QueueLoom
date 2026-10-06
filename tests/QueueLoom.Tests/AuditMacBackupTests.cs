using System.Text.Json;
using QueueLoom.App.Services;

namespace QueueLoom.Tests;

public sealed partial class AppUpdaterTests
{
    [Fact]
    public async Task Audit_MacUpdateCleanupPreservesLegacyMessageBackupsOutsideBundle()
    {
        var install = Path.Combine(_root, "Applications");
        var bundle = Path.Combine(install, "QueueLoom.app");
        var target = new UpdateTarget("osx-arm64", install, Path.Combine(bundle, "Contents", "MacOS", "QueueLoom"), bundle);
        Directory.CreateDirectory(Path.GetDirectoryName(target.Executable)!);
        File.WriteAllText(target.Executable, "old program");
        var legacy = Path.Combine(Path.GetDirectoryName(target.Executable)!, "backups");
        Directory.CreateDirectory(legacy);
        File.WriteAllText(Path.Combine(legacy, "sentinel.json"), "message backup");
        var staging = Path.Combine(_root, "mac-staging");
        var executable = Path.Combine(staging, "QueueLoom.app", "Contents", "MacOS", "QueueLoom");
        Directory.CreateDirectory(Path.GetDirectoryName(executable)!);
        File.WriteAllText(executable, "new program");

        AppUpdater.Install(target, staging);
        var receiptPath = UpdateRestart.ReceiptPath(target);
        var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(receiptPath))!;
        await UpdateRestart.CleanAsync(receipt, receiptPath, TimeSpan.Zero);

        Assert.False(Directory.Exists(Assert.Single(receipt.Entries).Backup));
        Assert.Equal("message backup", File.ReadAllText(Path.Combine(target.InstallDirectory, "backups", "sentinel.json")));
    }
}
