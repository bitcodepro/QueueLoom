using System.Diagnostics;
using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests;

public sealed partial class AppUpdaterTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Review_BackupCopyStillRejectsLinksInsideItsSubtrees(bool destinationLink)
    {
        var (target, legacy, _) = MacBackupFixture();
        var outside = Path.Combine(_root, "outside-copy");
        Directory.CreateDirectory(outside);
        File.WriteAllText(Path.Combine(outside, "sentinel.json"), "must remain unchanged");
        var destination = Path.Combine(target.InstallDirectory, "backups");
        Directory.CreateDirectory(destination);
        var link = Path.Combine(destinationLink ? destination : legacy, "linked-child");
        await CreateReviewDirectoryLink(link, outside);
        if (destinationLink)
        {
            Directory.CreateDirectory(Path.Combine(legacy, "linked-child"));
            File.WriteAllText(Path.Combine(legacy, "linked-child", "sentinel.json"), "source backup");
        }
        try
        {
            var receipt = new UpdateRestart.Receipt(Guid.NewGuid().ToString("N"), target, "", []);
            var error = Assert.Throws<IOException>(() => MacBackupMigration.Preserve(receipt, target.Bundle!));
            Assert.Contains("cannot follow a link", error.Message, StringComparison.Ordinal);
            Assert.Equal("must remain unchanged", File.ReadAllText(Path.Combine(outside, "sentinel.json")));
            Assert.Equal("message backup", File.ReadAllText(Path.Combine(legacy, "sentinel.json")));
        }
        finally
        {
            Assert.StartsWith(Path.GetFullPath(_root) + Path.DirectorySeparatorChar, Path.GetFullPath(link), StringComparison.Ordinal);
            Assert.True((File.GetAttributes(link) & FileAttributes.ReparsePoint) != 0);
            Directory.Delete(link, recursive: false);
        }
    }

    private static async Task CreateReviewDirectoryLink(string link, string target)
    {
        if (!OperatingSystem.IsWindows()) { Directory.CreateSymbolicLink(link, target); return; }
        var start = new ProcessStartInfo("cmd.exe") { UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var argument in new[] { "/c", "mklink", "/J", link, target }) start.ArgumentList.Add(argument);
        using var process = Process.Start(start)!;
        await process.WaitForExitAsync();
        Assert.Equal(0, process.ExitCode);
    }

    [Fact]
    public async Task Review_MacUpdateUnderLinkedApplicationsParentPreservesBackups()
    {
        var real = Path.Combine(_root, "real-applications");
        Directory.CreateDirectory(real);
        var alias = Path.Combine(_root, "Applications");
        if (OperatingSystem.IsWindows())
        {
            var start = new ProcessStartInfo("cmd.exe") { UseShellExecute = false, CreateNoWindow = true,
                RedirectStandardOutput = true, RedirectStandardError = true };
            foreach (var argument in new[] { "/c", "mklink", "/J", alias, real }) start.ArgumentList.Add(argument);
            using var process = Process.Start(start)!;
            await process.WaitForExitAsync();
            Assert.Equal(0, process.ExitCode);
        }
        else Directory.CreateSymbolicLink(alias, real);
        try
        {
            var (target, _, staging) = MacBackupFixture();
            AppUpdater.Install(target, staging);
            var path = UpdateRestart.ReceiptPath(target);
            var receipt = JsonSerializer.Deserialize<UpdateRestart.Receipt>(File.ReadAllText(path))!;
            await UpdateRestart.CleanAsync(receipt, path, TimeSpan.Zero);
            Assert.False(Directory.Exists(Assert.Single(receipt.Entries).Backup));
            Assert.Equal("message backup", File.ReadAllText(Path.Combine(alias, "backups", "sentinel.json")));
        }
        finally
        {
            // Remove the fixture's link before its target is cleaned up, without traversing it.
            if ((File.GetAttributes(alias) & FileAttributes.ReparsePoint) != 0 &&
                Path.GetFullPath(alias).StartsWith(Path.GetFullPath(_root) + Path.DirectorySeparatorChar, StringComparison.Ordinal))
                Directory.Delete(alias, recursive: false);
        }
    }

    [Fact]
    public void Review_BundleItselfAsBackupOverrideUsesSafeExternalDefault()
    {
        var (target, _, _) = MacBackupFixture();
        var selected = QueueLoomPaths.OutsideApplicationBundle(target.Bundle!, Path.GetDirectoryName(target.Executable)!);
        Assert.Equal(Path.Combine(target.InstallDirectory, "backups"), selected);
    }
}
