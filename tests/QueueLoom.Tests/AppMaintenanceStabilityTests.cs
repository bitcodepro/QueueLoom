using QueueLoom.App.Services;

namespace QueueLoom.Tests;

public sealed class AppMaintenanceStabilityTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "maintenance", Guid.NewGuid().ToString("N"));

    public AppMaintenanceStabilityTests() => Directory.CreateDirectory(_root);

    public void Dispose() => Directory.Delete(_root, recursive: true);

    [Theory]
    [InlineData("")]
    [InlineData("{\"Id\":\"0123")]
    [InlineData("null")]
    public void UnreadableUpdateReceipt_IsSetAsideSoLaterUpdatesAreNotBlockedForever(string damaged)
    {
        // A crash while the receipt was being written leaves it empty or cut short. Every later start used to throw
        // while cleaning up, so the update check was skipped and Install refused to overwrite the receipt: no update
        // could ever be installed again.
        var directory = Path.Combine(_root, "app");
        Directory.CreateDirectory(directory);
        var target = new UpdateTarget("win-x64", directory, Path.Combine(directory, "QueueLoom.exe"), null);
        File.WriteAllText(target.Executable, "running program");
        var receipt = UpdateRestart.ReceiptPath(target);
        File.WriteAllText(receipt, damaged);

        new AppUpdater(new HttpClient(), Path.Combine(_root, "download")).CleanUpPreviousUpdate(target);

        Assert.False(File.Exists(receipt));
        Assert.Single(Directory.GetFiles(directory, Path.GetFileName(receipt) + ".damaged-*"));
        var staging = Path.Combine(_root, "staging");
        Directory.CreateDirectory(staging);
        File.WriteAllText(Path.Combine(staging, "QueueLoom.exe"), "new program");
        AppUpdater.Install(target, staging);
        Assert.Equal("new program", File.ReadAllText(target.Executable));
    }
}
