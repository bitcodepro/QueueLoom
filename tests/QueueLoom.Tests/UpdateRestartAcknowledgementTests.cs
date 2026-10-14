using QueueLoom.App.Services;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class UpdateRestartAcknowledgementTests
{
    [Fact]
    public void CompleteExactPidIsRequiredAndDisappearanceRemainsPending()
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "ack");
        var missing = Assert.Throws<FileNotFoundException>(() => File.OpenRead(path));
        Assert.Equal(unchecked((int)0x80070002), missing.HResult);
        Assert.False(UpdateRestart.HelperAcknowledged(path, 12345));
        File.WriteAllText(path, "12345");
        Assert.True(UpdateRestart.HelperAcknowledged(path, 12345));
        File.Delete(path);
        Assert.False(UpdateRestart.HelperAcknowledged(path, 12345));
    }

    [Theory]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("54321")]
    [InlineData("12345 ")]
    [InlineData(" 12345")]
    [InlineData("12345\n")]
    [InlineData("not-a-pid")]
    public void WrongOrMalformedIdentityIsNeverAccepted(string contents)
    {
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "ack");
        File.WriteAllText(path, contents);
        Assert.False(UpdateRestart.HelperAcknowledged(path, 12345));
    }

    [Fact]
    public void LiveWriterRemainsPendingWithTheProvenSharingCode()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows sharing semantics require Windows."); return; }
        using var directory = new TemporaryDirectory();
        var path = Path.Combine(directory.Path, "ack");
        File.WriteAllText(path, "12345");
        using var writer = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.Read | FileShare.Write | FileShare.Delete);
        var conflict = Assert.Throws<IOException>(() =>
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
        });
        Assert.Equal(unchecked((int)0x80070020), conflict.HResult);
        Assert.False(UpdateRestart.HelperAcknowledged(path, 12345));
        writer.Dispose();
        Assert.True(UpdateRestart.HelperAcknowledged(path, 12345));
    }

    [Fact]
    public void UnknownIoErrorPropagatesInsteadOfBeingTreatedAsAnAcknowledgementWait()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "a file is not a directory");
        File.WriteAllText(file, "owned");
        var error = Assert.Throws<DirectoryNotFoundException>(() => UpdateRestart.HelperAcknowledged(Path.Combine(file, "ack"), 12345));
        Assert.Equal(unchecked((int)0x80070003), error.HResult);
    }

    [Fact]
    public void AccessErrorPropagates()
    {
        using var directory = new TemporaryDirectory();
        Assert.Throws<UnauthorizedAccessException>(() => UpdateRestart.HelperAcknowledged(directory.Path, 12345));
    }
}
