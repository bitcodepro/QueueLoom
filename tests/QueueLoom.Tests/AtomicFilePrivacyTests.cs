using System.Runtime.Versioning;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// A file written atomically is the current user's alone before any of its content exists on disk, text or bytes alike.
public sealed class AtomicFilePrivacyTests
{
    [Theory]
    [InlineData("text")]
    [InlineData("bytes")]
    public async Task TheTemporaryFileIsPrivateBeforeContentIsWritten(string kind)
    {
        if (OperatingSystem.IsWindows())
        {
            Assert.Skip("Unix file modes; Windows restricts the ACL, which this test cannot read portably.");
            return;
        }
        await VerifyUnix(kind);
    }

    [UnsupportedOSPlatform("windows")]
    private static async Task VerifyUnix(string kind)
    {
        using var directory = new TemporaryDirectory();
        var target = Path.Combine(directory.Path, "secret.bin");
        UnixFileMode? observed = null;
        long? length = null;
        var previous = AtomicFile.ContentWriting.Value;
        AtomicFile.ContentWriting.Value = temporary =>
        {
            observed = File.GetUnixFileMode(temporary);
            length = new FileInfo(temporary).Length;
        };
        try
        {
            if (kind == "text") await AtomicFile.WriteTextAsync(target, "protected", CancellationToken.None);
            else await AtomicFile.WriteBytesAsync(target, "protected"u8.ToArray(), CancellationToken.None);
        }
        finally
        {
            AtomicFile.ContentWriting.Value = previous;
        }

        Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, observed);
        Assert.Equal(0, length);
        Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(target));
        Assert.Equal("protected", File.ReadAllText(target));
    }
}
