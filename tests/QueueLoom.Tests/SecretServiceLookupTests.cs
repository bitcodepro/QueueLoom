using QueueLoom.Infrastructure.Security;

namespace QueueLoom.Tests;

[CollectionDefinition("Fake secret-tool", DisableParallelization = true)]
public sealed class FakeSecretToolCollection;

[Collection("Fake secret-tool")]
public sealed partial class SecretServiceLookupTests
{
    [Fact]
    public async Task LookupThatFailsWithAMessage_DoesNotReplaceTheStoredKey()
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }
        using var tool = FakeSecretTool.Create("""
            if [ "$1" = "lookup" ]; then echo "Cannot autolaunch D-Bus without X11" >&2; exit 1; fi
            if [ "$1" = "store" ]; then touch "$(dirname "$0")/stored"; exit 0; fi
            exit 2
            """);

        await Assert.ThrowsAsync<SecureStoreUnavailableException>(() =>
            new LinuxSecretServiceMasterKeyStore().GetOrCreateAsync("install-1").AsTask());

        Assert.False(File.Exists(Path.Combine(tool.Directory, "stored")));
    }

    [Fact]
    public async Task LookupThatFindsNothing_CreatesAndStoresAKey()
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }
        using var tool = FakeSecretTool.Create("""
            if [ "$1" = "lookup" ]; then exit 1; fi
            if [ "$1" = "store" ]; then cat > "$(dirname "$0")/stored"; exit 0; fi
            exit 2
            """);

        var key = await new LinuxSecretServiceMasterKeyStore().GetOrCreateAsync("install-1");

        Assert.Equal(32, key.Length);
        Assert.Equal(Convert.ToBase64String(key), File.ReadAllText(Path.Combine(tool.Directory, "stored")));
    }

    private sealed class FakeSecretTool : IDisposable
    {
        private readonly string? _path;

        private FakeSecretTool(string directory, string? path)
        {
            Directory = directory;
            _path = path;
        }

        public string Directory { get; }

        [System.Runtime.Versioning.UnsupportedOSPlatform("windows")]
        public static FakeSecretTool Create(string body)
        {
            var directory = Path.Combine(Path.GetTempPath(), "queueloom-secret-tool-" + Guid.NewGuid().ToString("N"));
            System.IO.Directory.CreateDirectory(directory);
            var script = Path.Combine(directory, "secret-tool");
            File.WriteAllText(script, "#!/bin/sh\n" + body.Replace("\r", string.Empty, StringComparison.Ordinal) + "\n");
            File.SetUnixFileMode(script, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            var path = Environment.GetEnvironmentVariable("PATH");
            Environment.SetEnvironmentVariable("PATH", directory + Path.PathSeparator + path);
            return new FakeSecretTool(directory, path);
        }

        public void Dispose()
        {
            Environment.SetEnvironmentVariable("PATH", _path);
            System.IO.Directory.Delete(Directory, recursive: true);
        }
    }
}
