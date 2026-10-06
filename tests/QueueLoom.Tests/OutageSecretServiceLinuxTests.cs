using System.Diagnostics;
using System.Security.Cryptography;
using QueueLoom.Infrastructure.Security;

namespace QueueLoom.Tests;

public sealed partial class SecretServiceLookupTests
{
    [Fact]
    public async Task Outage_LinuxCancelledKeyCreationCannotOverwriteTheRetryKey()
    {
        if (!OperatingSystem.IsLinux()) { Assert.Skip("Requires Linux; Windows process test does not cover Secret Service end to end."); return; }
        using var tool = FakeSecretTool.Create("""
            root=$(dirname "$0")
            if [ "$1" = "lookup" ]; then
              if [ -f "$root/stored" ]; then cat "$root/stored"; exit 0; fi
              exit 1
            fi
            if [ "$1" = "store" ]; then
              value=$(cat)
              if [ ! -f "$root/first-store.pid" ]; then
                echo $$ > "$root/first-store.pid"
                while [ ! -f "$root/release-first-store" ]; do sleep 0.02; done
                printf '%s' "$value" > "$root/stored"
                touch "$root/first-store.done"
              else printf '%s' "$value" > "$root/stored"; fi
              exit 0
            fi
            exit 2
            """);
        using var cancellation = new CancellationTokenSource();
        Process? first = null;
        try
        {
            var pending = new LinuxSecretServiceMasterKeyStore().GetOrCreateAsync("isolated-install", cancellation.Token).AsTask();
            var marker = Path.Combine(tool.Directory, "first-store.pid");
            for (var i = 0; i < 200 && !File.Exists(marker); i++) await Task.Delay(10);
            Assert.True(File.Exists(marker));
            first = Process.GetProcessById(int.Parse(File.ReadAllText(marker).Trim(), System.Globalization.CultureInfo.InvariantCulture));
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
            var key = await new LinuxSecretServiceMasterKeyStore().GetOrCreateAsync("isolated-install");
            File.WriteAllText(Path.Combine(tool.Directory, "release-first-store"), "release");
            if (!first.HasExited) await first.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            var stored = Convert.FromBase64String(File.ReadAllText(Path.Combine(tool.Directory, "stored")));
            using var encryption = new AesGcm(key, 16);
            byte[] nonce = new byte[12], encrypted = new byte[4], tag = new byte[16], restored = new byte[4];
            encryption.Encrypt(nonce, "test"u8, encrypted, tag);
            using var decryption = new AesGcm(stored, 16);
            decryption.Decrypt(nonce, encrypted, tag, restored);
            Assert.Equal("test"u8.ToArray(), restored);
            CryptographicOperations.ZeroMemory(key);
            CryptographicOperations.ZeroMemory(stored);
        }
        finally
        {
            if (first is not null) { if (!first.HasExited) { first.Kill(entireProcessTree: true); await first.WaitForExitAsync(); } first.Dispose(); }
        }
    }
}
