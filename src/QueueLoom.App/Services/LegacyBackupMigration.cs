using QueueLoom.Infrastructure.Persistence;
using System.Diagnostics;
using System.Text.Json;
using QueueLoom.Core.IO;

namespace QueueLoom.App.Services;

/// <summary>Makes backups from a running older bundle available in the selected external backup folder.</summary>
public sealed class LegacyBackupMigration
{
    public LegacyBackupMigration(QueueLoomPaths paths, string? executableDirectory = null, string? backupOverride = null)
    { Paths = paths; ExecutableDirectory = executableDirectory ?? QueueLoom.Core.Updates.PayloadLaunch.Current?.Installation.DataAnchor ?? AppContext.BaseDirectory; BackupOverride = backupOverride; }
    internal QueueLoomPaths Paths { get; }
    internal string ExecutableDirectory { get; }
    internal string? BackupOverride { get; }
    public Task<int> RunAsync(CancellationToken cancellationToken = default) => Task.Run(async () =>
    {
        var bundle = QueueLoomPaths.ApplicationBundleFor(ExecutableDirectory);
        if (bundle is null || !Directory.Exists(bundle)) return 0;
        var target = new UpdateTarget("osx-arm64", Path.GetDirectoryName(bundle)!, Path.Combine(ExecutableDirectory, "QueueLoom"), bundle);
        Paths.EnsureCreated();
        using var ownership = await AcquireMigrationFileAsync(cancellationToken).ConfigureAwait(false);
        var cachePath = Path.Combine(Paths.RootDirectory, "legacy-backup-migration.v1.json");
        Dictionary<string, MacBackupMigration.VerifiedCopy>? entries = null;
        try
        {
            if (File.Exists(cachePath)) entries = JsonSerializer.Deserialize<Dictionary<string, MacBackupMigration.VerifiedCopy>>(
                await File.ReadAllTextAsync(cachePath, cancellationToken).ConfigureAwait(false));
        }
        catch (JsonException) { /* Rebuild a damaged hint from verified copies; never trust it to skip a file. */ }
        var cache = new MacBackupMigration.CopyCache(entries ?? []);
        var copied = 0;
        foreach (var relative in MacBackupMigration.CaptureDirectories(target, BackupOverride) ?? [])
        {
            var source = Path.Combine(bundle, relative);
            if (Directory.Exists(source)) copied += MacBackupMigration.CopyLegacyDirectory(source, Paths.BackupsDirectory, bundle, cancellationToken, cache);
        }
        // Commit hints only after the complete pass succeeds; interrupted/failed passes remain safe to retry.
        if (cache.Changed) await SafeFileWriter.WriteTextAsync(cachePath, JsonSerializer.Serialize(cache.Entries), cancellationToken).ConfigureAwait(false);
        return copied;
    }, cancellationToken);

    private async Task<FileStream> AcquireMigrationFileAsync(CancellationToken token)
    {
        var elapsed = Stopwatch.StartNew();
        while (true)
        {
            token.ThrowIfCancellationRequested();
            try { return new FileStream(Path.Combine(Paths.RootDirectory, ".legacy-backups.lock"),
                FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None); }
            catch (IOException) when (elapsed.Elapsed < TimeSpan.FromSeconds(30))
            { await Task.Delay(50, token).ConfigureAwait(false); }
        }
    }
}
