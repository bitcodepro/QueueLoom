using System.IO.Compression;
using System.Net;
using System.Security.Cryptography;
using QueueLoom.App.Services;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class StabilityUpdaterTests
{
    [Theory]
    [InlineData(UpdatePhase.ChecksumFetch)]
    [InlineData(UpdatePhase.Downloading)]
    [InlineData(UpdatePhase.Verification)]
    [InlineData(UpdatePhase.Extraction)]
    public async Task InterruptedPreInstallStageRetainsCurrentFilesAndOtherDownloadThenAllowsFreshRetry(UpdatePhase interrupted)
    {
        using var directory = new TemporaryDirectory();
        var install = Path.Combine(directory.Path, "install");
        Directory.CreateDirectory(install);
        var target = new UpdateTarget("win-x64", install, Path.Combine(install, "QueueLoom.exe"), null);
        File.WriteAllText(target.Executable, "current version");
        var downloads = Path.Combine(directory.Path, "downloads");
        var unrelated = Path.Combine(downloads, "other-window", "keep");
        Directory.CreateDirectory(Path.GetDirectoryName(unrelated)!);
        File.WriteAllText(unrelated, "other download");
        using var bytes = new MemoryStream();
        using (var archive = new ZipArchive(bytes, ZipArchiveMode.Create, leaveOpen: true))
        using (var writer = new StreamWriter(archive.CreateEntry("QueueLoom.exe").Open())) writer.Write("replacement version");
        using var http = new HttpClient(new ArchiveHandler(bytes.ToArray()));
        var updater = new AppUpdater(http, downloads);
        var update = new UpdateCheckResult(new Version(9, 1, 0), "v9.1.0", new Uri("https://example.test/release"));
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => updater.DownloadAsync(update, target,
            new InlineProgress(progress => { if (progress.Phase == interrupted) cancellation.Cancel(); }), cancellation.Token));
        Assert.Equal("current version", File.ReadAllText(target.Executable));
        Assert.False(File.Exists(UpdateRestart.ReceiptPath(target)));
        Assert.Single(Directory.GetDirectories(downloads));
        Assert.Equal("other download", File.ReadAllText(unrelated));
        var phases = new List<UpdatePhase>();
        var staging = await updater.DownloadAsync(update, target, new InlineProgress(progress => phases.Add(progress.Phase)), default);
        Assert.Equal("replacement version", File.ReadAllText(Path.Combine(staging, "QueueLoom.exe")));
        Assert.Equal([UpdatePhase.ChecksumFetch, UpdatePhase.Downloading, UpdatePhase.Verification, UpdatePhase.Extraction], phases.Distinct());
        Assert.Equal("current version", File.ReadAllText(target.Executable));
        Assert.Equal("other download", File.ReadAllText(unrelated));
    }

    private sealed class InlineProgress(Action<UpdateProgress> callback) : IProgress<UpdateProgress>
    {
        public void Report(UpdateProgress value) => callback(value);
    }

    private sealed class ArchiveHandler(byte[] archive) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            HttpContent content = request.RequestUri!.AbsolutePath.EndsWith(".sha256", StringComparison.Ordinal)
                ? new StringContent(Convert.ToHexStringLower(SHA256.HashData(archive))) : new ByteArrayContent(archive);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content });
        }
    }
}
