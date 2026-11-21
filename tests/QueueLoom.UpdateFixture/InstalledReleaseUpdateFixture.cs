using System.Net;
using System.Security.Cryptography;
using QueueLoom.App.Services;
using QueueLoom.Core.Updates;

internal static class InstalledReleaseUpdateFixture
{
    public static async Task<int> RunAsync(string launcher, string archive, string downloads)
    {
        using var feed = new ReleaseFeed(archive);
        using var client = new HttpClient(feed);
        using var checker = new GitHubUpdateChecker(client, currentVersion: "0.0.0-0");
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        var update = await checker.CheckAsync(token);
        if (update?.Tag != "v0.0.1" || update.Version != new Version(0, 0, 1))
            throw new InvalidDataException("Release checker did not select the stable test release.");

        var target = AppUpdater.TargetFor("win-x64", launcher);
        var installation = new VersionInstallation(launcher);
        installation.VerifyInstallerOwnership();
        if (!AppUpdater.CanInstall(target) || installation.HasPendingActivation())
            throw new InvalidDataException("The installer-managed target is not ready for an update.");

        var updater = new AppUpdater(client, downloads);
        var rejected = false;
        try { await updater.DownloadAsync(update, target, null, token); }
        catch (UpdateStageException error) when (error.Phase == UpdatePhase.Verification) { rejected = true; }
        if (!rejected || installation.HasPendingActivation())
            throw new InvalidDataException("A release with an invalid ZIP checksum did not fail closed.");

        feed.SendBadChecksum = false;
        var staging = await updater.DownloadAsync(update, target, null, token);
        await AppUpdater.InstallWithProgressAsync(target, staging, new Progress<UpdateProgress>(), token);
        if (!installation.HasPendingActivation())
            throw new InvalidDataException("The verified ZIP was not staged.");
        if (!feed.Requests.SequenceEqual(new[] { ReleaseFeed.FeedUrl, ReleaseFeed.ZipUrl + ".sha256", ReleaseFeed.ZipUrl,
                ReleaseFeed.ZipUrl + ".sha256", ReleaseFeed.ZipUrl }))
            throw new InvalidDataException("The updater attempted a download other than the expected portable ZIP and checksum.");
        return 0;
    }

    private sealed class ReleaseFeed(string archive) : HttpMessageHandler
    {
        public const string FeedUrl = "https://api.github.com/repos/bitcodepro/QueueLoom/releases?per_page=30";
        public const string ZipUrl = "https://github.com/bitcodepro/QueueLoom/releases/download/v0.0.1/QueueLoom-0.0.1-win-x64.zip";
        private const string ZipName = "QueueLoom-0.0.1-win-x64.zip";
        public bool SendBadChecksum { get; set; } = true;
        public List<string> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            var uri = request.RequestUri?.AbsoluteUri ?? throw new InvalidDataException("Missing request URL.");
            if (request.Method != HttpMethod.Get) throw new InvalidDataException("Unexpected non-GET update request.");
            Requests.Add(uri);
            HttpContent content;
            switch (uri)
            {
                case FeedUrl:
                    content = new StringContent("""
                        [
                          {"tag_name":"v0.9.0","draft":false,"prerelease":true},
                          {"tag_name":"v0.0.1","draft":false,"prerelease":false,"assets":[
                            {"name":"QueueLoom-0.0.1-win-x64-setup.exe","browser_download_url":"https://github.com/bitcodepro/QueueLoom/releases/download/v0.0.1/QueueLoom-0.0.1-win-x64-setup.exe"},
                            {"name":"QueueLoom-0.0.1-win-x64.zip","browser_download_url":"https://github.com/bitcodepro/QueueLoom/releases/download/v0.0.1/QueueLoom-0.0.1-win-x64.zip"}
                          ]}
                        ]
                        """);
                    break;
                case ZipUrl + ".sha256":
                    await using (var file = File.OpenRead(archive))
                    {
                        var digest = SendBadChecksum ? new string('0', 64)
                            : Convert.ToHexStringLower(await SHA256.HashDataAsync(file, token));
                        content = new StringContent($"{digest}  {ZipName}\n");
                    }
                    break;
                case ZipUrl:
                    content = new StreamContent(File.OpenRead(archive));
                    break;
                default:
                    throw new InvalidDataException($"Unexpected updater URL: {uri}");
            }
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = content };
        }
    }
}
