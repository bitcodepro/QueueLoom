using System.Net;
using QueueLoom.App.Services;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class ReleaseUpdaterAuditTests
{
    [Theory]
    [InlineData("osx-arm64")]
    [InlineData("osx-x64")]
    public void MacPackageWithoutItsExecutableIsRejectedBeforeReplacingTheWorkingBundle(string rid)
    {
        using var directory = new TemporaryDirectory();
        var executable = Path.Combine(directory.Path, "Applications", "QueueLoom.app", "Contents", "MacOS", "QueueLoom");
        Directory.CreateDirectory(Path.GetDirectoryName(executable)!);
        File.WriteAllText(executable, "working version");
        var target = AppUpdater.TargetFor(rid, executable);
        var staging = Path.Combine(directory.Path, "staging");
        Directory.CreateDirectory(Path.Combine(staging, "QueueLoom.app", "Contents"));
        File.WriteAllText(Path.Combine(staging, "QueueLoom.app", "Contents", "Info.plist"), "incomplete bundle");
        Assert.Throws<InvalidOperationException>(() => AppUpdater.Install(target, staging));
        Assert.Equal("working version", File.ReadAllText(executable));
        Assert.False(File.Exists(UpdateRestart.ReceiptPath(target)));
        Assert.Empty(Directory.GetDirectories(target.InstallDirectory, "*.old"));
    }

    [Fact]
    public async Task UpdateDiscoveryIgnoresUnpublishedTagsDraftsAndPrereleases()
    {
        using var http = new HttpClient(new ReleaseHandler());
        using var checker = new GitHubUpdateChecker(http);
        var update = Assert.IsType<UpdateCheckResult>(await checker.CheckAsync());
        Assert.Equal(new Version(90, 1, 0), update.Version);
        Assert.Equal("v90.1.0", update.Tag);
        Assert.Equal("https://github.com/bitcodepro/QueueLoom/releases/tag/v90.1.0", update.ReleasePage.AbsoluteUri);
    }

    private sealed class ReleaseHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            var isTags = request.RequestUri!.AbsolutePath.EndsWith("/tags", StringComparison.Ordinal);
            var body = isTags ? """
                [{"name":"v99.0.0-alpha"},{"name":"v98.0.0"},{"name":"v97.0.0"},{"name":"v90.1.0"}]
                """ : """
                [{"tag_name":"v99.0.0-alpha","draft":false,"prerelease":true},
                 {"tag_name":"v97.0.0","draft":true,"prerelease":false},
                 {"tag_name":"v90.1.0","draft":false,"prerelease":false}]
                """;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) });
        }
    }
}
