using System.Net;
using QueueLoom.App.Services;

namespace QueueLoom.Tests;

public sealed class GitHubUpdateCheckerTests
{
    [Theory]
    [InlineData("1.0.0", true)]
    [InlineData("1.5.0", false)]
    public async Task LaterListedLowerStableDoesNotReplaceHighestStable(string runningVersion, bool expectsUpdate)
    {
        using var client = new HttpClient(new StubHandler(request =>
        {
            Assert.Equal("/repos/bitcodepro/QueueLoom/releases", request.RequestUri!.AbsolutePath);
            Assert.Equal("?per_page=30", request.RequestUri.Query);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("""
                    [{"tag_name":"v1.0.1","draft":false,"prerelease":false},
                     {"tag_name":"v1.5.0","draft":false,"prerelease":false},
                     {"tag_name":"v9.0.0-rc.1","draft":false,"prerelease":true},
                     {"tag_name":"v8.0.0","draft":true,"prerelease":false}]
                    """)
            };
        }));
        using var checker = new GitHubUpdateChecker(client, runningVersion);

        var result = await checker.CheckAsync();

        if (expectsUpdate)
        {
            Assert.NotNull(result);
            Assert.Equal("v1.5.0", result.Tag);
        }
        else
        {
            Assert.Null(result);
        }
    }

    [Fact]
    public async Task NewerPublishedRelease_ReturnsTrustedGitHubPage()
    {
        using var client = new HttpClient(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""
                [{"tag_name":"v2.0.0","draft":false,"prerelease":false},{"tag_name":"v99.1.0","draft":false,"prerelease":false},{"tag_name":"not-a-version","draft":false,"prerelease":false}]
                """)
        }));
        using var checker = new GitHubUpdateChecker(client);

        var result = await checker.CheckAsync();

        Assert.NotNull(result);
        Assert.Equal(new Version(99, 1, 0), result.Version);
        Assert.Equal("github.com", result.ReleasePage.Host);
    }

    [Fact]
    public async Task OfflineCheck_ReturnsNullInsteadOfThrowing()
    {
        using var client = new HttpClient(new StubHandler(_ => throw new HttpRequestException("offline")));
        using var checker = new GitHubUpdateChecker(client);

        var result = await checker.CheckAsync();

        Assert.Null(result);
    }

    [Fact]
    public async Task InvalidResponse_ReturnsNull()
    {
        using var client = new HttpClient(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""
                {"not":"an array"}
                """)
        }));
        using var checker = new GitHubUpdateChecker(client);

        Assert.Null(await checker.CheckAsync());
    }

    private sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> handler) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken) =>
            Task.FromResult(handler(request));
    }
}
