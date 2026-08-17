using System.Net;
using QueueLoom.App.Services;

namespace QueueLoom.Tests;

public sealed class GitHubUpdateCheckerTests
{
    [Fact]
    public async Task NewerTag_ReturnsTrustedGitHubPage()
    {
        using var client = new HttpClient(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""
                [{"name":"v2.0.0"},{"name":"v99.1.0"},{"name":"not-a-version"}]
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
