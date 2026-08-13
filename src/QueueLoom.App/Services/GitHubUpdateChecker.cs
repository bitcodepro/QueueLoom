using System.Net.Http.Headers;
using System.Reflection;
using System.Text.Json;

namespace QueueLoom.App.Services;

public sealed record UpdateCheckResult(Version Version, string Tag, Uri ReleasePage);

public sealed class GitHubUpdateChecker(HttpClient? httpClient = null) : IDisposable
{
    private static readonly Uri LatestReleaseApi =
        new("https://api.github.com/repos/bitcodepro/QueueLoom/releases/latest");
    private readonly HttpClient _httpClient = httpClient ?? CreateClient();
    private readonly bool _ownsClient = httpClient is null;

    public async Task<UpdateCheckResult?> CheckAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, LatestReleaseApi);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/vnd.github+json"));
            request.Headers.Add("X-GitHub-Api-Version", "2022-11-28");
            using var response = await _httpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken)
                .ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                return null;
            }

            await using var content = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            using var document = await JsonDocument.ParseAsync(content, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            var root = document.RootElement;
            if (!root.TryGetProperty("tag_name", out var tagElement) ||
                !root.TryGetProperty("html_url", out var urlElement))
            {
                return null;
            }

            var tag = tagElement.GetString();
            var url = urlElement.GetString();
            if (!TryParseVersion(tag, out var latestVersion) ||
                !Uri.TryCreate(url, UriKind.Absolute, out var releasePage) ||
                releasePage.Scheme != Uri.UriSchemeHttps ||
                !string.Equals(releasePage.Host, "github.com", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            var currentVersion = Assembly.GetEntryAssembly()?.GetName().Version ?? new Version(0, 0, 0);
            return latestVersion > Normalize(currentVersion)
                ? new UpdateCheckResult(latestVersion, tag!, releasePage)
                : null;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
        catch (HttpRequestException)
        {
            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    internal static bool TryParseVersion(string? tag, out Version version)
    {
        var value = tag?.Trim();
        if (value?.StartsWith("v", StringComparison.OrdinalIgnoreCase) == true)
        {
            value = value[1..];
        }
        var suffix = value?.IndexOfAny(['-', '+']) ?? -1;
        if (suffix >= 0)
        {
            value = value![..suffix];
        }
        if (Version.TryParse(value, out var parsed))
        {
            version = Normalize(parsed);
            return true;
        }
        version = new Version(0, 0, 0);
        return false;
    }

    private static Version Normalize(Version version) =>
        new(Math.Max(0, version.Major), Math.Max(0, version.Minor), Math.Max(0, version.Build));

    private static HttpClient CreateClient()
    {
        var client = new HttpClient { Timeout = TimeSpan.FromSeconds(8) };
        client.DefaultRequestHeaders.UserAgent.ParseAdd("QueueLoom-UpdateCheck");
        return client;
    }

    public void Dispose()
    {
        if (_ownsClient)
        {
            _httpClient.Dispose();
        }
    }
}
