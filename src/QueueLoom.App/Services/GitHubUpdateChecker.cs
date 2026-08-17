using System.Net.Http.Headers;
using System.Reflection;
using System.Text.Json;

namespace QueueLoom.App.Services;

public sealed record UpdateCheckResult(Version Version, string Tag, Uri ReleasePage);

public sealed class GitHubUpdateChecker(HttpClient? httpClient = null) : IDisposable
{
    private static readonly Uri TagsApi =
        new("https://api.github.com/repos/bitcodepro/QueueLoom/tags?per_page=30");
    private static readonly Uri TagsPage =
        new("https://github.com/bitcodepro/QueueLoom/tags");
    private readonly HttpClient _httpClient = httpClient ?? CreateClient();
    private readonly bool _ownsClient = httpClient is null;

    public async Task<UpdateCheckResult?> CheckAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, TagsApi);
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
            if (document.RootElement.ValueKind != JsonValueKind.Array)
            {
                return null;
            }

            string? latestTag = null;
            Version? latestVersion = null;
            foreach (var item in document.RootElement.EnumerateArray())
            {
                if (!item.TryGetProperty("name", out var nameElement))
                {
                    continue;
                }
                var tag = nameElement.GetString();
                if (TryParseVersion(tag, out var candidate) &&
                    (latestVersion is null || candidate > latestVersion))
                {
                    latestVersion = candidate;
                    latestTag = tag;
                }
            }

            var currentVersion = Assembly.GetEntryAssembly()?.GetName().Version ?? new Version(0, 0, 0);
            return latestVersion is not null && latestVersion > Normalize(currentVersion)
                ? new UpdateCheckResult(latestVersion, latestTag!, TagsPage)
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
