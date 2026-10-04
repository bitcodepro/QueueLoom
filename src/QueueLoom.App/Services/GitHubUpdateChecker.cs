using System.Net.Http.Headers;
using System.Reflection;
using System.Text.Json;

namespace QueueLoom.App.Services;

public sealed record UpdateCheckResult(Version Version, string Tag, Uri ReleasePage);

// currentVersion: the running version with its pre-release tag (for example "1.0.0-rc.1"); null reads it from the application.
public sealed class GitHubUpdateChecker(HttpClient? httpClient = null, string? currentVersion = null) : IDisposable
{
    private static readonly Uri ReleasesApi =
        new("https://api.github.com/repos/bitcodepro/QueueLoom/releases?per_page=30");
    private readonly HttpClient _httpClient = httpClient ?? CreateClient();
    private readonly bool _ownsClient = httpClient is null;

    public async Task<UpdateCheckResult?> CheckAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, ReleasesApi);
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
                if (!item.TryGetProperty("draft", out var draft) || draft.ValueKind != JsonValueKind.False ||
                    !item.TryGetProperty("prerelease", out var prerelease) || prerelease.ValueKind != JsonValueKind.False ||
                    !item.TryGetProperty("tag_name", out var nameElement) || nameElement.ValueKind != JsonValueKind.String)
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

            return latestVersion is not null && IsNewer(latestVersion, currentVersion ?? CurrentVersionText)
                ? new UpdateCheckResult(latestVersion, latestTag!,
                    new Uri($"https://github.com/bitcodepro/QueueLoom/releases/tag/{Uri.EscapeDataString(latestTag!)}"))
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

    public static Version CurrentVersion => Normalize(Assembly.GetEntryAssembly()?.GetName().Version ?? new Version(0, 0, 0));

    /// <summary>
    /// The running version as released, with its pre-release tag ("1.0.0-rc.1") and without build metadata. The
    /// assembly version drops the tag, so a release candidate would otherwise look as new as its final release.
    /// </summary>
    public static string CurrentVersionText
    {
        get
        {
            var informational = Assembly.GetEntryAssembly()?.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
                ?.InformationalVersion;
            var metadata = informational?.IndexOf('+', StringComparison.Ordinal) ?? -1;
            if (metadata >= 0)
            {
                informational = informational![..metadata];
            }
            return TryParseVersion(informational, out _) ? informational!.Trim() : CurrentVersion.ToString(3);
        }
    }

    /// <summary>
    /// True when a published release is newer than the running version. A final release is newer than a
    /// pre-release of the same version (1.0.0 after 1.0.0-rc.1), as semantic versioning orders them.
    /// </summary>
    internal static bool IsNewer(Version release, string current)
    {
        if (!TryParseVersion(current, out var running))
        {
            return true;
        }
        var value = current.Trim();
        var metadata = value.IndexOf('+', StringComparison.Ordinal);
        var isPrerelease = (metadata >= 0 ? value[..metadata] : value).Contains('-', StringComparison.Ordinal);
        return release > running || (release == running && isPrerelease);
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
