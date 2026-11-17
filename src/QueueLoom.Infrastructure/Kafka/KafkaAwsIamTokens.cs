using Amazon;
using AWS.MSK.Auth;
using Confluent.Kafka;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Infrastructure.Kafka;

/// <summary>
/// Amazon MSK IAM tokens for SASL/OAUTHBEARER: signed with the AWS credential chain, or a named profile, and handed to
/// librdkafka whenever it asks for a new one (before the previous one expires).
/// </summary>
internal sealed class KafkaAwsIamTokens(KafkaAwsIamSettings settings, Func<Task<(string Token, long ExpiryMs)>>? generate = null)
{
    private readonly Func<Task<(string Token, long ExpiryMs)>> _generate = generate ?? (() => GenerateAsync(settings));

    /// <summary>The principal name librdkafka logs; MSK identifies the caller from the signed token itself.</summary>
    internal const string Principal = "queueloom";

    public async Task<(string Token, long ExpiryMs)> NextAsync() => await _generate().ConfigureAwait(false);

    /// <summary>A token for the consumer group listing child, which cannot sign one itself.</summary>
    public async Task<KafkaGroupListing.ListingToken> ListingTokenAsync()
    {
        var (token, expiryMs) = await NextAsync().ConfigureAwait(false);
        return new KafkaGroupListing.ListingToken(token, expiryMs, Principal);
    }

    /// <summary>librdkafka's refresh callback: runs on its background thread, so it may block.</summary>
    public void Refresh(IClient client)
    {
        var (token, expiryMs, error) = Fetch();
        try
        {
            if (error is null)
            {
                client.OAuthBearerSetToken(token!, expiryMs, Principal);
            }
            else
            {
                // librdkafka reports this as the connection error and asks again.
                client.OAuthBearerSetTokenFailure(error);
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            // The client was closed while the token was being signed; nothing is waiting for it.
        }
    }

    /// <summary>A new token, or why there is none.</summary>
    internal (string? Token, long ExpiryMs, string? Error) Fetch()
    {
        try
        {
            var (token, expiryMs) = NextAsync().GetAwaiter().GetResult();
            return (token, expiryMs, null);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            return (null, 0, $"The AWS IAM token could not be created: {exception.Message}");
        }
    }

    private static async Task<(string Token, long ExpiryMs)> GenerateAsync(KafkaAwsIamSettings settings)
    {
        var generator = new AWSMSKAuthTokenGenerator();
        var region = RegionEndpoint.GetBySystemName(settings.Region);
        return string.IsNullOrWhiteSpace(settings.ProfileName)
            ? await generator.GenerateAuthTokenAsync(region).ConfigureAwait(false)
            : await generator.GenerateAuthTokenFromProfileAsync(settings.ProfileName.Trim(), region).ConfigureAwait(false);
    }
}
