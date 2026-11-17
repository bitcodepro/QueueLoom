using System.Text.Json;
using Confluent.Kafka;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

public sealed class KafkaSignInTests
{
    private static ServiceBusProfile Kafka(AuthenticationKind kind, KafkaSettings settings) =>
        ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development, new(kind)) with { Provider = MessagingProvider.Kafka, Kafka = settings };

    [Fact]
    public void OAuthAndAwsIamAreKafkaSignInMethods()
    {
        Assert.Contains(AuthenticationKind.KafkaOAuth, AuthenticationKinds.For(MessagingProvider.Kafka));
        Assert.Contains(AuthenticationKind.KafkaAwsIam, AuthenticationKinds.For(MessagingProvider.Kafka));
        Assert.Equal(MessagingProvider.Kafka, AuthenticationKind.KafkaOAuth.Provider());
        Assert.Equal(MessagingProvider.Kafka, AuthenticationKind.KafkaAwsIam.Provider());
        // The OAuth client secret is kept in the vault; IAM signs with the computer's AWS credentials.
        Assert.True(AuthenticationKind.KafkaOAuth.UsesStoredSecret());
        Assert.False(AuthenticationKind.KafkaAwsIam.UsesStoredSecret());
    }

    [Fact]
    public void OAuthSignInIsLeftToLibrdkafkaWithTheClientCredentials()
    {
        var config = new ClientConfig();

        KafkaWorkspace.ApplyOAuth(config, new KafkaOAuthSettings("https://login.invalid/oauth2/token", "client", " kafka ", "logicalCluster=lkc-1,identityPoolId=pool-2"), "synthetic");

        Assert.Equal(SaslMechanism.OAuthBearer, config.SaslMechanism);
        Assert.Equal(SaslOauthbearerMethod.Oidc, config.SaslOauthbearerMethod);
        Assert.Equal("https://login.invalid/oauth2/token", config.SaslOauthbearerTokenEndpointUrl);
        Assert.Equal("client", config.SaslOauthbearerClientId);
        Assert.Equal("synthetic", config.SaslOauthbearerClientSecret);
        Assert.Equal("kafka", config.SaslOauthbearerScope);
        Assert.Equal("logicalCluster=lkc-1,identityPoolId=pool-2", config.SaslOauthbearerExtensions);

        var bare = new ClientConfig();
        KafkaWorkspace.ApplyOAuth(bare, new KafkaOAuthSettings("https://login.invalid/token", "client"), "synthetic");
        Assert.Null(bare.SaslOauthbearerScope);
        Assert.Null(bare.SaslOauthbearerExtensions);
    }

    [Theory]
    [InlineData("https://login.invalid/token", "client", null, null)]
    [InlineData("http://localhost:8080/token", "client", "logicalCluster=lkc-1", null)]
    [InlineData("http://login.invalid/token", "client", null, "profile.kafka.oauth.endpoint")]
    [InlineData("https://user:secret@login.invalid/token", "client", null, "profile.kafka.oauth.endpoint")]
    [InlineData("login.invalid/token", "client", null, "profile.kafka.oauth.endpoint")]
    [InlineData("https://login.invalid/token", " ", null, "profile.kafka.oauth.client")]
    [InlineData("https://login.invalid/token", "client", "logicalCluster", "profile.kafka.oauth.extensions")]
    [InlineData("https://login.invalid/token", "client", "a=b,c=d,e", "profile.kafka.oauth.extensions")]
    public void OAuthSettingsAreValidated(string endpoint, string clientId, string? extensions, string? expected)
    {
        var profile = Kafka(AuthenticationKind.KafkaOAuth,
            new KafkaSettings("broker.invalid:9093", true, OAuth: new KafkaOAuthSettings(endpoint, clientId, Extensions: extensions)));

        var codes = ProfileValidator.Validate(profile).Errors.Select(error => error.Code).ToArray();

        if (expected is null) Assert.Empty(codes); else Assert.Contains(expected, codes);
    }

    [Fact]
    public void OAuthSignInWithoutItsSettingsIsRefused()
    {
        var codes = ProfileValidator.Validate(Kafka(AuthenticationKind.KafkaOAuth, new KafkaSettings("broker.invalid:9093", true)))
            .Errors.Select(error => error.Code).ToArray();

        Assert.Contains("profile.kafka.oauth.endpoint", codes);
        Assert.Contains("profile.kafka.oauth.client", codes);
    }

    [Theory]
    [InlineData("eu-west-1", true, null)]
    [InlineData("us-gov-west-1", true, null)]
    [InlineData("Europe", true, "profile.kafka.aws_iam.region")]
    [InlineData("eu-west-1", false, "profile.kafka.aws_iam.tls")]
    public void AwsIamNeedsARegionAndTls(string region, bool useTls, string? expected)
    {
        var profile = Kafka(AuthenticationKind.KafkaAwsIam, new KafkaSettings("b-1.msk.invalid:9098", useTls, AwsIam: new KafkaAwsIamSettings(region)));

        var codes = ProfileValidator.Validate(profile).Errors.Select(error => error.Code).ToArray();

        if (expected is null) Assert.Empty(codes); else Assert.Contains(expected, codes);
    }

    [Fact]
    public void AnIamTokenOrTheReasonThereIsNoneIsReadyForLibrdkafka()
    {
        var signed = new KafkaAwsIamTokens(new KafkaAwsIamSettings("eu-west-1"), () => Task.FromResult(("synthetic-token", 1_800_000_000_000L)))
            .Fetch();
        var failed = new KafkaAwsIamTokens(new KafkaAwsIamSettings("eu-west-1"), () => throw new InvalidOperationException("no credentials"))
            .Fetch();

        Assert.Equal(("synthetic-token", 1_800_000_000_000L, (string?)null), signed);
        Assert.Null(failed.Token);
        Assert.Contains("no credentials", failed.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void TheListingChildReceivesTheSignedTokenThroughThePipe()
    {
        var request = new KafkaGroupListing.ListingRequest(new Dictionary<string, string> { ["sasl.mechanism"] = "OAUTHBEARER" }, 5000,
            new KafkaGroupListing.ListingToken("synthetic-token", 42, KafkaAwsIamTokens.Principal));

        var json = JsonSerializer.Serialize(request, KafkaGroupListing.ListingJson.Default.ListingRequest);
        var read = JsonSerializer.Deserialize(json, KafkaGroupListing.ListingJson.Default.ListingRequest)!;

        Assert.Equal(request.Token, read.Token);
        Assert.Null(JsonSerializer.Deserialize("""{"Config":{},"TimeoutMs":1}""", KafkaGroupListing.ListingJson.Default.ListingRequest)!.Token);
    }

    [Fact]
    public void TheEditorBuildsOAuthSettingsAndAsksForTheClientSecret()
    {
        var editor = new ProfileEditorViewModel(null) { Name = "Confluent Cloud" };
        editor.SelectedProvider = editor.ProviderOptions.Single(option => option.Provider == MessagingProvider.Kafka);
        editor.SelectedAuthentication = editor.AuthenticationOptions.Single(option => option.Kind == AuthenticationKind.KafkaOAuth);
        editor.KafkaBootstrapServers = "pkc-1.invalid:9092";
        editor.KafkaUseTls = true;
        editor.KafkaOAuthTokenEndpoint = " https://login.invalid/token ";
        editor.KafkaOAuthClientId = "client";
        editor.KafkaOAuthExtensions = "logicalCluster=lkc-1, identityPoolId=pool-2";

        Assert.True(editor.IsKafkaOAuth);
        Assert.False(editor.TryBuild(out _));
        Assert.Contains("client secret", editor.Error, StringComparison.Ordinal);

        editor.BrokerPassword = "synthetic";
        Assert.True(editor.TryBuild(out var result), editor.Error);
        Assert.Equal(new KafkaOAuthSettings("https://login.invalid/token", "client", null, "logicalCluster=lkc-1,identityPoolId=pool-2"),
            result!.Profile.Kafka!.OAuth);
        Assert.Null(result.Profile.Kafka.AwsIam);
        Assert.Equal("synthetic", result.ConnectionString);
    }

    [Fact]
    public void TheEditorBuildsAwsIamSettingsWithoutASecret()
    {
        var editor = new ProfileEditorViewModel(null) { Name = "MSK" };
        editor.SelectedProvider = editor.ProviderOptions.Single(option => option.Provider == MessagingProvider.Kafka);
        editor.SelectedAuthentication = editor.AuthenticationOptions.Single(option => option.Kind == AuthenticationKind.KafkaAwsIam);
        editor.KafkaBootstrapServers = "b-1.msk.invalid:9098";
        editor.KafkaUseTls = true;
        editor.KafkaAwsRegion = "eu-west-1";
        editor.KafkaAwsProfileName = " ";

        Assert.True(editor.TryBuild(out var result), editor.Error);

        Assert.Equal(new KafkaAwsIamSettings("eu-west-1"), result!.Profile.Kafka!.AwsIam);
        Assert.Null(result.Profile.Kafka.OAuth);
        Assert.Null(result.ConnectionString);
    }
}
