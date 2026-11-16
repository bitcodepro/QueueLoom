namespace QueueLoom.Core.Profiles;

/// <summary>
/// OAuth 2.0 sign-in to Kafka (SASL/OAUTHBEARER) with the client credentials flow, as Confluent Cloud, Aiven, Redpanda
/// and Strimzi with Keycloak use. The client secret is kept in the vault.
/// </summary>
/// <param name="TokenEndpoint">The identity provider's token endpoint, for example https://login.example.com/oauth2/token.</param>
/// <param name="ClientId">The application (client) ID.</param>
/// <param name="Scope">Requested scopes, separated by spaces; empty when the provider needs none.</param>
/// <param name="Extensions">
/// SASL extensions as name=value pairs separated by commas, for example
/// logicalCluster=lkc-abc12,identityPoolId=pool-XyZ1 for Confluent Cloud.
/// </param>
public sealed record KafkaOAuthSettings(
    string TokenEndpoint,
    string ClientId,
    string? Scope = null,
    string? Extensions = null);

/// <summary>
/// AWS IAM sign-in to Amazon MSK. A token is signed with the standard AWS credential chain (environment variables,
/// shared credentials, SSO) or a named profile, and renewed before it expires.
/// </summary>
/// <param name="Region">The cluster's region, for example eu-west-1.</param>
/// <param name="ProfileName">A profile from the AWS credentials or config file; empty for the default chain.</param>
public sealed record KafkaAwsIamSettings(string Region, string? ProfileName = null);
