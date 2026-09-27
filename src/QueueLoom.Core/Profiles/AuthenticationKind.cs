namespace QueueLoom.Core.Profiles;

public enum AuthenticationKind
{
    ConnectionString,
    EntraId,

    /// <summary>AWS access key ID and secret access key, kept in the secret vault.</summary>
    AwsAccessKey,

    /// <summary>The standard AWS credential chain: environment variables, shared credentials file or SSO.</summary>
    AwsDefaultCredentials,

    /// <summary>A Google service account JSON key, kept in the secret vault.</summary>
    GoogleServiceAccountKey,

    /// <summary>Google Application Default Credentials, for example from gcloud auth application-default login.</summary>
    GoogleApplicationDefault
}

public static class AuthenticationKinds
{
    public static MessagingProvider Provider(this AuthenticationKind kind) => kind switch
    {
        AuthenticationKind.AwsAccessKey or AuthenticationKind.AwsDefaultCredentials => MessagingProvider.AmazonSqsSns,
        AuthenticationKind.GoogleServiceAccountKey or AuthenticationKind.GoogleApplicationDefault => MessagingProvider.GooglePubSub,
        _ => MessagingProvider.AzureServiceBus
    };

    /// <summary>Whether this method keeps a secret (connection string, access key or key file) in the vault.</summary>
    public static bool UsesStoredSecret(this AuthenticationKind kind) =>
        kind is AuthenticationKind.ConnectionString or AuthenticationKind.AwsAccessKey or AuthenticationKind.GoogleServiceAccountKey;

    public static IReadOnlyList<AuthenticationKind> For(MessagingProvider provider) => provider switch
    {
        MessagingProvider.AmazonSqsSns => [AuthenticationKind.AwsAccessKey, AuthenticationKind.AwsDefaultCredentials],
        MessagingProvider.GooglePubSub => [AuthenticationKind.GoogleServiceAccountKey, AuthenticationKind.GoogleApplicationDefault],
        _ => [AuthenticationKind.EntraId, AuthenticationKind.ConnectionString]
    };
}
