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
    GoogleApplicationDefault,

    /// <summary>A RabbitMQ user name and password; the password is kept in the vault.</summary>
    RabbitMqPassword,

    /// <summary>A Kafka cluster without authentication (development clusters, private networks).</summary>
    KafkaNone,

    /// <summary>Kafka SASL (PLAIN or SCRAM) with a user name and password; the password is kept in the vault.</summary>
    KafkaSaslPassword
}

public static class AuthenticationKinds
{
    public static MessagingProvider Provider(this AuthenticationKind kind) => kind switch
    {
        AuthenticationKind.AwsAccessKey or AuthenticationKind.AwsDefaultCredentials => MessagingProvider.AmazonSqsSns,
        AuthenticationKind.GoogleServiceAccountKey or AuthenticationKind.GoogleApplicationDefault => MessagingProvider.GooglePubSub,
        AuthenticationKind.RabbitMqPassword => MessagingProvider.RabbitMq,
        AuthenticationKind.KafkaNone or AuthenticationKind.KafkaSaslPassword => MessagingProvider.Kafka,
        _ => MessagingProvider.AzureServiceBus
    };

    /// <summary>Whether this method keeps a secret (connection string, access key or key file) in the vault.</summary>
    public static bool UsesStoredSecret(this AuthenticationKind kind) =>
        kind is AuthenticationKind.ConnectionString or AuthenticationKind.AwsAccessKey or AuthenticationKind.GoogleServiceAccountKey
            or AuthenticationKind.RabbitMqPassword or AuthenticationKind.KafkaSaslPassword;

    public static IReadOnlyList<AuthenticationKind> For(MessagingProvider provider) => provider switch
    {
        MessagingProvider.AmazonSqsSns => [AuthenticationKind.AwsAccessKey, AuthenticationKind.AwsDefaultCredentials],
        MessagingProvider.GooglePubSub => [AuthenticationKind.GoogleServiceAccountKey, AuthenticationKind.GoogleApplicationDefault],
        MessagingProvider.RabbitMq => [AuthenticationKind.RabbitMqPassword],
        MessagingProvider.Kafka => [AuthenticationKind.KafkaNone, AuthenticationKind.KafkaSaslPassword],
        _ => [AuthenticationKind.EntraId, AuthenticationKind.ConnectionString]
    };
}
