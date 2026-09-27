namespace QueueLoom.Core.Profiles;

/// <summary>Non-secret settings of an Amazon SQS / SNS environment.</summary>
/// <param name="Region">AWS region, for example eu-west-1.</param>
/// <param name="ServiceUrl">Optional endpoint override, for example http://localhost:4566 for LocalStack.</param>
/// <param name="ProfileName">Optional named profile from the shared AWS credentials file.</param>
public sealed record AwsSettings(
    string Region,
    string? ServiceUrl = null,
    string? ProfileName = null);
