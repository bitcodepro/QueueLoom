namespace QueueLoom.Core.Profiles;

/// <summary>Non-secret settings of a Google Cloud Pub/Sub environment.</summary>
/// <param name="ProjectId">The Google Cloud project that owns the topics and subscriptions.</param>
/// <param name="EmulatorHost">Optional host:port of a local Pub/Sub emulator, for example localhost:8085.</param>
public sealed record GooglePubSubSettings(
    string ProjectId,
    string? EmulatorHost = null);
