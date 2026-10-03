using QueueLoom.Core.Profiles;

namespace QueueLoom.Core.Abstractions;

/// <summary>Reads a credential only while its supplied profile still matches committed metadata.</summary>
public interface IProfileSecretReader
{
    ValueTask<string?> RetrieveForProfileAsync(ServiceBusProfile profile, ProfileSecretKind kind, CancellationToken cancellationToken = default);
}

public static class ProfileSecretReader
{
    public static ValueTask<string?> RetrieveForProfileAsync(this ISecretVault vault, ServiceBusProfile profile,
        ProfileSecretKind kind, CancellationToken cancellationToken = default) =>
        vault is IProfileSecretReader reader
            ? reader.RetrieveForProfileAsync(profile, kind, cancellationToken)
            : vault.RetrieveAsync(new ProfileSecretKey(profile.Id, kind), cancellationToken);
}
