namespace QueueLoom.Core.Abstractions;

public readonly record struct ProfileSecretKey(Guid ProfileId, ProfileSecretKind Kind)
{
    public static ProfileSecretKey ConnectionString(Guid profileId) =>
        new(profileId, ProfileSecretKind.ConnectionString);

    public static ProfileSecretKey SchemaRegistryPassword(Guid profileId) =>
        new(profileId, ProfileSecretKind.SchemaRegistryPassword);
}
