using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Settings;

/// <summary>A queue, topic or subscription of one environment, marked as a favourite or recently opened.</summary>
public sealed record FavoriteEntity(Guid ProfileId, ServiceBusEntityReference Entity)
{
    public const int MaximumFavorites = 500;
    public const int MaximumRecentPerEnvironment = 10;

    /// <summary>The recent list with <paramref name="opened"/> first and not repeated further down.</summary>
    public static IReadOnlyList<FavoriteEntity> Remember(IEnumerable<FavoriteEntity> recent, FavoriteEntity opened)
    {
        ArgumentNullException.ThrowIfNull(recent);
        ArgumentNullException.ThrowIfNull(opened);
        return [opened, .. recent.Where(entity => entity != opened)];
    }

    internal static IReadOnlyList<FavoriteEntity> NormalizeRecent(IEnumerable<FavoriteEntity>? recent)
    {
        var perEnvironment = new Dictionary<Guid, int>();
        return Valid(recent)
            .Where(entity => (perEnvironment[entity.ProfileId] = perEnvironment.GetValueOrDefault(entity.ProfileId) + 1)
                             <= MaximumRecentPerEnvironment)
            .ToArray();
    }

    internal static IReadOnlyList<FavoriteEntity> NormalizeFavorites(IEnumerable<FavoriteEntity>? favorites) =>
        Valid(favorites).Take(MaximumFavorites).ToArray();

    private static IEnumerable<FavoriteEntity> Valid(IEnumerable<FavoriteEntity>? entities) =>
        (entities ?? []).Where(entity => entity?.Entity is not null && entity.ProfileId != Guid.Empty).Distinct();
}
