using System.Collections.ObjectModel;
using QueueLoom.App.Commands;
using QueueLoom.Core.Settings;

namespace QueueLoom.App.ViewModels;

public sealed partial class MainWindowViewModel
{
    private IReadOnlyList<FavoriteEntity> _favoriteEntities = [];
    private IReadOnlyList<FavoriteEntity> _recentEntities = [];
    private bool _showFavoritesOnly;
    private RelayCommand<EntityItemViewModel>? _toggleFavoriteCommand;
    private RelayCommand<EntityItemViewModel>? _openRecentEntityCommand;

    /// <summary>Favourite entities of every environment; a change is saved with the settings.</summary>
    public IReadOnlyList<FavoriteEntity> FavoriteEntities => _favoriteEntities;

    /// <summary>Recently opened entities of every environment, newest first; a change is saved with the settings.</summary>
    public IReadOnlyList<FavoriteEntity> RecentEntities => _recentEntities;

    /// <summary>The connected environment's recent entities that exist in its current topology.</summary>
    public ObservableCollection<EntityItemViewModel> RecentEntityItems { get; } = [];

    public bool HasRecentEntities => RecentEntityItems.Count > 0;

    public bool ShowFavoritesOnly
    {
        get => _showFavoritesOnly;
        set
        {
            if (SetProperty(ref _showFavoritesOnly, value))
            {
                ApplyEntityFilter();
            }
        }
    }

    public RelayCommand<EntityItemViewModel> ToggleFavoriteCommand => _toggleFavoriteCommand ??= new(entity =>
    {
        if (entity is null || ConnectedProfileId is not { } profileId)
        {
            return;
        }
        var favorite = new FavoriteEntity(profileId, entity.Reference);
        _favoriteEntities = _favoriteEntities.Contains(favorite)
            ? _favoriteEntities.Where(item => item != favorite).ToArray()
            : [.. _favoriteEntities, favorite];
        OnPropertyChanged(nameof(FavoriteEntities));
        ApplyEntityFilter();
    }, entity => entity is not null);

    /// <summary>Selects a recent entity in Explorer, clearing a search or filter that hides it.</summary>
    public RelayCommand<EntityItemViewModel> OpenRecentEntityCommand => _openRecentEntityCommand ??= new(entity =>
    {
        if (entity is null)
        {
            return;
        }
        if (!Entities.Contains(entity))
        {
            _showFavoritesOnly = false;
            OnPropertyChanged(nameof(ShowFavoritesOnly));
            SearchText = string.Empty;
            ApplyEntityFilter();
        }
        SelectedEntity = Entities.FirstOrDefault(item => item.Reference == entity.Reference);
    }, entity => entity is not null);

    private void LoadFavorites(AppSettings settings)
    {
        _favoriteEntities = settings.FavoriteEntities;
        _recentEntities = settings.RecentEntities;
        ApplyEntityFilter();
    }

    // The environment Explorer shows, not the workspace's connection: a background monitor check of another
    // environment connects the workspace there for a moment while this one stays on screen.
    private bool IsFavorite(EntityItemViewModel entity) =>
        ConnectedProfileId is { } profileId && _favoriteEntities.Contains(new FavoriteEntity(profileId, entity.Reference));

    /// <summary>Favourites first (each group in the chosen order), or only favourites.</summary>
    private IEnumerable<EntityItemViewModel> PinFavorites(IEnumerable<EntityItemViewModel> ordered)
    {
        var list = ordered.ToList();
        foreach (var entity in _allEntities)
        {
            entity.IsFavorite = IsFavorite(entity);
        }
        var favorites = list.Where(entity => entity.IsFavorite);
        return ShowFavoritesOnly ? favorites : favorites.Concat(list.Where(entity => !entity.IsFavorite));
    }

    /// <summary>Records that an entity was opened, for the Recent list.</summary>
    private void RememberEntity(EntityItemViewModel entity)
    {
        if (ConnectedProfileId is not { } profileId)
        {
            return;
        }
        var opened = new FavoriteEntity(profileId, entity.Reference);
        if (_recentEntities.Count > 0 && _recentEntities[0] == opened)
        {
            return;
        }
        _recentEntities = (AppSettings.Default with { RecentEntities = FavoriteEntity.Remember(_recentEntities, opened) })
            .Normalize().RecentEntities;
        OnPropertyChanged(nameof(RecentEntities));
        RefreshRecentEntityItems();
    }

    private void RefreshRecentEntityItems()
    {
        RecentEntityItems.Clear();
        if (ConnectedProfileId is { } profileId)
        {
            foreach (var recent in _recentEntities.Where(item => item.ProfileId == profileId))
            {
                if (_allEntities.FirstOrDefault(entity => entity.Reference == recent.Entity) is { } entity)
                {
                    RecentEntityItems.Add(entity);
                }
            }
        }
        OnPropertyChanged(nameof(HasRecentEntities));
    }
}
