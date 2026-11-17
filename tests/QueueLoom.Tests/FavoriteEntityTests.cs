using QueueLoom.App.Models;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class FavoriteEntityTests
{
    private static readonly Guid Profile = Guid.NewGuid();

    [Fact]
    public async Task FavouritesAndRecentEntitiesAreSavedWithTheSettings()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var store = new JsonAppSettingsStore(paths);
        var favorite = new FavoriteEntity(Profile, ServiceBusEntityReference.Subscription("orders", "audit"));
        var recent = new FavoriteEntity(Profile, ServiceBusEntityReference.Queue("payments"));

        await store.UpdateAsync(settings => settings with { FavoriteEntities = [favorite], RecentEntities = [recent] });
        var loaded = await store.LoadAsync();

        Assert.Equal([favorite], loaded.FavoriteEntities);
        Assert.Equal([recent], loaded.RecentEntities);
    }

    [Fact]
    public void RecentEntitiesKeepTheNewestTenOfEachEnvironmentWithoutRepeats()
    {
        var other = Guid.NewGuid();
        var recent = Enumerable.Range(0, 12).Select(index => new FavoriteEntity(Profile, ServiceBusEntityReference.Queue($"q{index}")))
            .Append(new FavoriteEntity(Profile, ServiceBusEntityReference.Queue("q0")))
            .Append(new FavoriteEntity(other, ServiceBusEntityReference.Queue("q0")))
            .ToArray();

        var normalized = (AppSettings.Default with { RecentEntities = recent, FavoriteEntities = [recent[0], recent[0]] }).Normalize();

        Assert.Equal(10, normalized.RecentEntities.Count(entity => entity.ProfileId == Profile));
        Assert.Single(normalized.RecentEntities, entity => entity.ProfileId == other);
        Assert.Equal("q0", normalized.RecentEntities[0].Entity.Name);
        Assert.Single(normalized.FavoriteEntities);
    }

    [Fact]
    public void RememberingAnEntityPutsItFirst()
    {
        var first = new FavoriteEntity(Profile, ServiceBusEntityReference.Queue("a"));
        var second = new FavoriteEntity(Profile, ServiceBusEntityReference.Queue("b"));

        Assert.Equal([second, first], FavoriteEntity.Remember([first], second));
        Assert.Equal([first, second], FavoriteEntity.Remember([second, first], first));
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task FavouritesComeFirstInExplorerAndCanBeShownAlone()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(
                DateTimeOffset.UtcNow,
                [
                    new ServiceBusQueue("alpha", new ServiceBusEntityRuntime(new ServiceBusMessageCounts())),
                    new ServiceBusQueue("beta", new ServiceBusEntityRuntime(new ServiceBusMessageCounts())),
                    new ServiceBusQueue("gamma", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))
                ])
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        var changes = new List<string?>();
        viewModel.PropertyChanged += (_, args) => changes.Add(args.PropertyName);

        var gamma = viewModel.Entities.Single(entity => entity.Name == "gamma");
        viewModel.ToggleFavoriteCommand.Execute(gamma);

        Assert.True(gamma.IsFavorite);
        Assert.Equal(["gamma", "alpha", "beta"], viewModel.Entities.Select(entity => entity.Name));
        Assert.Equal([new FavoriteEntity(profile.Id, ServiceBusEntityReference.Queue("gamma"))], viewModel.FavoriteEntities);
        Assert.Contains(nameof(MainWindowViewModel.FavoriteEntities), changes);

        viewModel.ShowFavoritesOnly = true;
        Assert.Equal(["gamma"], viewModel.Entities.Select(entity => entity.Name));

        viewModel.ToggleFavoriteCommand.Execute(gamma);
        Assert.False(gamma.IsFavorite);
        Assert.Empty(viewModel.FavoriteEntities);
        Assert.Empty(viewModel.Entities);
    }

    [Fact]
    public async Task AFavouriteBelongsToTheShownEnvironmentWhileAMonitorReadsAnother()
    {
        var shown = CreateProfile("Development", EnvironmentKind.Development);
        var monitored = CreateProfile("Test", EnvironmentKind.Test);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(
                DateTimeOffset.UtcNow,
                [
                    new ServiceBusQueue("alpha", new ServiceBusEntityRuntime(new ServiceBusMessageCounts())),
                    new ServiceBusQueue("beta", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))
                ])
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([shown, monitored], shown.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();

        // A background monitor check connects the workspace to another environment; Explorer still shows this one.
        await workspace.ConnectAsync(monitored);
        var beta = viewModel.Entities.Single(entity => entity.Name == "beta");
        viewModel.ToggleFavoriteCommand.Execute(beta);

        Assert.Equal([new FavoriteEntity(shown.Id, ServiceBusEntityReference.Queue("beta"))], viewModel.FavoriteEntities);
        Assert.True(beta.IsFavorite);
        Assert.Equal(["beta", "alpha"], viewModel.Entities.Select(entity => entity.Name));
    }

    [Fact]
    public async Task StoredFavouritesAndRecentEntitiesApplyToTheirOwnEnvironment()
    {
        var profile = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(
                DateTimeOffset.UtcNow,
                [
                    new ServiceBusQueue("alpha", new ServiceBusEntityRuntime(new ServiceBusMessageCounts())),
                    new ServiceBusQueue("beta", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))
                ])
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        viewModel.ApplyPreferences(AppSettings.Default with
        {
            FavoriteEntities =
            [
                new FavoriteEntity(profile.Id, ServiceBusEntityReference.Queue("beta")),
                new FavoriteEntity(Guid.NewGuid(), ServiceBusEntityReference.Queue("alpha"))
            ],
            RecentEntities = [new FavoriteEntity(profile.Id, ServiceBusEntityReference.Queue("alpha"))]
        });
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();

        Assert.Equal(["beta", "alpha"], viewModel.Entities.Select(entity => entity.Name));
        Assert.Equal(["alpha"], viewModel.RecentEntityItems.Select(entity => entity.Name));

        viewModel.OpenRecentEntityCommand.Execute(viewModel.RecentEntityItems[0]);
        Assert.Equal("alpha", viewModel.SelectedEntity?.Name);
    }
}
