using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests.Infrastructure;

public sealed class JsonProfileRepositoryTests
{
    [Fact]
    public async Task CrudAndSelectedProfile_RoundTripAcrossRepositoryInstances()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(temporaryDirectory.Path);
        var development = ServiceBusProfile.CreateNew(
            "Development",
            EnvironmentKind.Development,
            AuthenticationSettings.ConnectionString(),
            accessMode: ProfileAccessMode.ReadWrite);
        var production = ServiceBusProfile.CreateNew(
            "Production",
            EnvironmentKind.Production,
            AuthenticationSettings.Entra(),
            "orders.servicebus.windows.net");
        var updatedProduction = production with
        {
            Name = "Production read-only",
            AccessMode = ProfileAccessMode.ReadOnly
        };

        using (var repository = new JsonProfileRepository(paths))
        {
            Assert.Empty(await repository.ListAsync());
            Assert.Null(await repository.GetAsync(development.Id));
            Assert.Null(await repository.GetSelectedProfileIdAsync());

            await repository.UpsertAsync(production);
            await repository.UpsertAsync(development);

            Assert.Equal(development, await repository.GetAsync(development.Id));
            Assert.Equal(
                [development, production],
                await repository.ListAsync());

            await repository.SetSelectedProfileIdAsync(production.Id);
            Assert.Equal(production.Id, await repository.GetSelectedProfileIdAsync());

            await repository.UpsertAsync(updatedProduction);
            Assert.Equal(updatedProduction, await repository.GetAsync(production.Id));
        }

        using (var repository = new JsonProfileRepository(paths))
        {
            Assert.Equal(updatedProduction, await repository.GetAsync(production.Id));
            Assert.Equal(production.Id, await repository.GetSelectedProfileIdAsync());

            Assert.True(await repository.DeleteAsync(development.Id));
            Assert.False(await repository.DeleteAsync(development.Id));
            Assert.Equal(production.Id, await repository.GetSelectedProfileIdAsync());

            Assert.True(await repository.DeleteAsync(production.Id));
            Assert.Empty(await repository.ListAsync());
            Assert.Null(await repository.GetSelectedProfileIdAsync());
        }
    }

    [Fact]
    public async Task SetSelectedProfile_RejectsUnknownProfile()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var repository = new JsonProfileRepository(
            QueueLoomPaths.ForRoot(temporaryDirectory.Path));

        await Assert.ThrowsAsync<ArgumentException>(
            () => repository.SetSelectedProfileIdAsync(Guid.NewGuid()));
    }

    [Fact]
    public async Task ManuallyEnabledProductionProfile_IsLoadedReadOnly()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(temporaryDirectory.Path);
        var production = ServiceBusProfile.CreateNew(
            "Production",
            EnvironmentKind.Production,
            AuthenticationSettings.Entra(),
            "orders.servicebus.windows.net",
            accessMode: ProfileAccessMode.ReadOnly);

        using (var repository = new JsonProfileRepository(paths))
        {
            await repository.UpsertAsync(production);
        }

        var json = await File.ReadAllTextAsync(paths.ProfilesFile);
        Assert.Contains("\"accessMode\": \"ReadOnly\"", json, StringComparison.Ordinal);
        await File.WriteAllTextAsync(
            paths.ProfilesFile,
            json.Replace(
                "\"accessMode\": \"ReadOnly\"",
                "\"accessMode\": \"ReadWrite\"",
                StringComparison.Ordinal));

        using var reopened = new JsonProfileRepository(paths);
        var loaded = Assert.Single(await reopened.ListAsync());
        Assert.Equal(ProfileAccessMode.ReadOnly, loaded.AccessMode);
    }

    [Fact]
    public async Task ConcurrentRepositoryInstances_DoNotLoseProfiles()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(temporaryDirectory.Path);
        using var first = new JsonProfileRepository(paths);
        using var second = new JsonProfileRepository(paths);
        var profiles = Enumerable.Range(0, 20)
            .Select(index => ServiceBusProfile.CreateNew(
                $"Development {index}",
                EnvironmentKind.Development,
                AuthenticationSettings.ConnectionString(),
                accessMode: ProfileAccessMode.ReadOnly))
            .ToArray();

        await Task.WhenAll(profiles.Select((profile, index) =>
            (index % 2 == 0 ? first : second).UpsertAsync(profile)));

        Assert.Equal(profiles.Length, (await first.ListAsync()).Count);
    }

    [Fact]
    public async Task NullProfilesDocument_ProducesControlledDataError()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(temporaryDirectory.Path);
        paths.EnsureCreated();
        await File.WriteAllTextAsync(paths.ProfilesFile, "{\"schemaVersion\":1,\"profiles\":null}");
        using var repository = new JsonProfileRepository(paths);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(() => repository.ListAsync());

        Assert.Contains("damaged", exception.Message, StringComparison.OrdinalIgnoreCase);
    }
}
