using System.Text.Json.Nodes;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class CycleThreeImportRegressionTests
{
    [Theory]
    [InlineData("Id", "AccessMode", true)]
    [InlineData("ID", "ACCESSMODE", true)]
    [InlineData("Id", "AccessMode", false)]
    [InlineData("ID", "ACCESSMODE", false)]
    public async Task CycleThreeImport_CaseVariantsCannotReuseLocalIdentityOrEnableWrites(string idKey, string accessKey, bool lowerFirst)
    {
        using var directory = new TemporaryDirectory();
        using var repository = new JsonProfileRepository(QueueLoomPaths.ForRoot(directory.Path));
        var existing = ServiceBusProfile.CreateNew("Local", EnvironmentKind.Test, AuthenticationSettings.ConnectionString(), "local.servicebus.windows.net");
        await repository.UpsertAndSelectAsync(existing);
        var root = JsonNode.Parse(EnvironmentTransfer.Export([existing]))!.AsObject();
        var node = root["environments"]!.AsArray()[0]!.AsObject();
        if (lowerFirst) { node["id"] = Guid.NewGuid(); node["accessMode"] = "ReadOnly"; }
        node[idKey] = existing.Id;
        node[accessKey] = "ReadWrite";
        node["ConfigurationRevision"] = Guid.NewGuid();
        if (!lowerFirst) { node["id"] = Guid.NewGuid(); node["accessMode"] = "ReadOnly"; }
        var imported = Assert.Single(EnvironmentTransfer.Import(root.ToJsonString(), [existing]).Profiles);
        Assert.NotEqual(existing.Id, imported.Id);
        Assert.Equal(ProfileAccessMode.ReadOnly, imported.AccessMode);
        Assert.Equal(Guid.Empty, imported.ConfigurationRevision);
        await repository.UpsertAsync(imported);
        Assert.Equal(existing, await repository.GetAsync(existing.Id));
        Assert.Equal(2, (await repository.ListAsync()).Count);
        // A fresh identity cannot address the existing environment's credential slot.
        var credentials = new Dictionary<ProfileSecretKey, string> { [ProfileSecretKey.ConnectionString(existing.Id)] = "synthetic-local-secret" };
        Assert.False(credentials.ContainsKey(ProfileSecretKey.ConnectionString(imported.Id)));
    }

    [Fact]
    public void CycleThreeImport_OrdinaryLegacyExportRemainsReadOnlyAndNeedsItsOwnSecret()
    {
        var original = ServiceBusProfile.CreateNew("Legacy", EnvironmentKind.Development, AuthenticationSettings.ConnectionString(), "legacy.servicebus.windows.net");
        var imported = EnvironmentTransfer.Import(EnvironmentTransfer.Export([original]), [original]);
        var profile = Assert.Single(imported.Profiles);
        Assert.NotEqual(original.Id, profile.Id);
        Assert.Equal(ProfileAccessMode.ReadOnly, profile.AccessMode);
        Assert.Equal("Legacy (2)", profile.Name);
        Assert.Equal(1, imported.NeedSecrets);
    }
}
