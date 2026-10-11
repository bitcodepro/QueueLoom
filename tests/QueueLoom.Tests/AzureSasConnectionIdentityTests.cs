using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class AzureSasConnectionIdentityTests
{
    private const string SyntheticSas = "Endpoint=sb://sas-a.servicebus.windows.net/;SharedAccessKeyName=TestOnly;SharedAccessKey=c3ludGhldGlj;";
    private const string SyntheticEmulatorSas = "Endpoint=sb://localhost;SharedAccessKeyName=TestOnly;SharedAccessKey=c3ludGhldGlj;UseDevelopmentEmulator=true;";

    [Theory]
    [InlineData(SyntheticSas)]
    [InlineData(SyntheticEmulatorSas)]
    public async Task AzureSasIdentity_ConnectRejectsNamespaceMismatchBeforeAnyNetworkRequest(string secret)
    {
        using var directory = new TemporaryDirectory();
        using var cancellation = new CancellationTokenSource();
        await using var workspace = new AzureServiceBusWorkspace(new CancelAfterReadVault(secret, cancellation),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        var profile = ServiceBusProfile.CreateNew("SAS", EnvironmentKind.Development,
            AuthenticationSettings.ConnectionString(), "sas-b.servicebus.windows.net");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.ConnectAsync(profile, cancellation.Token));

        Assert.Contains("does not match", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("c3ludGhldGlj", error.Message, StringComparison.Ordinal);
        Assert.Null(workspace.ConnectedProfileId);
        Assert.Null(workspace.ConnectedNamespace);
        Assert.Equal(WorkspaceConnectionState.Faulted, workspace.ConnectionState);
    }

    [Theory]
    [InlineData("sas-a.servicebus.windows.net", SyntheticSas)]
    [InlineData(" SAS-A.SERVICEBUS.WINDOWS.NET ", SyntheticSas)]
    [InlineData("sb://SAS-A.SERVICEBUS.WINDOWS.NET/", SyntheticSas)]
    [InlineData(null, SyntheticSas)]
    [InlineData("", SyntheticSas)]
    [InlineData("localhost", SyntheticEmulatorSas)]
    [InlineData(null, SyntheticEmulatorSas)]
    public async Task AzureSasIdentity_MatchingLegacyAndEmulatorProfilesReachCancelledNamespaceProbe(string? namespaceValue, string secret)
    {
        using var directory = new TemporaryDirectory();
        using var cancellation = new CancellationTokenSource();
        await using var workspace = new AzureServiceBusWorkspace(new CancelAfterReadVault(secret, cancellation),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        var profile = ServiceBusProfile.CreateNew("SAS", EnvironmentKind.Development,
            AuthenticationSettings.ConnectionString(), namespaceValue) with { EmulatorManagementPort = 6300 };

        // Cancellation happens after the lifecycle gate and vault read, before any SDK network request.
        // Passing this validation must leave the existing SDK namespace probe as the next step.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => workspace.ConnectAsync(profile, cancellation.Token));

        Assert.Null(workspace.ConnectedProfileId);
        Assert.Equal(WorkspaceConnectionState.Faulted, workspace.ConnectionState);
    }

    private sealed class CancelAfterReadVault(string secret, CancellationTokenSource cancellation) : ISecretVault
    {
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default)
        {
            cancellation.Cancel();
            return ValueTask.FromResult<string?>(secret);
        }

        public ValueTask StoreAsync(ProfileSecretKey key, string value, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
