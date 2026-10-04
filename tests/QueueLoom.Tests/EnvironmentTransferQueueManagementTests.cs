using QueueLoom.Core.Profiles;

namespace QueueLoom.Tests;

public sealed class EnvironmentTransferQueueManagementTests
{
    private static ServiceBusProfile Managed() => ServiceBusProfile.CreateNew("Rabbit", EnvironmentKind.Test,
            new AuthenticationSettings(AuthenticationKind.RabbitMqPassword), accessMode: ProfileAccessMode.ReadWrite) with
    {
        Provider = MessagingProvider.RabbitMq,
        RabbitMq = new RabbitMqSettings("broker", "guest"),
        AllowQueueManagement = true
    };

    [Fact]
    public void Export_LeavesPermissionToManageQueuesOnThisComputer()
    {
        var json = EnvironmentTransfer.Export([Managed()]);

        Assert.DoesNotContain("allowQueueManagement", json, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("allowQueueManagement")]
    [InlineData("AllowQueueManagement")]
    public void Import_NeverTurnsOnQueueManagement(string member)
    {
        // A file written elsewhere (or edited by hand) cannot grant permission to delete queues on import.
        var json = EnvironmentTransfer.Export([Managed()])
            .Replace("\"name\": \"Rabbit\"", $"\"{member}\": true, \"name\": \"Rabbit\"", StringComparison.Ordinal);
        Assert.Contains(member, json, StringComparison.Ordinal);

        var imported = Assert.Single(EnvironmentTransfer.Import(json, []).Profiles);

        Assert.False(imported.AllowQueueManagement);
        Assert.Throws<InvalidOperationException>(imported.EnsureQueueManagementAllowed);
    }
}
