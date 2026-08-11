using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;

namespace QueueLoom.Tests;

public sealed class ProfileValidatorTests
{
    [Fact]
    public void ConnectionStringProfile_DoesNotPersistOrRequireNamespace()
    {
        var profile = new ServiceBusProfile(
            Guid.NewGuid(),
            "Local dev",
            EnvironmentKind.Development,
            null,
            null,
            AuthenticationSettings.ConnectionString(),
            ProfileAccessMode.ReadWrite);

        var result = ProfileValidator.Validate(profile);

        Assert.True(result.IsValid);
    }

    [Fact]
    public void EntraIdProfile_RequiresBareNamespace()
    {
        var profile = new ServiceBusProfile(
            Guid.NewGuid(),
            "Production",
            EnvironmentKind.Production,
            null,
            "sb://orders.servicebus.windows.net/",
            AuthenticationSettings.Entra(),
            ProfileAccessMode.ReadOnly);

        var result = ProfileValidator.Validate(profile);

        Assert.True(result.HasError("profile.namespace.invalid"));
    }

    [Fact]
    public void CustomEnvironment_RequiresName()
    {
        var profile = new ServiceBusProfile(
            Guid.NewGuid(),
            "Sandbox",
            EnvironmentKind.Custom,
            " ",
            null,
            AuthenticationSettings.ConnectionString());

        var result = ProfileValidator.Validate(profile);

        Assert.True(result.HasError("profile.environment_name.required"));
    }

    [Fact]
    public void ProductionProfile_CannotPersistWriteAccess()
    {
        var profile = new ServiceBusProfile(
            Guid.NewGuid(),
            "Production",
            EnvironmentKind.Production,
            null,
            "orders.servicebus.windows.net",
            AuthenticationSettings.Entra(),
            ProfileAccessMode.ReadWrite);

        var result = ProfileValidator.Validate(profile);

        Assert.True(result.HasError("profile.production.read_only"));
    }

    [Theory]
    [InlineData("sb://Orders.ServiceBus.Windows.Net/", "orders.servicebus.windows.net")]
    [InlineData(" orders.servicebus.windows.net ", "orders.servicebus.windows.net")]
    public void NamespaceNormalization_RemovesTransportDecoration(string input, string expected)
    {
        Assert.Equal(expected, ProfileValidator.NormalizeFullyQualifiedNamespace(input));
    }
}
