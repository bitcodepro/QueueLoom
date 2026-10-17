using Azure.Messaging.ServiceBus;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

// Networks that allow only HTTPS block AMQP's port 5671. Azure Service Bus also speaks AMQP over WebSockets on port 443;
// an Azure environment can now choose it.
public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false, ServiceBusTransportType.AmqpTcp)]
    [InlineData(true, ServiceBusTransportType.AmqpWebSockets)]
    public void AzureWebSockets_ChoosesTheMessagingTransport(bool webSockets, ServiceBusTransportType expected)
    {
        Assert.Equal(expected, AzureServiceBusWorkspace.CreateMessagingClientOptions(webSockets).TransportType);
    }

    [Theory]
    [InlineData(AuthenticationKind.ConnectionString)]
    [InlineData(AuthenticationKind.EntraId)]
    public void AzureWebSockets_TheEditorKeepsTheChoiceForBothSignInKinds(AuthenticationKind kind)
    {
        var original = kind == AuthenticationKind.ConnectionString
            ? CreateConnectionStringProfile("SAS") with { FullyQualifiedNamespace = "sas-a.servicebus.windows.net" }
            : CreateConnectionStringProfile("Entra") with
            {
                FullyQualifiedNamespace = "contoso.servicebus.windows.net",
                Authentication = new AuthenticationSettings(AuthenticationKind.EntraId,
                    new EntraIdSettings(EntraIdCredentialKind.DefaultAzureCredential, null, null))
            };
        var editor = new ProfileEditorViewModel(original) { UseAmqpWebSockets = true };

        Assert.True(editor.TryBuild(out var result), editor.Error);
        Assert.True(result!.Profile.UseAmqpWebSockets);
        Assert.True(new ProfileEditorViewModel(result.Profile).UseAmqpWebSockets);
    }

    // Saved environments and scheduled resends carry a hash of the environment's settings. An environment without the
    // option must hash as before, or every scheduled resend saved before this version would count as changed.
    [Fact]
    public void AzureWebSockets_LeavesTheSettingsOfEnvironmentsWithoutItUnchanged()
    {
        var profile = CreateConnectionStringProfile("SAS");

        Assert.DoesNotContain("WebSockets", System.Text.Json.JsonSerializer.Serialize(profile), StringComparison.OrdinalIgnoreCase);
        Assert.NotEqual(ScheduledResend.IdentityFor(profile), ScheduledResend.IdentityFor(profile with { UseAmqpWebSockets = true }));
    }

    [Fact]
    public void AzureWebSockets_IsKeptByExportAndImport()
    {
        var profile = CreateConnectionStringProfile("Staging") with { UseAmqpWebSockets = true };

        var imported = EnvironmentTransfer.Import(EnvironmentTransfer.Export([profile]), []);

        Assert.True(Assert.Single(imported.Profiles).UseAmqpWebSockets);
    }

    [Theory]
    [InlineData(MessagingProvider.AzureServiceBus, true)]
    [InlineData(MessagingProvider.RabbitMq, false)]
    public void AzureWebSockets_IsAnAzureOnlyOption(MessagingProvider provider, bool valid)
    {
        var profile = provider == MessagingProvider.AzureServiceBus
            ? CreateConnectionStringProfile("Azure") with { UseAmqpWebSockets = true }
            : ServiceBusProfile.CreateNew("Rabbit", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.RabbitMqPassword)) with
            {
                Provider = MessagingProvider.RabbitMq,
                RabbitMq = new RabbitMqSettings("localhost", "guest", "/", 5672, 15672),
                UseAmqpWebSockets = true
            };

        Assert.Equal(valid, ProfileValidator.Validate(profile).IsValid);
    }
}
