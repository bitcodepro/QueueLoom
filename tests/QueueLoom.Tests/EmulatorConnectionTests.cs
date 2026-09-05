using QueueLoom.Infrastructure.Azure;
using Azure.Messaging.ServiceBus;

namespace QueueLoom.Tests;

public sealed class EmulatorConnectionTests
{
    private const string Local = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    [Fact]
    public void AdminUsesSeparatePortWhileDataConnectionRemainsUnchanged()
    {
        var value = EmulatorConnection.AdministrationConnectionString(Local, 5301);
        Assert.Equal(5301, ServiceBusConnectionStringProperties.Parse(value).Endpoint.Port);
        Assert.DoesNotContain("5301", Local);
        Assert.True(EmulatorConnection.IsEmulator(value));
    }

    [Fact]
    public void CloudConnectionRemainsByteForByteUnchanged()
    {
        var cloud = "Endpoint=sb://example.servicebus.windows.net;SharedAccessKeyName=owner;SharedAccessKey=not-a-real-key;";
        Assert.Equal(cloud, EmulatorConnection.AdministrationConnectionString(cloud));
    }
}
