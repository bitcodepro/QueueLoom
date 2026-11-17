using System.Net;
using Azure.Core.Pipeline;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Infrastructure.Azure;

internal static class AzureProxy
{
    /// <summary>An HTTP transport through the environment's proxy, or null to keep the SDK's own (the computer's proxy).</summary>
    public static HttpClientTransport? Transport(IWebProxy? proxy)
    {
        if (proxy is null)
        {
            return null;
        }
        var handler = new SocketsHttpHandler();
        ProxyFactory.Apply(handler, proxy);
        return new HttpClientTransport(handler);
    }
}
