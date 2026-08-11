using Azure.Messaging.ServiceBus;

namespace QueueLoom.Infrastructure.Azure;

public static class ServiceBusConnectionStringInspector
{
    public static bool TryGetNamespace(
        string? connectionString,
        out string fullyQualifiedNamespace,
        out string? error)
    {
        fullyQualifiedNamespace = string.Empty;
        error = null;

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            error = "Connection string is required.";
            return false;
        }

        try
        {
            var properties = ServiceBusConnectionStringProperties.Parse(connectionString);
            if (!string.IsNullOrWhiteSpace(properties.EntityPath))
            {
                error = "Use a namespace-level connection string without EntityPath.";
                return false;
            }

            fullyQualifiedNamespace = properties.FullyQualifiedNamespace;
            return true;
        }
        catch (FormatException)
        {
            error = "The Azure Service Bus connection string has an invalid format.";
            return false;
        }
        catch (ArgumentException)
        {
            error = "The Azure Service Bus connection string is incomplete.";
            return false;
        }
    }
}
