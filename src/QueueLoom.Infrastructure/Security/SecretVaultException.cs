namespace QueueLoom.Infrastructure.Security;

public class SecretVaultException : Exception
{
    public SecretVaultException(string message) : base(message)
    {
    }

    public SecretVaultException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

public sealed class SecureStoreUnavailableException : SecretVaultException
{
    public SecureStoreUnavailableException(string message) : base(message)
    {
    }

    public SecureStoreUnavailableException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
