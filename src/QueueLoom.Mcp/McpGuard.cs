using ModelContextProtocol;
using QueueLoom.Core.Diagnostics;

namespace QueueLoom.Mcp;

internal static class McpGuard
{
    /// <summary>Turns failures into redacted messages the model can act on, instead of a generic tool error.</summary>
    public static async Task<T> RunAsync<T>(Func<Task<T>> action)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (McpException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw new McpException(SensitiveDataRedactor.SummarizeException(exception), exception);
        }
    }
}
