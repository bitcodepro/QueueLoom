using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;

namespace QueueLoom.Mcp;

/// <param name="ReadOnly">When true, only read tools are offered; nothing can change Service Bus.</param>
public sealed record McpServerSettings(bool ReadOnly = false);

/// <summary>Runs QueueLoom as an MCP server so LLM clients can inspect Service Bus and, with approval, change it.</summary>
public static class QueueLoomMcpServer
{
    public const string Instructions =
        "QueueLoom inspects Azure Service Bus namespaces saved by the user. Start with list_environments, then get_entities " +
        "or scan_dead_letters. peek_messages and search_dead_letters only read: they never lock or remove messages. " +
        "delete_dead_letter_messages, purge_dead_letters and send_message change Service Bus; each call is shown to the user, " +
        "who must approve it in QueueLoom before anything happens. Always pass a clear 'reason'. If a change is not approved, " +
        "report that to the user and do not retry it unasked.";

    /// <summary>
    /// Runs until the client disconnects or <paramref name="cancellationToken"/> fires.
    /// </summary>
    /// <param name="configureServices">Registers IProfileRepository, IServiceBusWorkspace, IOperationApprover and optionally IActivityJournal.</param>
    /// <param name="input">Client-to-server stream; stdin when null.</param>
    /// <param name="output">Server-to-client stream; stdout when null.</param>
    public static async Task RunAsync(
        McpServerSettings settings,
        Action<IServiceCollection> configureServices,
        Action<ILoggingBuilder>? configureLogging = null,
        Stream? input = null,
        Stream? output = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(configureServices);

        // The empty builder adds no console logger: stdout carries the protocol and must stay clean.
        var builder = Host.CreateEmptyApplicationBuilder(settings: null);
        builder.Logging.ClearProviders();
        configureLogging?.Invoke(builder.Logging);
        configureServices(builder.Services);
        builder.Services.AddSingleton<McpWorkspaceSession>();

        var mcp = builder.Services.AddMcpServer(options =>
        {
            options.ServerInfo = new Implementation
            {
                Name = "QueueLoom",
                Title = "QueueLoom — Azure Service Bus",
                Version = Assembly.GetEntryAssembly()?.GetName().Version?.ToString(3) ?? "0.0.0"
            };
            options.ServerInstructions = settings.ReadOnly
                ? Instructions + " This server was started read-only: change tools are not available."
                : Instructions;
        });
        mcp = input is not null && output is not null
            ? mcp.WithStreamServerTransport(input, output)
            : mcp.WithStdioServerTransport();
        mcp.WithTools<QueueLoomReadTools>();
        if (!settings.ReadOnly)
        {
            mcp.WithTools<QueueLoomChangeTools>();
        }

        using var host = builder.Build();
        await host.RunAsync(cancellationToken).ConfigureAwait(false);
    }
}
