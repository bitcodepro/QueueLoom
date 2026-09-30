using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;

namespace QueueLoom.Mcp;

/// <param name="ReadOnly">When true, only read tools are offered; nothing can change the queues.</param>
/// <param name="ExportDirectory">Where export_messages writes its files; a QueueLoom folder in the temp directory when null.</param>
public sealed record McpServerSettings(bool ReadOnly = false, string? ExportDirectory = null)
{
    public string ResolvedExportDirectory =>
        Path.GetFullPath(ExportDirectory ?? Path.Combine(Path.GetTempPath(), "QueueLoom", "exports"));
}

/// <summary>Runs QueueLoom as an MCP server so LLM clients can inspect message queues and, with approval, change them.</summary>
public static class QueueLoomMcpServer
{
    public const string Instructions =
        "QueueLoom inspects message queues saved by the user: Azure Service Bus namespaces, Amazon SQS / SNS regions, " +
        "Google Cloud Pub/Sub projects, RabbitMQ virtual hosts and Kafka clusters. Start with list_environments, then get_entities or scan_dead_letters. peek_messages and " +
        "search_dead_letters only read: they never remove messages (SQS, Pub/Sub and RabbitMQ cannot peek, so messages are received and " +
        "immediately released; on SQS and Pub/Sub that counts as a delivery). In RabbitMQ, topics are exchanges and the subject is the routing key. export_messages saves messages to a JSON or CSV file on this computer. " +
        "delete_dead_letter_messages, purge_dead_letters, resend_dead_letters and send_message change messages; each call is shown to the user, " +
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
        builder.Services.AddSingleton(settings);
        builder.Services.AddSingleton<McpWorkspaceSession>();

        var mcp = builder.Services.AddMcpServer(options =>
        {
            options.ServerInfo = new Implementation
            {
                Name = "QueueLoom",
                Title = "QueueLoom — message queues",
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
