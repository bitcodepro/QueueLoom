using QueueLoom.Core.Monitoring;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Threading;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using QueueLoom.Core.Abstractions;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Logging;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;
using QueueLoom.Mcp;

namespace QueueLoom.App.Mcp;

/// <summary>
/// <c>QueueLoom --mcp</c>: serve MCP over stdin/stdout for LLM clients instead of opening the main window.
/// Changes are approved in a QueueLoom window; without a desktop session the client is asked instead.
/// </summary>
internal static class McpMode
{
    public const string Argument = "--mcp";
    public const string ReadOnlyArgument = "--read-only";

    public static bool IsRequested(string[] args) =>
        args.Any(arg => string.Equals(arg, Argument, StringComparison.OrdinalIgnoreCase));

    public static int Run(string[] args, Func<AppBuilder> buildAvaloniaApp)
    {
        var paths = QueueLoomPaths.CreateDefault();
        var settings = new McpServerSettings(
            ReadOnly: args.Any(arg => string.Equals(arg, ReadOnlyArgument, StringComparison.OrdinalIgnoreCase)),
            ExportDirectory: Path.Combine(paths.RootDirectory, "exports"));
        var logs = new FileLoggerProvider(Path.Combine(paths.RootDirectory, "logs"));
        LoadProtobufSchemas(paths);

        // Also in read-only mode: a read of live messages on SQS, Pub/Sub or RabbitMQ asks for approval, and a client
        // without elicitation can only get it from the desktop window.
        if (UsesDesktopApprover(HasDesktopSession()))
        {
            // Avalonia must own the main thread; the server runs beside it and ends the app when the client leaves.
            App.BackgroundService = () => RunServerAsync(settings, paths, logs, new DesktopApprover());
            return buildAvaloniaApp().StartWithClassicDesktopLifetime(args, ShutdownMode.OnExplicitShutdown);
        }

        RunServerAsync(settings, paths, logs, new ElicitationApprover()).GetAwaiter().GetResult();
        return 0;
    }

    /// <summary>The .proto files chosen in the app give Protobuf bodies their field names here too; a failure only loses the names.</summary>
    private static void LoadProtobufSchemas(QueueLoomPaths paths)
    {
        try
        {
            using var store = new JsonAppSettingsStore(paths);
            if (store.LoadAsync().GetAwaiter().GetResult().ProtobufSchemaPath is { Length: > 0 } path)
            {
                ProtoSchemaCatalog.Current = ProtoSchemaSet.Load(path);
            }
        }
        catch (Exception exception) when (exception is ProtoSchemaException or IOException or UnauthorizedAccessException
                                              or System.Text.Json.JsonException)
        {
        }
    }

    private static async Task RunServerAsync(
        McpServerSettings settings,
        QueueLoomPaths paths,
        FileLoggerProvider logs,
        IOperationApprover approver)
    {
        // An MCP server can run for days and appends to the shared Activity journal, so it applies the same retention.
        // Operation history belongs to the desktop app, which cleans it up itself.
        using var retention = new LocalHistoryRetention(new FileActivityJournal(Path.Combine(paths.RootDirectory, "activity")), null);
        _ = retention.Start();
        await RunMcpServerAsync(settings, paths, logs, approver).ConfigureAwait(false);
    }

    private static Task RunMcpServerAsync(
        McpServerSettings settings,
        QueueLoomPaths paths,
        FileLoggerProvider logs,
        IOperationApprover approver) =>
        QueueLoomMcpServer.RunAsync(
            settings,
            services =>
            {
                services.AddSingleton(paths);
                services.AddSingleton<IProfileRepository, JsonProfileRepository>();
                services.AddSingleton<ISecretVault, EncryptedFileSecretVault>();
                services.AddSingleton<IServiceBusWorkspace>(provider =>
                    MessagingWorkspaces.Create(provider.GetRequiredService<ISecretVault>(), paths));
                services.AddSingleton<IActivityJournal>(_ => new FileActivityJournal(Path.Combine(paths.RootDirectory, "activity")));
                services.AddSingleton<IDeadLetterHistoryStore>(_ =>
                    new JsonLinesDeadLetterHistoryStore(Path.Combine(paths.RootDirectory, "dlq-history.jsonl")));
                services.AddSingleton(approver);
            },
            logging =>
            {
                logging.SetMinimumLevel(LogLevel.Information);
                logging.AddProvider(logs);
            });

    /// <summary>Whether approvals use the desktop window; it does not depend on read-only mode, which only hides change tools.</summary>
    internal static bool UsesDesktopApprover(bool hasDesktopSession) => hasDesktopSession;

    private static bool HasDesktopSession() =>
        OperatingSystem.IsWindows() || OperatingSystem.IsMacOS() ||
        !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DISPLAY")) ||
        !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("WAYLAND_DISPLAY"));
}
