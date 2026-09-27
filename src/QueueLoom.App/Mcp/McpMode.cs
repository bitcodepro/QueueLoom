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
        var settings = new McpServerSettings(
            ReadOnly: args.Any(arg => string.Equals(arg, ReadOnlyArgument, StringComparison.OrdinalIgnoreCase)));
        var paths = QueueLoomPaths.CreateDefault();
        var logs = new FileLoggerProvider(Path.Combine(paths.RootDirectory, "logs"));

        if (!settings.ReadOnly && HasDesktopSession())
        {
            // Avalonia must own the main thread; the server runs beside it and ends the app when the client leaves.
            App.BackgroundService = () => RunServerAsync(settings, paths, logs, new DesktopApprover());
            return buildAvaloniaApp().StartWithClassicDesktopLifetime(args, ShutdownMode.OnExplicitShutdown);
        }

        RunServerAsync(settings, paths, logs, new ElicitationApprover()).GetAwaiter().GetResult();
        return 0;
    }

    private static Task RunServerAsync(
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
                services.AddSingleton(approver);
            },
            logging =>
            {
                logging.SetMinimumLevel(LogLevel.Information);
                logging.AddProvider(logs);
            });

    private static bool HasDesktopSession() =>
        OperatingSystem.IsWindows() || OperatingSystem.IsMacOS() ||
        !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DISPLAY")) ||
        !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("WAYLAND_DISPLAY"));
}
