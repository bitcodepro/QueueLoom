using Avalonia;
using QueueLoom.App.Mcp;
using QueueLoom.App.Services;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.App;

internal static class Program
{
    [STAThread]
    public static int Main(string[] args)
    {
        if (args.Length == 1 && args[0] == KafkaGroupListing.HelperArgument)
        {
            // A child listing Kafka consumer groups for the app: no window, no local storage.
            return KafkaGroupListing.RunHelper(Console.OpenStandardInput(), Console.OpenStandardOutput());
        }
        KafkaGroupListing.UseThisExecutable(typeof(Program).Assembly.Location);
        _ = QueueLoom.Core.Updates.PayloadLaunch.Current; // Validate supplied installation context before any local storage.
        if (UpdateRestart.HandleArguments(args, out var exitCode)) return exitCode;
        return McpMode.IsRequested(args)
            ? McpMode.Run(args, BuildAvaloniaApp)
            : BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);
    }

    public static AppBuilder BuildAvaloniaApp() =>
        AppBuilder.Configure<App>()
            .UsePlatformDetect()
            .WithInterFont()
            .LogToTrace();
}
