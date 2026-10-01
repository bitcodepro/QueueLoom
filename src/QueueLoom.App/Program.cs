using Avalonia;
using QueueLoom.App.Mcp;
using QueueLoom.App.Services;

namespace QueueLoom.App;

internal static class Program
{
    [STAThread]
    public static int Main(string[] args)
    {
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
