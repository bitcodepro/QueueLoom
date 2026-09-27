using Avalonia;
using QueueLoom.App.Mcp;

namespace QueueLoom.App;

internal static class Program
{
    [STAThread]
    public static int Main(string[] args) =>
        McpMode.IsRequested(args)
            ? McpMode.Run(args, BuildAvaloniaApp)
            : BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);

    public static AppBuilder BuildAvaloniaApp() =>
        AppBuilder.Configure<App>()
            .UsePlatformDetect()
            .WithInterFont()
            .LogToTrace();
}
