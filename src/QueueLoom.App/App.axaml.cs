using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using Microsoft.Extensions.DependencyInjection;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.App;

public sealed partial class App : Application
{
    private ServiceProvider? _services;

    /// <summary>
    /// Set before start-up to run without a main window (MCP mode): the task runs in the background and the
    /// application exits when it completes. Windows such as approval prompts can still be shown.
    /// </summary>
    internal static Func<Task>? BackgroundService { get; set; }

    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime { } backgroundHost && BackgroundService is { } service)
        {
            _ = RunBackgroundServiceAsync(backgroundHost, service);
        }
        else if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            try
            {
                _services = AppServices.Build(QueueLoomPaths.CreateDefault());
                var window = _services.GetRequiredService<MainWindow>();
                window.ShutdownCompleted = DisposeServicesAsync;
                desktop.MainWindow = window;
            }
            catch (Exception exception)
            {
                // Without storage or credentials the console cannot operate. Explain why
                // instead of terminating silently.
                desktop.MainWindow = new ConfirmDialogWindow(new ConfirmDialogViewModel(
                    "QueueLoom could not start",
                    $"Local settings could not be opened: {SensitiveDataRedactor.SummarizeException(exception)}",
                    isDangerous: false,
                    requiredText: null,
                    showCancel: false,
                    confirmLabel: "Close"));
            }
        }

        base.OnFrameworkInitializationCompleted();
    }

    private static async Task RunBackgroundServiceAsync(IClassicDesktopStyleApplicationLifetime desktop, Func<Task> service)
    {
        var exitCode = 0;
        try
        {
            await Task.Run(service).ConfigureAwait(true);
        }
        catch (Exception exception)
        {
            exitCode = 1;
            await Console.Error.WriteLineAsync($"QueueLoom stopped: {SensitiveDataRedactor.SummarizeException(exception)}")
                .ConfigureAwait(true);
        }
        finally
        {
            desktop.Shutdown(exitCode);
        }
    }

    private async ValueTask DisposeServicesAsync()
    {
        if (_services is { } services)
        {
            _services = null;
            await services.DisposeAsync().ConfigureAwait(true);
        }
    }
}
