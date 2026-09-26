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

    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
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

    private async ValueTask DisposeServicesAsync()
    {
        if (_services is { } services)
        {
            _services = null;
            await services.DisposeAsync().ConfigureAwait(true);
        }
    }
}
