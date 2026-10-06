using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Logging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;

namespace QueueLoom.App.Services;

/// <summary>The application's composition root.</summary>
public static class AppServices
{
    public static ServiceProvider Build(QueueLoomPaths paths)
    {
        ArgumentNullException.ThrowIfNull(paths);
        var services = new ServiceCollection();

        services.AddLogging(logging =>
        {
            logging.SetMinimumLevel(LogLevel.Information);
            logging.AddProvider(new FileLoggerProvider(Path.Combine(paths.RootDirectory, "logs")));
        });

        services.AddSingleton(paths);
        services.AddSingleton(_ => new LegacyBackupMigration(paths));
        services.AddSingleton(DiagnosticsJournal.Session);
        services.AddSingleton<JsonProfileRepository>();
        services.AddSingleton<IProfileRepository>(provider => provider.GetRequiredService<JsonProfileRepository>());
        services.AddSingleton<EncryptedFileSecretVault>();
        services.AddSingleton<ISecretVault>(provider => provider.GetRequiredService<EncryptedFileSecretVault>());
        services.AddSingleton<JsonAppSettingsStore>();
        services.AddSingleton<IServiceBusWorkspace>(provider =>
            MessagingWorkspaces.Create(provider.GetRequiredService<ISecretVault>(), paths));
        services.AddSingleton<IDeadLetterBackupRepository>(_ => new JsonDeadLetterBackupRepository(paths));
        services.AddSingleton<IActivityJournal>(_ => new FileActivityJournal(Path.Combine(paths.RootDirectory, "activity")));
        services.AddSingleton<IDeadLetterHistoryStore>(_ =>
            new JsonLinesDeadLetterHistoryStore(Path.Combine(paths.RootDirectory, "dlq-history.jsonl")));
        services.AddSingleton<QueueLoom.Core.ServiceBus.IScheduledResendStore>(_ => new JsonScheduledResendStore(paths));
        services.AddSingleton<IBatchReplayStore>(_ => new BatchReplayStore(Path.Combine(paths.RootDirectory, "replay")));
        // Started by App once the window exists; deletes Activity records and finished operations after three days.
        services.AddSingleton(provider => new LocalHistoryRetention(
            provider.GetRequiredService<IActivityJournal>() as FileActivityJournal,
            provider.GetRequiredService<IBatchReplayStore>() as BatchReplayStore,
            provider.GetRequiredService<QueueLoom.Core.ServiceBus.IScheduledResendStore>(),
            TimeProvider.System,
            provider.GetService<ILogger<LocalHistoryRetention>>()));

        services.AddSingleton<TopLevelAccessor>();
        services.AddSingleton<WindowDialogService>();
        services.AddSingleton<IUserDialogService>(provider => provider.GetRequiredService<WindowDialogService>());
        services.AddSingleton<IClipboardService, AvaloniaClipboardService>();
        services.AddSingleton<IAppLauncher, AvaloniaAppLauncher>();
        services.AddSingleton<INotificationService, WindowNotificationService>();
        services.AddSingleton<IThemeService, AvaloniaThemeService>();
        services.AddSingleton<GitHubUpdateChecker>(_ => new GitHubUpdateChecker());
        services.AddSingleton(_ =>
        {
            var client = new HttpClient { Timeout = TimeSpan.FromMinutes(15) };
            client.DefaultRequestHeaders.UserAgent.ParseAdd("QueueLoom-Update");
            return new AppUpdater(client);
        });
        services.AddSingleton<IMonitorAlertService>(provider => new MonitorAlertService(
            provider.GetRequiredService<TopLevelAccessor>(),
            new HttpClient { Timeout = TimeSpan.FromSeconds(15) },
            provider.GetService<Microsoft.Extensions.Logging.ILogger<MonitorAlertService>>()));

        services.AddSingleton<MainWindowViewModel>();
        services.AddSingleton<MainWindow>();
        return services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true
        });
    }
}
