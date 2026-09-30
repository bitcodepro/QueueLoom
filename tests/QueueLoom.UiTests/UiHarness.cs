using Avalonia;
using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.Logging;
using Avalonia.Threading;
using Microsoft.Extensions.Logging.Abstractions;
using QueueLoom.App;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.UiTests;

public static class TestAppBuilder
{
    public static AppBuilder BuildAvaloniaApp() =>
        AppBuilder.Configure<QueueLoom.App.App>()
            .UseSkia()
            .WithInterFont()
            .UseHeadless(new AvaloniaHeadlessPlatformOptions { UseHeadlessDrawing = false });
}

/// <summary>One headless Avalonia platform per test process; every UI test runs on its dispatcher.</summary>
internal static class UiSession
{
    private static readonly Lazy<HeadlessUnitTestSession> Session = new(() =>
    {
        Logger.Sink = BindingErrors.Instance;
        return HeadlessUnitTestSession.StartNew(typeof(TestAppBuilder));
    });

    public static Task RunAsync(Func<Task> test) => Session.Value.Dispatch(async () =>
    {
        await test();
        return true;
    }, CancellationToken.None);
}

/// <summary>Collects binding warnings so tests can fail on broken XAML bindings.</summary>
internal sealed class BindingErrors : ILogSink
{
    public static BindingErrors Instance { get; } = new();

    private readonly List<string> _messages = [];

    public IReadOnlyList<string> Messages
    {
        get
        {
            lock (_messages)
            {
                return _messages.ToArray();
            }
        }
    }

    public void Clear()
    {
        lock (_messages)
        {
            _messages.Clear();
        }
    }

    public bool IsEnabled(LogEventLevel level, string area) =>
        level >= LogEventLevel.Warning && area == LogArea.Binding;

    public void Log(LogEventLevel level, string area, object? source, string messageTemplate) =>
        Log(level, area, source, messageTemplate, []);

    public void Log(LogEventLevel level, string area, object? source, string messageTemplate, params object?[] propertyValues)
    {
        if (!IsEnabled(level, area))
        {
            return;
        }

        var message = $"{source?.GetType().Name}: {messageTemplate} [{string.Join(", ", propertyValues)}]";
        lock (_messages)
        {
            _messages.Add(message);
        }
    }
}

internal sealed class RecordingClipboard : IClipboardService
{
    public List<string> Copied { get; } = [];

    public Task<bool> SetTextAsync(string text)
    {
        Copied.Add(text);
        return Task.FromResult(true);
    }
}

internal sealed class RecordingNotifications : INotificationService
{
    public List<(string Title, string Message, NotificationTone Tone)> Shown { get; } = [];

    public void Show(string title, string message, NotificationTone tone = NotificationTone.Information) =>
        Shown.Add((title, message, tone));
}

internal sealed class NoopLauncher : IAppLauncher
{
    public Task<bool> OpenUriAsync(Uri uri) => Task.FromResult(true);

    public Task<bool> OpenFolderAsync(string path) => Task.FromResult(true);
}

/// <summary>Builds the real main window around in-memory services.</summary>
internal sealed class WindowFixture : IAsyncDisposable
{
    private readonly string _dataDirectory = Path.Combine(Path.GetTempPath(), "queueloom-ui", Guid.NewGuid().ToString("N"));
    private readonly JsonAppSettingsStore _settings;
    private readonly HttpClient _offlineHttp = new(new OfflineHandler());

    private WindowFixture(params QueueLoom.Core.Profiles.ServiceBusProfile[] profiles)
        : this(new DemoWorkspace(), new InMemorySecretVault(), profiles)
    {
    }

    private WindowFixture(
        QueueLoom.Core.Abstractions.IServiceBusWorkspace workspace,
        QueueLoom.Core.Abstractions.ISecretVault secretVault,
        params QueueLoom.Core.Profiles.ServiceBusProfile[] profiles)
    {
        _settings = new JsonAppSettingsStore(QueueLoomPaths.ForRoot(_dataDirectory));
        History = new JsonLinesDeadLetterHistoryStore(Path.Combine(_dataDirectory, "dlq-history.jsonl"));
        foreach (var sample in profiles.Take(1).SelectMany(profile => DemoData.History(profile, DateTimeOffset.UtcNow)))
        {
            History.Append(sample);
        }
        var accessor = new TopLevelAccessor();
        ViewModel = new MainWindowViewModel(
            new InMemoryProfileRepository(profiles),
            secretVault,
            workspace,
            new WindowDialogService(accessor),
            new InMemoryBackupRepository(),
            clipboard: Clipboard,
            launcher: new NoopLauncher(),
            notifications: Notifications,
            theme: new AvaloniaThemeService(),
            history: History);
        Window = new MainWindow(
            ViewModel,
            _settings,
            accessor,
            new WindowDialogService(accessor),
            new GitHubUpdateChecker(_offlineHttp),
            new NoopLauncher(),
            new AvaloniaThemeService(),
            NullLogger<MainWindow>.Instance)
        {
            Width = 1440,
            Height = 900
        };
    }

    public MainWindowViewModel ViewModel { get; }

    public JsonLinesDeadLetterHistoryStore History { get; }

    public MainWindow Window { get; }

    public RecordingClipboard Clipboard { get; } = new();

    public RecordingNotifications Notifications { get; } = new();

    public static async Task<WindowFixture> OpenAsync(bool connect = true, bool allClouds = false)
    {
        var fixture = !connect
            ? new WindowFixture()
            : allClouds
                ? new WindowFixture(DemoData.Development, DemoData.Production, DemoData.AwsStaging, DemoData.GoogleDevelopment,
                    DemoData.RabbitStaging, DemoData.KafkaDevelopment)
                : new WindowFixture(DemoData.Development, DemoData.Production);
        fixture.Window.Show();
        await fixture.SettleAsync();
        if (connect)
        {
            await fixture.ViewModel.ConnectCommand.ExecuteAsync();
            await fixture.ViewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
            await fixture.SettleAsync();
        }
        return fixture;
    }

    /// <summary>Opens the window on real services, for example a workspace talking to LocalStack.</summary>
    public static async Task<WindowFixture> OpenWithAsync(
        QueueLoom.Core.Abstractions.IServiceBusWorkspace workspace,
        QueueLoom.Core.Abstractions.ISecretVault secretVault,
        params QueueLoom.Core.Profiles.ServiceBusProfile[] profiles)
    {
        var fixture = new WindowFixture(workspace, secretVault, profiles);
        fixture.Window.Show();
        await fixture.SettleAsync();
        return fixture;
    }

    public async Task SettleAsync()
    {
        for (var i = 0; i < 5; i++)
        {
            Dispatcher.UIThread.RunJobs();
            await Task.Delay(10);
        }
        Dispatcher.UIThread.RunJobs();
    }

    /// <summary>Peeks the "orders" DLQ and selects its first message, as an operator would.</summary>
    public async Task OpenDeadLettersAsync()
    {
        ViewModel.SelectedDlqSource = ViewModel.FilteredDeadLetterSources.First(source => source.EntityName == "orders");
        await ViewModel.BrowseDlqSourceCommand.ExecuteAsync();
        ViewModel.SelectedMessage = ViewModel.Messages.FirstOrDefault();
        await SettleAsync();
    }

    public async Task NavigateAsync(string page)
    {
        ViewModel.NavigateCommand.Execute(page);
        await SettleAsync();
        Window.UpdateLayout();
    }

    public async ValueTask DisposeAsync()
    {
        Window.Close();
        await SettleAsync();
        _settings.Dispose();
        _offlineHttp.Dispose();
        try
        {
            Directory.Delete(_dataDirectory, recursive: true);
        }
        catch (DirectoryNotFoundException)
        {
        }
    }

    private sealed class OfflineHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.NotFound));
    }
}
