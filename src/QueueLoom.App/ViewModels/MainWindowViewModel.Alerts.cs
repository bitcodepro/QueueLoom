using QueueLoom.App.Commands;
using QueueLoom.App.Services;
using QueueLoom.Core.Settings;

namespace QueueLoom.App.ViewModels;

/// <summary>Monitor alerts outside the window: operating-system notifications and a Slack or Teams webhook.</summary>
public sealed partial class MainWindowViewModel
{
    private IMonitorAlertService? _alerts;
    private bool _systemNotifications = true;
    private string _alertWebhookUrl = string.Empty;

    /// <summary>Show an operating-system notification for new dead letters while QueueLoom is in the background.</summary>
    public bool SystemNotifications
    {
        get => _systemNotifications;
        set
        {
            if (SetProperty(ref _systemNotifications, value))
            {
                SendTestAlertCommand?.NotifyCanExecuteChanged();
            }
        }
    }

    private bool _keepInTray;

    /// <summary>Closing the window hides QueueLoom in the system tray; monitors and scheduled resends keep running.</summary>
    public bool KeepInTray
    {
        get => _keepInTray;
        set => SetProperty(ref _keepInTray, value);
    }

    /// <summary>The tray icon's tooltip: whether monitors run and what they last found.</summary>
    public string TrayToolTip => IsMonitoring ? $"QueueLoom · {MonitorStatus}" : "QueueLoom · monitors are off";

    /// <summary>Slack or Teams incoming-webhook address; empty turns webhook alerts off.</summary>
    public string AlertWebhookUrl
    {
        get => _alertWebhookUrl;
        set
        {
            if (SetProperty(ref _alertWebhookUrl, value ?? string.Empty))
            {
                OnPropertyChanged(nameof(AlertWebhookError));
                OnPropertyChanged(nameof(HasAlertWebhookError));
                SendTestAlertCommand?.NotifyCanExecuteChanged();
            }
        }
    }

    public bool HasValidAlertWebhook => AppSettings.IsValidWebhookUrl(AlertWebhookUrl);

    public string AlertWebhookError => string.IsNullOrWhiteSpace(AlertWebhookUrl) || HasValidAlertWebhook
        ? string.Empty
        : "Use the https address of a Slack or Microsoft Teams incoming webhook.";

    public bool HasAlertWebhookError => !string.IsNullOrEmpty(AlertWebhookError);

    public AsyncRelayCommand SendTestAlertCommand { get; private set; } = null!;

    private void InitializeAlerts(IMonitorAlertService? alerts)
    {
        _alerts = alerts;
        SendTestAlertCommand = _commands.Create(
            token => RunOperationAsync("Sending a test alert", SendTestAlertAsync, token),
            () => !IsBusy && _alerts is not null && (SystemNotifications || HasValidAlertWebhook));
    }

    private async Task SendTestAlertAsync(CancellationToken cancellationToken)
    {
        var alerts = _alerts ?? throw new InvalidOperationException("Alerts are not available.");
        var alert = new MonitorAlert("Test", "orders (DLQ)", 3, null);
        var shown = SystemNotifications && await alerts.ShowSystemNotificationAsync(alert, evenWhenActive: true).ConfigureAwait(true);
        if (HasValidAlertWebhook)
        {
            await alerts.PostWebhookAsync(AlertWebhookUrl.Trim(), alert, cancellationToken).ConfigureAwait(true);
        }

        StatusText = (shown, HasValidAlertWebhook) switch
        {
            (true, true) => "Test alert shown and posted to the webhook",
            (true, false) => "Test alert shown",
            (false, true) => "Test alert posted to the webhook",
            _ => "No notification tool was found on this system; alerts stay in QueueLoom"
        };
    }

    /// <summary>At most this many alerts are delivered at once; a slow webhook cannot pile up deliveries.</summary>
    internal const int MaximumAlertsInFlight = 4;

    /// <summary>How long one alert may take to post to the webhook.</summary>
    internal static readonly TimeSpan AlertDeliveryTimeout = TimeSpan.FromSeconds(30);

    private int _alertsInFlight;

    /// <summary>Alerts currently being delivered; tests wait on it.</summary>
    internal int AlertsInFlight => Volatile.Read(ref _alertsInFlight);

    /// <summary>
    /// Sends a monitor alert without blocking the monitor; failures only go to the activity log. While
    /// <see cref="MaximumAlertsInFlight"/> earlier alerts are still being delivered (a slow or hanging webhook), a new
    /// one is skipped and noted instead of starting yet another delivery; each delivery is bounded in time.
    /// </summary>
    internal void RaiseMonitorAlert(string environment, string source, long count, long? previousCount) =>
        RaiseMonitorAlert(new MonitorAlert(environment, source, count, previousCount));

    private void RaiseMonitorAlert(MonitorAlert alert)
    {
        if (_alerts is not { } alerts)
        {
            return;
        }

        if (Interlocked.Increment(ref _alertsInFlight) > MaximumAlertsInFlight)
        {
            Interlocked.Decrement(ref _alertsInFlight);
            AddActivity("Warning", "Alert not delivered",
                $"{alert.Text}. {MaximumAlertsInFlight} earlier alerts are still being delivered, so this one was skipped; it is listed here.");
            return;
        }
        var webhook = HasValidAlertWebhook ? AlertWebhookUrl.Trim() : null;
        var system = SystemNotifications;
        _ = Task.Run(async () =>
        {
            try
            {
                if (system)
                {
                    await alerts.ShowSystemNotificationAsync(alert).ConfigureAwait(false);
                }
                if (webhook is not null)
                {
                    using var timeout = new CancellationTokenSource(AlertDeliveryTimeout);
                    await alerts.PostWebhookAsync(webhook, alert, timeout.Token).ConfigureAwait(false);
                }
            }
            catch (Exception exception)
            {
                var text = exception is OperationCanceledException
                    ? $"The webhook did not answer within {AlertDeliveryTimeout.TotalSeconds:0} seconds."
                    : SanitizeException(exception);
                Avalonia.Threading.Dispatcher.UIThread.Post(() => AddActivity("Error", "Alert not delivered", text));
            }
            finally
            {
                Interlocked.Decrement(ref _alertsInFlight);
            }
        });
    }
}
