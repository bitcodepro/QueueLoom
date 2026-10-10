using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace QueueLoom.App.Services;

/// <summary>A monitor found new dead letters (or more of them) in a queue or subscription.</summary>
public sealed record MonitorAlert(string Environment, string Source, long Count, long? PreviousCount)
{
    /// <summary>How far the count can be trusted: written "1,000+" for a lower bound, "≈1,000" for an estimate.</summary>
    public QueueLoom.Core.Monitoring.DeadLetterCountQuality CountQuality { get; init; }

    /// <summary>How far the previous count could be trusted.</summary>
    public QueueLoom.Core.Monitoring.DeadLetterCountQuality PreviousQuality { get; init; }

    public string Title => "QueueLoom: dead letters";

    public string Text => Combined is { Count: > 0 } combined
        ? $"{Environment}: dead letters in {combined.Count:N0} sources · " +
          string.Join("; ", combined.Take(MaximumListed).Select(alert => alert.SourceText)) +
          (combined.Count > MaximumListed ? $"; and {combined.Count - MaximumListed:N0} more" : string.Empty)
        : $"{Environment} · {SourceText}";

    /// <summary>The alerts of one monitor check, when it found more than one source: sent as one notification.</summary>
    public IReadOnlyList<MonitorAlert>? Combined { get; init; }

    internal const int MaximumListed = 5;

    private string SourceText => PreviousCount is { } previous
        ? $"{Source}: {CountText} dead-lettered messages (was {QueueLoom.Core.Monitoring.DeadLetterCountText.Format(previous, PreviousQuality)})"
        : $"{Source}: {CountText} dead-lettered messages";

    private string CountText => QueueLoom.Core.Monitoring.DeadLetterCountText.Format(Count, CountQuality);

    /// <summary>One alert for everything one check found: a check of many sources sends one message, not one each.</summary>
    public static MonitorAlert Combine(IReadOnlyList<MonitorAlert> alerts) => alerts.Count == 1
        ? alerts[0]
        : new MonitorAlert(alerts[0].Environment, $"{alerts.Count:N0} sources", alerts.Sum(alert => alert.Count), null)
            { Combined = alerts, CountQuality = QueueLoom.Core.Monitoring.DeadLetterCountQualities.Combine(alerts.Select(alert => alert.CountQuality)) };
}

/// <summary>Sends monitor alerts outside the QueueLoom window.</summary>
public interface IMonitorAlertService
{
    /// <summary>Shows an operating-system notification, when the QueueLoom window is not in front.</summary>
    Task<bool> ShowSystemNotificationAsync(MonitorAlert alert, bool evenWhenActive = false);

    /// <summary>Posts the alert to a Slack or Microsoft Teams incoming webhook.</summary>
    Task PostWebhookAsync(string webhookUrl, MonitorAlert alert, CancellationToken cancellationToken = default);
}

public sealed class MonitorAlertService(
    TopLevelAccessor? topLevel,
    HttpClient httpClient,
    ILogger<MonitorAlertService>? logger = null) : IMonitorAlertService
{
    private readonly ILogger _logger = logger ?? NullLogger<MonitorAlertService>.Instance;

    public async Task<bool> ShowSystemNotificationAsync(MonitorAlert alert, bool evenWhenActive = false)
    {
        ArgumentNullException.ThrowIfNull(alert);
        if (!evenWhenActive && topLevel?.Current is Avalonia.Controls.Window { IsActive: true })
        {
            return false;
        }

        var start = BuildNotificationCommand(alert.Title, alert.Text);
        if (start is null)
        {
            return false;
        }

        return await RunNotificationAsync(start, NotificationTimeout, _logger).ConfigureAwait(false);
    }

    private static readonly TimeSpan NotificationTimeout = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Runs the notification command and waits for it at most <paramref name="timeout"/>. A command still running then
    /// (a hung notification daemon, a blocked PowerShell) is killed with its children, so alerts cannot pile up one
    /// stuck process each.
    /// </summary>
    internal static async Task<bool> RunNotificationAsync(ProcessStartInfo start, TimeSpan timeout, ILogger logger)
    {
        Process? process = null;
        try
        {
            process = Process.Start(start);
            if (process is null)
            {
                return false;
            }
            using var deadline = new CancellationTokenSource(timeout);
            await process.WaitForExitAsync(deadline.Token).ConfigureAwait(false);
            return process.ExitCode == 0;
        }
        catch (Exception exception) when (exception is System.ComponentModel.Win32Exception or InvalidOperationException or OperationCanceledException)
        {
            // No notification tool on this system (for example notify-send missing); the in-app list still has the alert.
            logger.LogInformation("System notification not shown: {Reason}", exception.Message);
            if (process is not null)
            {
                try
                {
                    if (!process.HasExited)
                    {
                        process.Kill(entireProcessTree: true);
                    }
                }
                catch (Exception kill) when (kill is InvalidOperationException or System.ComponentModel.Win32Exception
                                                 or NotSupportedException)
                {
                    // It exited meanwhile, or cannot be stopped from here.
                }
            }
            return false;
        }
        finally
        {
            process?.Dispose();
        }
    }

    public async Task PostWebhookAsync(string webhookUrl, MonitorAlert alert, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(webhookUrl);
        ArgumentNullException.ThrowIfNull(alert);
        if (!QueueLoom.Core.Settings.AppSettings.IsValidWebhookUrl(webhookUrl))
        {
            throw new InvalidOperationException("The alert webhook must be an https address.");
        }

        using var response = await httpClient.PostAsJsonAsync(webhookUrl, BuildWebhookPayload(webhookUrl, alert), cancellationToken)
            .ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"The webhook answered {(int)response.StatusCode} {response.ReasonPhrase}. Check the address in Monitors.");
        }
    }

    /// <summary>
    /// Slack takes plain text. Microsoft Teams workflows ("When a Teams webhook request is received") take an
    /// Adaptive Card; the text is included too for other services that read it.
    /// An Adaptive Card TextBlock renders a Markdown subset (**bold**, _italic_, lists and [title](url) links), so an
    /// environment or queue name such as "[click](https://evil)" would become a link. Broker-supplied text therefore goes
    /// into a RichTextBlock TextRun, which Adaptive Cards documents as not supporting Markdown
    /// (https://learn.microsoft.com/adaptive-cards/authoring-cards/text-features): it is shown literally.
    /// </summary>
    public static JsonObject BuildWebhookPayload(string webhookUrl, MonitorAlert alert)
    {
        var text = $"{alert.Title} — {alert.Text}";
        if (Uri.TryCreate(webhookUrl, UriKind.Absolute, out var uri) &&
            uri.Host.EndsWith("slack.com", StringComparison.OrdinalIgnoreCase))
        {
            // Slack parses <!channel>, <@user> and <url|label> in text; broker names must stay literal.
            return new JsonObject { ["text"] = text.Replace("&", "&amp;", StringComparison.Ordinal)
                .Replace("<", "&lt;", StringComparison.Ordinal).Replace(">", "&gt;", StringComparison.Ordinal) };
        }

        return new JsonObject
        {
            ["type"] = "message",
            ["text"] = text,
            ["attachments"] = new JsonArray(new JsonObject
            {
                ["contentType"] = "application/vnd.microsoft.card.adaptive",
                ["content"] = new JsonObject
                {
                    ["$schema"] = "http://adaptivecards.io/schemas/adaptive-card.json",
                    ["type"] = "AdaptiveCard",
                    ["version"] = "1.4",
                    ["body"] = new JsonArray(
                        new JsonObject { ["type"] = "TextBlock", ["text"] = alert.Title, ["weight"] = "Bolder", ["size"] = "Medium" },
                        new JsonObject
                        {
                            ["type"] = "RichTextBlock",
                            ["inlines"] = new JsonArray(new JsonObject { ["type"] = "TextRun", ["text"] = alert.Text })
                        })
                }
            })
        };
    }

    /// <summary>
    /// The notification command for this operating system. Title and text travel as environment variables or
    /// separate arguments, never inside a script, so queue names cannot inject commands.
    /// </summary>
    public static ProcessStartInfo? BuildNotificationCommand(string title, string text)
    {
        ProcessStartInfo start;
        if (OperatingSystem.IsWindows())
        {
            start = new ProcessStartInfo("powershell.exe");
            start.ArgumentList.Add("-NoProfile");
            start.ArgumentList.Add("-NonInteractive");
            start.ArgumentList.Add("-Command");
            start.ArgumentList.Add(
                "[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null; " +
                "$xml = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02); " +
                "$texts = $xml.GetElementsByTagName('text'); " +
                "$texts.Item(0).AppendChild($xml.CreateTextNode($env:QUEUELOOM_ALERT_TITLE)) > $null; " +
                "$texts.Item(1).AppendChild($xml.CreateTextNode($env:QUEUELOOM_ALERT_TEXT)) > $null; " +
                "$id = '{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\\WindowsPowerShell\\v1.0\\powershell.exe'; " +
                "[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier($id).Show([Windows.UI.Notifications.ToastNotification]::new($xml))");
        }
        else if (OperatingSystem.IsMacOS())
        {
            start = new ProcessStartInfo("osascript");
            start.ArgumentList.Add("-e");
            start.ArgumentList.Add(
                "display notification (system attribute \"QUEUELOOM_ALERT_TEXT\") with title (system attribute \"QUEUELOOM_ALERT_TITLE\")");
        }
        else if (OperatingSystem.IsLinux())
        {
            start = new ProcessStartInfo("notify-send");
            start.ArgumentList.Add("--app-name=QueueLoom");
            // The text starts with the environment's name; one like "-prod" must not be read as an option.
            start.ArgumentList.Add("--");
            start.ArgumentList.Add(title);
            start.ArgumentList.Add(text);
        }
        else
        {
            return null;
        }

        start.UseShellExecute = false;
        start.CreateNoWindow = true;
        start.Environment["QUEUELOOM_ALERT_TITLE"] = title;
        start.Environment["QUEUELOOM_ALERT_TEXT"] = text;
        return start;
    }
}
