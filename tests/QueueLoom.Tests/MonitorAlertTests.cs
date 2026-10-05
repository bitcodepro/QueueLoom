using System.Net;
using System.Text.Json.Nodes;
using QueueLoom.App.Services;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class MonitorAlertTests
{
    private static readonly MonitorAlert Alert = new("Orders production", "orders (DLQ)", 12, 3);

    [Fact]
    public void Alert_text_names_the_environment_source_and_counts_only()
    {
        Assert.Equal("Orders production · orders (DLQ): 12 dead-lettered messages (was 3)", Alert.Text);
        Assert.Equal("Orders production · orders (DLQ): 12 dead-lettered messages", (Alert with { PreviousCount = null }).Text);
    }

    [Fact]
    public async Task Slack_gets_plain_text_and_Teams_gets_an_adaptive_card()
    {
        var handler = new RecordingHandler(HttpStatusCode.OK);
        var service = new MonitorAlertService(null, new HttpClient(handler));

        await service.PostWebhookAsync("https://hooks.slack.com/services/T/B/X", Alert);
        await service.PostWebhookAsync("https://prod-01.westeurope.logic.azure.com/workflows/abc", Alert);

        var slack = JsonNode.Parse(handler.Bodies[0])!;
        Assert.Equal("QueueLoom: dead letters — " + Alert.Text, slack["text"]!.GetValue<string>());
        Assert.Null(slack["attachments"]);
        var teams = JsonNode.Parse(handler.Bodies[1])!;
        Assert.Equal("message", teams["type"]!.GetValue<string>());
        Assert.Equal("AdaptiveCard", teams["attachments"]![0]!["content"]!["type"]!.GetValue<string>());
        Assert.Equal(Alert.Text, teams["attachments"]![0]!["content"]!["body"]![1]!["inlines"]![0]!["text"]!.GetValue<string>());
    }

    [Fact]
    public async Task A_failing_or_insecure_webhook_is_reported()
    {
        var service = new MonitorAlertService(null, new HttpClient(new RecordingHandler(HttpStatusCode.NotFound)));

        var failed = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            service.PostWebhookAsync("https://hooks.slack.com/services/T/B/X", Alert));
        Assert.Contains("404", failed.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<InvalidOperationException>(() => service.PostWebhookAsync("http://example.com/hook", Alert));
    }

    [Fact]
    public void The_notification_command_passes_text_as_data_not_as_script()
    {
        var start = MonitorAlertService.BuildNotificationCommand("QueueLoom", "orders\"; rm -rf ~ #");

        Assert.NotNull(start);
        Assert.False(start.UseShellExecute);
        Assert.Equal("orders\"; rm -rf ~ #", start.Environment["QUEUELOOM_ALERT_TEXT"]);
        if (OperatingSystem.IsLinux())
        {
            Assert.Equal("notify-send", start.FileName);
            Assert.Equal(["--app-name=QueueLoom", "QueueLoom", "orders\"; rm -rf ~ #"], start.ArgumentList);
        }
        else
        {
            Assert.DoesNotContain(start.ArgumentList, argument => argument.Contains("rm -rf", StringComparison.Ordinal));
        }
    }

    private sealed class RecordingHandler(HttpStatusCode status) : HttpMessageHandler
    {
        public List<string> Bodies { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Bodies.Add(await request.Content!.ReadAsStringAsync(cancellationToken));
            return new HttpResponseMessage(status);
        }
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task Monitor_AlertsOutsideTheWindowWhenDeadLettersAppear()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Snapshots = { [dev.Id] = Snapshot(dev.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 3)) }
        };
        var alerts = new RecordingAlerts();
        await using var viewModel = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace, alerts: alerts);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.AlertWebhookUrl = "https://hooks.slack.com/services/T/B/X";

        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await WaitUntilAsync(() => alerts.Webhooks.Count == 1);
        await viewModel.ToggleMonitorCommand.ExecuteAsync();

        var alert = Assert.Single(alerts.System);
        Assert.Equal("Development", alert.Environment);
        Assert.Equal(3, alert.Count);
        Assert.Equal("https://hooks.slack.com/services/T/B/X", Assert.Single(alerts.Webhooks).Url);

        viewModel.AlertWebhookUrl = "not a url";
        Assert.True(viewModel.HasAlertWebhookError);
    }

    private sealed class RecordingAlerts : IMonitorAlertService
    {
        public List<MonitorAlert> System { get; } = [];
        public List<(string Url, MonitorAlert Alert)> Webhooks { get; } = [];

        public Task<bool> ShowSystemNotificationAsync(MonitorAlert alert, bool evenWhenActive = false)
        {
            lock (System)
            {
                System.Add(alert);
            }
            return Task.FromResult(true);
        }

        public Task PostWebhookAsync(string webhookUrl, MonitorAlert alert, CancellationToken cancellationToken = default)
        {
            lock (Webhooks)
            {
                Webhooks.Add((webhookUrl, alert));
            }
            return Task.CompletedTask;
        }
    }
}
