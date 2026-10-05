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
            Assert.Equal(["--app-name=QueueLoom", "--", "QueueLoom", "orders\"; rm -rf ~ #"], start.ArgumentList);
        }
        else
        {
            Assert.DoesNotContain(start.ArgumentList, argument => argument.Contains("rm -rf", StringComparison.Ordinal));
        }
    }

    // An environment named "-prod" starts the notification text: notify-send must not read it as an option.
    [Fact]
    public void A_notification_text_starting_with_a_dash_is_not_an_option()
    {
        var start = MonitorAlertService.BuildNotificationCommand("QueueLoom: dead letters", "-prod · orders: 3 dead-lettered messages");

        Assert.NotNull(start);
        if (OperatingSystem.IsLinux())
        {
            var separator = start.ArgumentList.IndexOf("--");
            Assert.True(separator >= 0 && separator < start.ArgumentList.IndexOf("-prod · orders: 3 dead-lettered messages"));
        }
    }

    // A notification command that hangs is stopped after the timeout instead of being left running.
    [Fact]
    public async Task A_hung_notification_command_is_killed_after_the_timeout()
    {
        if (!OperatingSystem.IsLinux()) return; // checks /proc
        var marker = $"{30 + Random.Shared.Next(1, 999) / 1000.0:0.000}";
        var start = new System.Diagnostics.ProcessStartInfo("sleep") { UseShellExecute = false };
        start.ArgumentList.Add(marker);

        var shown = await MonitorAlertService.RunNotificationAsync(start, TimeSpan.FromMilliseconds(300),
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        Assert.False(shown);
        await Task.Delay(300);
        Assert.DoesNotContain(System.Diagnostics.Process.GetProcessesByName("sleep"), process =>
        {
            try { return File.ReadAllText($"/proc/{process.Id}/cmdline").Contains(marker, StringComparison.Ordinal); }
            catch (IOException) { return false; }
            catch (UnauthorizedAccessException) { return false; }
        });
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

public sealed class ProtoSchemaLoadBoundsTests
{
    // A large file with a schema extension (a dump named .pb) is refused before it is read into memory.
    [Fact]
    public void AnOversizedSchemaFileIsRefusedBeforeItIsRead()
    {
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        File.WriteAllText(Path.Combine(directory.Path, "a.proto"), "syntax = \"proto3\"; message A { string x = 1; }");
        using (var big = File.Create(Path.Combine(directory.Path, "dump.pb")))
        {
            big.SetLength(16L * 1024 * 1024 + 1);
        }

        var refused = Assert.Throws<ProtoSchemaException>(() => ProtoSchemaSet.Load(directory.Path));
        Assert.Contains("dump.pb", refused.Message, StringComparison.Ordinal);
        Assert.Contains("are not read", refused.Message, StringComparison.Ordinal);
    }

    // A folder picked by mistake (a home folder) is not walked to its end: the walk stops and says why.
    [Fact]
    public void AHugeFolderIsNotWalkedToItsEnd()
    {
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        for (var index = 0; index < 30; index++)
        {
            File.WriteAllText(Path.Combine(directory.Path, $"note-{index}.txt"), "x");
        }
        File.WriteAllText(Path.Combine(directory.Path, "a.proto"), "syntax = \"proto3\"; message A { string x = 1; }");
        ProtoSchemaSet.EntriesVisitedOverride.Value = 10;
        try
        {
            var refused = Assert.Throws<ProtoSchemaException>(() => ProtoSchemaSet.Load(directory.Path));
            Assert.Contains("more than 10 files", refused.Message, StringComparison.Ordinal);
        }
        finally
        {
            ProtoSchemaSet.EntriesVisitedOverride.Value = null;
        }
        Assert.NotNull(ProtoSchemaSet.Load(directory.Path).Resolve("A"));
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

    // One check that finds dead letters in many queues (a downstream outage) sends one notification and one webhook
    // post naming the first few, instead of one per queue that the in-flight limit then drops and lists as errors.
    [Fact]
    public async Task Monitor_SendsOneAlertForEverythingOneCheckFinds()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Snapshots =
            {
                [dev.Id] = Snapshot(dev.Id, Enumerable.Range(1, 12)
                    .Select(index => new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue($"orders-{index:00}"), index)).ToArray())
            }
        };
        var alerts = new RecordingAlerts();
        await using var viewModel = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace, alerts: alerts);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.AlertWebhookUrl = "https://hooks.slack.com/services/T/B/X";

        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await WaitUntilAsync(() => alerts.Webhooks.Count >= 1);
        await viewModel.ToggleMonitorCommand.ExecuteAsync();
        await WaitUntilAsync(() => viewModel.AlertsInFlight == 0);

        var alert = Assert.Single(alerts.Webhooks).Alert;
        Assert.Single(alerts.System);
        Assert.Equal(78, alert.Count);
        Assert.StartsWith("Development: dead letters in 12 sources · ", alert.Text, StringComparison.Ordinal);
        Assert.EndsWith("; and 7 more", alert.Text, StringComparison.Ordinal);
        Assert.DoesNotContain(viewModel.Activity, item => item.Action == "Alert not delivered");
        Assert.Equal(12, viewModel.MonitorNotifications.Count);
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
