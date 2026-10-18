using QueueLoom.App.Services;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task EditingAnUnrelatedEnvironmentKeepsTheProductionMonitorRunning()
    {
        var prod = CreateProfile("Production", EnvironmentKind.Production);
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var workspace = new FakeWorkspace
        {
            Snapshots = { [prod.Id] = Snapshot(prod.Id, new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 0)) }
        };
        var dialogs = new FakeDialogService { EditResult = new ProfileEditorResult(dev with { Name = "Development (renamed)" }, null, false) };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([prod, dev], prod.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ToggleMonitorCommand.ExecuteAsync(); // "Current environment" = Production
        Assert.True(viewModel.IsMonitoring);

        viewModel.SelectedProfile = viewModel.Profiles.Single(p => p.Id == dev.Id);
        await viewModel.EditEnvironmentCommand.ExecuteAsync();

        Assert.Contains(viewModel.Profiles, p => p.Name == "Development (renamed)");
        Assert.True(viewModel.IsMonitoring, $"Production alerts silently stopped: {viewModel.MonitorStatus}");
        await viewModel.ToggleMonitorCommand.ExecuteAsync();
    }

    [Fact]
    public async Task TogglingSystemNotificationsRefreshesTheTestAlertButton()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        await using var viewModel = CreateViewModel(new FakeProfileRepository([dev], dev.Id), new FakeWorkspace(), alerts: new RecordingAlerts());
        await viewModel.InitializeAsync();
        Assert.True(viewModel.SendTestAlertCommand.CanExecute(null));
        var raised = 0;
        viewModel.SendTestAlertCommand.CanExecuteChanged += (_, _) => raised++;

        viewModel.SystemNotifications = false; // no webhook either: the bound button must disable

        Assert.False(viewModel.SendTestAlertCommand.CanExecute(null));
        Assert.True(raised > 0, "SendTestAlertCommand.CanExecuteChanged was not raised; the button keeps its stale state.");
    }
}

public sealed class SlackWebhookEscapingTests
{
    [Fact]
    public void SlackPayloadEscapesControlSequencesInBrokerNames()
    {
        // RabbitMQ queue names are arbitrary UTF-8; Slack reads <...> in "text" as mentions and links.
        var alert = new MonitorAlert("Production", "<!channel> <https://evil.example|reset password> (DLQ)", 3, null);

        var text = MonitorAlertService.BuildWebhookPayload("https://hooks.slack.com/services/T/B/X", alert)["text"]!.GetValue<string>();

        Assert.DoesNotContain("<!channel>", text, StringComparison.Ordinal);
        Assert.Contains("&lt;!channel&gt; &lt;https://evil.example|reset password&gt;", text, StringComparison.Ordinal);
    }

    // The Teams/other payload put broker names raw into its top-level "text", which Markdown-reading receivers render:
    // a queue named "[click](https://evil)" became a link there. The card body was already literal.
    [Fact]
    public void TheOtherPayloadsTopLevelTextKeepsBrokerNamesLiteral()
    {
        var alert = new MonitorAlert("Production", "[click](https://evil.example) **urgent** `x` <b>", 3, null);

        var payload = MonitorAlertService.BuildWebhookPayload("https://contoso.webhook.office.com/workflows/1", alert);
        var text = payload["text"]!.GetValue<string>();

        Assert.StartsWith("QueueLoom: dead letters — ", text, StringComparison.Ordinal);
        Assert.Contains(@"\[click\]\(https\:\/\/evil\.example\)", text, StringComparison.Ordinal);
        Assert.Contains(@"\*\*urgent\*\*", text, StringComparison.Ordinal);
        Assert.Contains(@"\`x\` \<b\>", text, StringComparison.Ordinal);
        Assert.DoesNotContain("[click](", text, StringComparison.Ordinal);
        // The card's TextRun shows the names as they are: it renders no Markdown.
        Assert.Contains("[click](https://evil.example)", payload.ToJsonString(), StringComparison.Ordinal);
    }

    // Entities, line breaks and block markers stay literal too: every ASCII punctuation is escaped and a control
    // character is shown as its Unicode control picture, so no name can form an entity, a list or a heading.
    [Theory]
    [InlineData("orders&amp;archive", @"orders\&amp\;archive")]
    [InlineData("orders\n\n- forged", "orders\u240A\u240A\\- forged")]
    [InlineData("orders\n1. forged", "orders\u240A1\\. forged")]
    [InlineData("orders\n===\nrest", "orders\u240A\\=\\=\\=\u240Arest")]
    [InlineData("orders\r\n    code", "orders\u240D\u240A    code")]
    [InlineData("a+b", @"a\+b")]
    public void TheOtherPayloadsTopLevelTextHasNoEntitiesOrBlocks(string source, string expected)
    {
        var payload = MonitorAlertService.BuildWebhookPayload("https://contoso.webhook.office.com/workflows/1", new MonitorAlert("Production", source, 3, null));
        var text = payload["text"]!.GetValue<string>();

        Assert.Contains(expected, text, StringComparison.Ordinal);
        Assert.DoesNotContain('\n', text);
        Assert.DoesNotContain('\r', text);
        // The card's TextRun keeps the name exactly.
        Assert.Contains(source, payload["attachments"]![0]!["content"]!["body"]![1]!["inlines"]![0]!["text"]!.GetValue<string>(), StringComparison.Ordinal);
    }

    // A host that only ends in "slack.com" is not Slack.
    [Theory]
    [InlineData("https://hooks.slack.com/services/T/B/X", true)]
    [InlineData("https://slack.com/api/x", true)]
    [InlineData("https://notslack.com/hook", false)]
    [InlineData("https://evilslack.com/hook", false)]
    public void OnlySlacksOwnHostsGetTheSlackPayload(string url, bool slack)
    {
        var payload = MonitorAlertService.BuildWebhookPayload(url, new MonitorAlert("Production", "orders", 3, null));

        Assert.Equal(!slack, payload.ContainsKey("attachments"));
    }
}
