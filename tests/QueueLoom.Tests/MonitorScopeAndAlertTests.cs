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
}
