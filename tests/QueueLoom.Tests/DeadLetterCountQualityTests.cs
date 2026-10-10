using System.Globalization;
using System.Reflection;
using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Mcp;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// Every dead-letter count carries how far it can be trusted: exact, estimated (SQS, Pub/Sub Cloud Monitoring), a lower
// bound (Pub/Sub counted by reading), or unqualified (old history). An unknown count has no value at all.
public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(1_234, DeadLetterCountQuality.Exact, "1,234")]
    [InlineData(1_234, DeadLetterCountQuality.Estimated, "≈1,234")]
    [InlineData(1_000, DeadLetterCountQuality.LowerBound, "1,000+")]
    [InlineData(0, DeadLetterCountQuality.LowerBound, "none seen")]
    [InlineData(1_234, DeadLetterCountQuality.Unqualified, "1,234")]
    public void EachQualityIsWrittenDifferently(long count, DeadLetterCountQuality quality, string expected) =>
        Assert.Equal(expected, DeadLetterCountText.Format(count, quality, CultureInfo.InvariantCulture));

    [Theory]
    [InlineData(5, DeadLetterCountQuality.Exact, "+5")]
    [InlineData(-3, DeadLetterCountQuality.Exact, "−3")]
    [InlineData(5, DeadLetterCountQuality.Estimated, "≈+5")]
    [InlineData(20, DeadLetterCountQuality.LowerBound, "+20+")]
    public void AChangeKeepsItsQuality(long change, DeadLetterCountQuality quality, string expected) =>
        Assert.Equal(expected, DeadLetterCountText.FormatChange(change, quality, CultureInfo.InvariantCulture));

    [Theory]
    [InlineData(new[] { DeadLetterCountQuality.Exact, DeadLetterCountQuality.Exact }, DeadLetterCountQuality.Exact)]
    [InlineData(new[] { DeadLetterCountQuality.Exact, DeadLetterCountQuality.Unqualified }, DeadLetterCountQuality.Unqualified)]
    [InlineData(new[] { DeadLetterCountQuality.Unqualified, DeadLetterCountQuality.Estimated }, DeadLetterCountQuality.Estimated)]
    [InlineData(new[] { DeadLetterCountQuality.Estimated, DeadLetterCountQuality.LowerBound }, DeadLetterCountQuality.LowerBound)]
    public void ATotalIsNoMoreCertainThanItsLeastCertainPart(DeadLetterCountQuality[] parts, DeadLetterCountQuality expected) =>
        Assert.Equal(expected, DeadLetterCountQualities.Combine(parts));

    // SQS reports ApproximateNumberOf…: the topology marks it, and a scan shows "≈" with an approximate change.
    [Fact]
    public async Task SqsCountsAreEstimatesInTheTopologyAndTheScan()
    {
        var queue = new AwsQueueInfo("orders", "http://localhost/orders", "arn:aws:sqs:us-east-1:123:orders",
            false, 0, 0, 0, "arn:aws:sqs:us-east-1:123:orders-dlq", null, null);
        var deadLetterQueue = new AwsQueueInfo("orders-dlq", "http://localhost/orders-dlq", "arn:aws:sqs:us-east-1:123:orders-dlq",
            false, 25, 0, 0, null, null, null);
        var topology = new AwsTopologyIndex([queue, deadLetterQueue], []).ToTopology(DateTimeOffset.UtcNow);
        Assert.True(topology.Queues.Single(item => item.Name == "orders").Runtime.CountsAreEstimates);

        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var source = ServiceBusEntityReference.Queue("orders");
        DeadLetterSnapshot Estimated(long count) => Snapshot(dev.Id, new DeadLetterEntitySnapshot(source, count) { CountQuality = DeadLetterCountQuality.Estimated });
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Estimated(25) } };
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();
        workspace.Snapshots[dev.Id] = Estimated(30);
        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();

        var row = Assert.Single(vm.DeadLetterSources);
        Assert.StartsWith("≈", row.CountText, StringComparison.Ordinal);
        Assert.Equal("≈+5", row.Delta.Replace(",", string.Empty, StringComparison.Ordinal));
        Assert.StartsWith("≈", vm.GlobalDlqDisplay, StringComparison.Ordinal);
        Assert.NotNull(row.CountNote);
    }

    // The monitor alerts on estimated growth too, but says it is approximate, in the source and in the total.
    [Fact]
    public async Task EstimatedGrowthIsAlertedAsApproximate()
    {
        var dev = CreateProfile("Development", EnvironmentKind.Development);
        var source = ServiceBusEntityReference.Queue("orders");
        DeadLetterSnapshot Estimated(long count) => Snapshot(dev.Id, new DeadLetterEntitySnapshot(source, count) { CountQuality = DeadLetterCountQuality.Estimated });
        var workspace = new FakeWorkspace { Snapshots = { [dev.Id] = Estimated(10) } };
        var alerts = new RecordingAlerts();
        await using var vm = CreateViewModel(new FakeProfileRepository([dev], dev.Id), workspace, alerts: alerts);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.MonitorScope = "All environments";
        var check = typeof(MainWindowViewModel).GetMethod("RunMonitorCheckAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;
        workspace.Snapshots[dev.Id] = Estimated(15);
        await (Task<bool>)check.Invoke(vm, [CancellationToken.None])!;

        Assert.Contains("increased by ≈5;", vm.MonitorAlert, StringComparison.Ordinal);
        await WaitUntilAsync(() => alerts.System.Count == 2);
        // Alerts are sent in the background, so the list's order is not the checks' order.
        Assert.Contains(alerts.System, alert => alert.Text.Contains("≈15 dead-lettered messages (was ≈10)", StringComparison.Ordinal));
        Assert.Equal(DeadLetterCountQuality.Estimated, Assert.Single(vm.MonitorNotifications).CountQuality);
    }

    // History written before count quality was kept reads as unqualified: shown as before, never claimed exact.
    [Fact]
    public void AnOldHistorySampleIsUnqualifiedNotExact()
    {
        var at = DateTimeOffset.Parse("2026-10-01T10:00:00Z", CultureInfo.InvariantCulture);
        var profile = Guid.NewGuid();
        var legacy = JsonSerializer.Deserialize<DeadLetterHistorySample>(
            "{\"At\":\"2026-10-01T10:00:00+00:00\",\"ProfileId\":\"" + profile + "\",\"Environment\":\"Test\",\"Total\":40,\"Sources\":{\"orders\":40}}")!;
        var current = DeadLetterHistorySample.FromSnapshot(new DeadLetterSnapshot(profile, at.AddMinutes(5),
            [new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 45)]), "Test");

        Assert.Equal(DeadLetterCountQuality.Unqualified, legacy.Quality);
        Assert.Equal(DeadLetterCountQuality.Unqualified, legacy.QualityOf("orders"));
        var summary = DeadLetterHistory.Summarize([legacy, current], at, at.AddMinutes(10))!;
        Assert.Equal(DeadLetterCountQuality.Unqualified, summary.StartQuality);
        Assert.Equal(DeadLetterCountQuality.Exact, summary.NowQuality);
        Assert.Equal(5, summary.Change);
        Assert.Equal(DeadLetterCountQuality.Unqualified, summary.ChangeQuality);
        Assert.Equal(DeadLetterCountQuality.Unqualified, summary.Peak.Quality);
    }

    [Fact]
    public void McpNamesEveryQuality()
    {
        var estimated = McpMapping.ToInfo(new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 7) { CountQuality = DeadLetterCountQuality.Estimated });
        var exact = McpMapping.ToInfo(new DeadLetterEntitySnapshot(ServiceBusEntityReference.Queue("orders"), 7));
        Assert.Equal("estimated", estimated.CountQuality);
        Assert.Equal("exact", exact.CountQuality);
        Assert.Equal("unqualified", DeadLetterCountQualities.Name(DeadLetterCountQuality.Unqualified));
    }
}
