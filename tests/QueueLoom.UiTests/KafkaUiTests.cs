using System.Text;
using Avalonia.Headless;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.UiTests;

/// <summary>
/// Drives the real window against a Kafka broker. Runs only when QUEUELOOM_KAFKA names the bootstrap servers;
/// writes screenshots when QUEUELOOM_SCREENSHOT_DIR is set too.
/// </summary>
public sealed class KafkaUiTests
{
    private static readonly string? Servers = Environment.GetEnvironmentVariable("QUEUELOOM_KAFKA");
    private static readonly string? ScreenshotDirectory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");

    [Fact]
    public Task The_app_reads_kafka_dead_letter_topics_and_offers_only_what_kafka_can_do() => UiSession.RunAsync(async () =>
    {
        if (string.IsNullOrWhiteSpace(Servers))
        {
            return;
        }

        var run = Guid.NewGuid().ToString("N")[..6];
        var shipments = $"shipments-{run}";
        var topics = new[] { shipments, shipments + ".DLT", $"tracking-{run}" };
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Servers }).Build();
        await admin.CreateTopicsAsync(topics.Select(name => new TopicSpecification { Name = name, NumPartitions = 2, ReplicationFactor = 1 }));
        try
        {
            using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = Servers }).Build())
            {
                for (var index = 1; index <= 6; index++)
                {
                    await producer.ProduceAsync(shipments, new Message<string, string> { Key = $"parcel-{index}", Value = $$"""{"parcel":{{index}},"status":"sent"}""" });
                }
                foreach (var (parcel, error) in new[] { (3, "Address not found"), (5, "Carrier timeout") })
                {
                    await producer.ProduceAsync(shipments + ".DLT", new Message<string, string>
                    {
                        Key = $"parcel-{parcel}",
                        Value = $$"""{"parcel":{{parcel}},"status":"sent"}""",
                        Headers = new Headers
                        {
                            { "kafka_dlt-exception-fqcn", Encoding.UTF8.GetBytes("com.example.shipping.ShipmentException") },
                            { "kafka_dlt-exception-message", Encoding.UTF8.GetBytes(error) },
                            { "kafka_dlt-original-topic", Encoding.UTF8.GetBytes(shipments) }
                        }
                    });
                }
            }

            var root = Path.Combine(Path.GetTempPath(), "queueloom-ui-kafka", Guid.NewGuid().ToString("N"));
            var vault = new InMemorySecretVault();
            var kafka = DemoData.KafkaDevelopment with { Kafka = new KafkaSettings(Servers) };
            await using var workspace = MessagingWorkspaces.Create(vault, QueueLoomPaths.ForRoot(root));
            await using var fixture = await WindowFixture.OpenWithAsync(workspace, vault, DemoData.Production, kafka);
            fixture.ViewModel.ThemePreference = AppThemePreference.Dark;
            fixture.ViewModel.SelectedProfile = fixture.ViewModel.Profiles.Single(profile => profile.Id == kafka.Id);
            await fixture.ViewModel.ConnectCommand.ExecuteAsync();
            await fixture.ViewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
            Assert.True(fixture.ViewModel.IsConnected, fixture.ViewModel.StatusText);

            await fixture.NavigateAsync("Explorer");
            fixture.ViewModel.SearchText = run;
            await fixture.SettleAsync();
            var row = fixture.ViewModel.Entities.Single(entity => entity.Name == shipments);
            Assert.Equal("TOPIC", row.KindLabel);
            Assert.Equal(6, row.Active);
            Assert.Equal(2, row.DeadLetters);
            Save(fixture, "kafka-explorer.png");

            await fixture.NavigateAsync("DeadLetters");
            fixture.ViewModel.SelectedDlqSource = fixture.ViewModel.FilteredDeadLetterSources.Single(source => source.EntityName == shipments);
            await fixture.ViewModel.BrowseDlqSourceCommand.ExecuteAsync();
            fixture.ViewModel.SelectedMessage = fixture.ViewModel.Messages.First();
            foreach (var message in fixture.ViewModel.Messages)
            {
                message.IsMarked = true;
            }
            await fixture.SettleAsync();
            Assert.Equal(["ShipmentException", "ShipmentException"], fixture.ViewModel.Messages.Select(message => message.DeadLetterReason));
            Assert.False(fixture.ViewModel.ShowDeleteMarkedMessages);
            Assert.False(new ResendDialogViewModel(fixture.ViewModel.Messages.Select(message => message.Message).ToArray(), [], kafka.Name, false,
                canRemoveOriginals: fixture.ViewModel.CanDeleteSelectedMessages).CanMove);
            Save(fixture, "kafka-dead-letters.png");
        }
        finally
        {
            await admin.DeleteTopicsAsync(topics);
        }
    });

    private static void Save(WindowFixture fixture, string name)
    {
        fixture.Window.UpdateLayout();
        using var frame = fixture.Window.CaptureRenderedFrame();
        if (string.IsNullOrWhiteSpace(ScreenshotDirectory))
        {
            return;
        }

        Directory.CreateDirectory(ScreenshotDirectory);
        using var file = File.Create(Path.Combine(ScreenshotDirectory, name));
        frame?.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
    }
}
