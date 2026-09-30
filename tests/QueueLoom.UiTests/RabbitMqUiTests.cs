using System.Net.Http.Headers;
using System.Text;
using Avalonia.Headless;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using RabbitMQ.Client;

namespace QueueLoom.UiTests;

/// <summary>
/// Drives the real window against a RabbitMQ broker. Runs only when QUEUELOOM_RABBITMQ is host:port (management
/// on port + 10000, user guest); writes screenshots when QUEUELOOM_SCREENSHOT_DIR is set too.
/// </summary>
public sealed class RabbitMqUiTests
{
    private static readonly string? Broker = Environment.GetEnvironmentVariable("QUEUELOOM_RABBITMQ");
    private static readonly string? ScreenshotDirectory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");

    [Fact]
    public Task The_app_explores_and_reads_rabbitmq_dead_letters() => UiSession.RunAsync(async () =>
    {
        if (string.IsNullOrWhiteSpace(Broker))
        {
            return;
        }

        var host = Broker.Split(':')[0];
        var port = int.Parse(Broker.Split(':')[1], System.Globalization.CultureInfo.InvariantCulture);
        var vhost = $"billing-{Guid.NewGuid():N}"[..16];
        using var management = new HttpClient { BaseAddress = new Uri($"http://{host}:{port + 10000}/") };
        management.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String("guest:guest"u8.ToArray()));
        (await management.PutAsync($"api/vhosts/{vhost}", null)).EnsureSuccessStatusCode();
        (await management.PutAsync($"api/permissions/{vhost}/guest",
            new StringContent("""{"configure":".*","write":".*","read":".*"}""", Encoding.UTF8, "application/json"))).EnsureSuccessStatusCode();
        try
        {
            await SeedAsync(host, port, vhost);
            var root = Path.Combine(Path.GetTempPath(), "queueloom-ui-rabbit", Guid.NewGuid().ToString("N"));
            var vault = new InMemorySecretVault();
            var rabbit = DemoData.RabbitStaging with { RabbitMq = new RabbitMqSettings(host, "guest", vhost, port, port + 10000) };
            await vault.StoreAsync(ProfileSecretKey.ConnectionString(rabbit.Id), "guest");
            await using var workspace = MessagingWorkspaces.Create(vault, QueueLoomPaths.ForRoot(root));
            await using var fixture = await WindowFixture.OpenWithAsync(workspace, vault, DemoData.Production, rabbit);
            fixture.ViewModel.ThemePreference = AppThemePreference.Dark;

            fixture.ViewModel.SelectedProfile = fixture.ViewModel.Profiles.Single(profile => profile.Id == rabbit.Id);
            await fixture.ViewModel.ConnectCommand.ExecuteAsync();
            Assert.True(fixture.ViewModel.IsConnected, fixture.ViewModel.StatusText);
            Assert.Equal(MessagingProvider.RabbitMq, fixture.ViewModel.ConnectedProvider);
            for (var attempt = 0; attempt < 30 && fixture.ViewModel.Entities.SingleOrDefault(entity => entity.Name == "invoices")?.DeadLetters != 3; attempt++)
            {
                await Task.Delay(500);
                await fixture.ViewModel.RefreshTopologyCommand.ExecuteAsync();
            }
            await fixture.ViewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
            await fixture.NavigateAsync("Explorer");
            Assert.Equal(3, fixture.ViewModel.Entities.Single(entity => entity.Name == "invoices").DeadLetters);
            Assert.Contains("Dead-letter queue of invoices", fixture.ViewModel.Entities.Single(entity => entity.Name == "invoices.dlq").Detail);
            Save(fixture, "rabbitmq-explorer.png");

            await fixture.NavigateAsync("DeadLetters");
            fixture.ViewModel.SelectedDlqSource = fixture.ViewModel.FilteredDeadLetterSources.Single(source => source.EntityName == "invoices");
            await fixture.ViewModel.BrowseDlqSourceCommand.ExecuteAsync();
            fixture.ViewModel.SelectedMessage = fixture.ViewModel.Messages.First();
            await fixture.SettleAsync();
            Assert.Equal(3, fixture.ViewModel.Messages.Count);
            Assert.All(fixture.ViewModel.Messages, message => Assert.Equal("Rejected by a consumer", message.DeadLetterReason));
            Save(fixture, "rabbitmq-dead-letters.png");
        }
        finally
        {
            await management.DeleteAsync($"api/vhosts/{vhost}");
        }
    });

    private static async Task SeedAsync(string host, int port, string vhost)
    {
        await using var connection = await new ConnectionFactory { HostName = host, Port = port, VirtualHost = vhost }.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        await channel.ExchangeDeclareAsync("billing", ExchangeType.Topic, durable: true);
        await channel.ExchangeDeclareAsync("billing.dlx", ExchangeType.Fanout, durable: true);
        await channel.QueueDeclareAsync("invoices.dlq", durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync("invoices.dlq", "billing.dlx", string.Empty);
        await channel.QueueDeclareAsync("invoices", durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum", ["x-dead-letter-exchange"] = "billing.dlx" });
        await channel.QueueBindAsync("invoices", "billing", "invoice.#");
        await channel.QueueDeclareAsync("reminders", durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync("reminders", "billing", "reminder.*");

        var invoices = new[] { ("INV-1042", "ACME GmbH", 1250.00), ("INV-1043", "Globex", 89.90), ("INV-1044", "Initech", 5400.00), ("INV-1045", "Umbrella", 12.00) };
        foreach (var (number, customer, total) in invoices)
        {
            await channel.BasicPublishAsync("billing", "invoice.created", true,
                new BasicProperties
                {
                    MessageId = number, ContentType = "application/json", Persistent = true, CorrelationId = $"order-{number[4..]}",
                    Headers = new Dictionary<string, object?> { ["tenant"] = Encoding.UTF8.GetBytes("eu-1") }
                },
                Encoding.UTF8.GetBytes($$"""{"invoice":"{{number}}","customer":"{{customer}}","total":{{total.ToString(System.Globalization.CultureInfo.InvariantCulture)}},"currency":"EUR"}"""));
        }
        await channel.BasicPublishAsync("billing", "reminder.sent", true, new BasicProperties { MessageId = "R-1" }, "{}"u8.ToArray());

        for (var index = 0; index < 3; index++)
        {
            var message = await channel.BasicGetAsync("invoices", autoAck: false);
            await channel.BasicRejectAsync(message!.DeliveryTag, requeue: false);
        }
    }

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
