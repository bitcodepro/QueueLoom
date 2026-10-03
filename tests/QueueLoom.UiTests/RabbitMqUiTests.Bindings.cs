using System.Net.Http.Headers;
using System.Text;
using Avalonia.Automation;
using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Threading;
using Avalonia.VisualTree;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using RabbitMQ.Client;

namespace QueueLoom.UiTests;

public sealed partial class RabbitMqUiTests
{
    // Emulator CI includes RabbitMqUiTests. Like the existing live-window fixture, this runs only with a broker.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task PresenceBindings_ModalEditorSavesReloadsAndDelivers(bool toExchange) => UiSession.RunAsync(async () =>
    {
        if (string.IsNullOrWhiteSpace(Broker)) return;
        var host = Broker.Split(':')[0];
        var port = int.Parse(Broker.Split(':')[1], System.Globalization.CultureInfo.InvariantCulture);
        var vhost = $"bindings-{Guid.NewGuid():N}";
        using var management = new HttpClient(new HttpClientHandler { UseProxy = false })
        { BaseAddress = new Uri($"http://{host}:{port + 10000}/") };
        management.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String("guest:guest"u8.ToArray()));
        (await management.PutAsync($"api/vhosts/{vhost}", null)).EnsureSuccessStatusCode();
        (await management.PutAsync($"api/permissions/{vhost}/guest",
            new StringContent("""{"configure":".*","write":".*","read":".*"}""", Encoding.UTF8, "application/json"))).EnsureSuccessStatusCode();
        try
        {
            await using var connection = await new ConnectionFactory { HostName = host, Port = port, VirtualHost = vhost }.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();
            await channel.ExchangeDeclareAsync("headers", ExchangeType.Headers, durable: true);
            // Shared names prove that destination metadata chooses the queue or exchange without guessing.
            await channel.ExchangeDeclareAsync("orders", ExchangeType.Direct, durable: true);
            await channel.QueueDeclareAsync("orders", durable: true, exclusive: false, autoDelete: false);
            await channel.QueueDeclareAsync("forwarded", durable: true, exclusive: false, autoDelete: false);
            await channel.QueueBindAsync("forwarded", "orders", "probe");
            var target = toExchange ? "forwarded" : "orders";
            var vault = new InMemorySecretVault();
            var profile = ServiceBusProfile.CreateNew("bindings", EnvironmentKind.Test, new(AuthenticationKind.RabbitMqPassword),
                accessMode: ProfileAccessMode.ReadWrite) with
            {
                Provider = MessagingProvider.RabbitMq, AllowQueueManagement = true,
                RabbitMq = new RabbitMqSettings(host, "guest", vhost, port, port + 10000)
            };
            await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), "guest");
            await using var workspace = new RabbitMqWorkspace(vault, httpHandler: new HttpClientHandler { UseProxy = false });
            await workspace.ConnectAsync(profile);
            await workspace.GetTopologyAsync(forceRefresh: true);

            SubscriptionRule? existing = null;
            for (var stage = 0; stage < 3; stage++)
            {
                var editor = new RuleEditorViewModel("headers", "orders", existing, RoutingService.RabbitMq,
                    bindingKind: RuleFilterKind.HeadersBinding);
                var rule = await SaveFromModalAsync(editor, changeMatch: stage == 2);
                await workspace.SaveSubscriptionRuleAsync("headers", "orders", rule with { ToExchange = toExchange }, replace: existing is not null);
                var destination = Assert.Single(await workspace.GetTopicRulesAsync("headers"),
                    item => item.Subscription == "orders" && item.IsExchange == toExchange);
                var reloaded = Assert.Single(destination.Rules);
                Assert.Null(reloaded.Arguments["trace"]);
                Assert.Equal(stage == 2 ? "any" : "all", reloaded.Arguments["x-match"]);
                Assert.Equal(toExchange, reloaded.ToExchange);
                if (stage == 1) Assert.Equal(existing!.Name, reloaded.Name);
                if (stage == 2) Assert.NotEqual(existing!.Name, reloaded.Name);
                existing = reloaded;

                await AssertDeliveryAsync(new() { ["trace"] = "present", ["region"] = "EU" }, true);
                await AssertDeliveryAsync(new() { ["trace"] = "present", ["region"] = "US" }, stage == 2);
                await AssertDeliveryAsync(new() { ["region"] = "US" }, false);
                // No header binding was accidentally added to the other destination sharing this name.
                Assert.Null(await channel.BasicGetAsync(toExchange ? "orders" : "forwarded", autoAck: true));
            }

            async Task AssertDeliveryAsync(Dictionary<string, object?> headers, bool expected)
            {
                await channel.BasicPublishAsync("headers", "probe", false,
                    new BasicProperties { Headers = headers }, "probe"u8.ToArray());
                // A synchronous RPC on the same channel is a barrier after publishing and routing.
                await channel.QueueDeclarePassiveAsync(target);
                var message = await channel.BasicGetAsync(target, autoAck: true);
                Assert.Equal(expected, message is not null);
                if (message is not null) Assert.Equal("probe"u8.ToArray(), message.Body.ToArray());
                Assert.Null(await channel.BasicGetAsync(target, autoAck: true));
            }
        }
        finally { await management.DeleteAsync($"api/vhosts/{vhost}"); }
    });

    private static async Task<SubscriptionRule> SaveFromModalAsync(RuleEditorViewModel editor, bool changeMatch)
    {
        BindingErrors.Instance.Clear();
        var owner = new Window();
        var window = new RuleEditorWindow(editor);
        owner.Show();
        var result = window.ShowDialog<SubscriptionRule?>(owner);
        try
        {
            Dispatcher.UIThread.RunJobs();
            var input = window.GetVisualDescendants().OfType<TextBox>()
                .Single(control => AutomationProperties.GetName(control) == "Binding headers");
            input.Text = "trace = null\nregion = 'EU'";
            if (changeMatch)
            {
                var match = window.GetVisualDescendants().OfType<ComboBox>()
                    .Single(control => ReferenceEquals(control.ItemsSource, RuleEditorViewModel.HeaderModes));
                match.SelectedIndex = 1;
            }
            Dispatcher.UIThread.RunJobs();
            var confirm = window.GetVisualDescendants().OfType<Button>().Single(control => Equals(control.Content, editor.ConfirmLabel));
            confirm.RaiseEvent(new RoutedEventArgs(Button.ClickEvent));
            var saved = Assert.IsType<SubscriptionRule>(await result.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Null(saved.Arguments["trace"]);
            Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
            return saved;
        }
        finally { window.Close(); owner.Close(); }
    }
}
