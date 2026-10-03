using System.Net.Http.Headers;
using System.Text;
using System.Runtime.CompilerServices;
using System.Reflection;
using System.Text.Json;
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
    private readonly ITestOutputHelper _bindingOutput;
    public RabbitMqUiTests(ITestOutputHelper output) => _bindingOutput = output;
    // Emulator CI includes RabbitMqUiTests. Missing broker configuration is an explicit skip, not a pass.
    [RabbitBrokerTheory]
    [InlineData(false)]
    [InlineData(true)]
    public Task PresenceBindings_ModalEditorSavesReloadsAndDelivers(bool toExchange) => UiSession.RunAsync(async () =>
    {
        var host = Broker!.Split(':')[0];
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
            using var overview = JsonDocument.Parse(await management.GetStringAsync("api/overview"));
            var erlang = overview.RootElement.GetProperty("erlang_version").GetString()!;
            _bindingOutput.WriteLine($"RabbitMQ {overview.RootElement.GetProperty("rabbitmq_version").GetString()}, Erlang/OTP {erlang}");
            Assert.True(int.Parse(erlang.Split('.')[0], System.Globalization.CultureInfo.InvariantCulture) >= 27,
                "The signed-zero fixture requires OTP 27+ (supported by RabbitMQ 4.1+).");
            await using var connection = await new ConnectionFactory { HostName = host, Port = port, VirtualHost = vhost }.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();
            await channel.ExchangeDeclareAsync("headers", ExchangeType.Headers, durable: true);
            // Shared names prove that destination metadata chooses the queue or exchange without guessing.
            await channel.ExchangeDeclareAsync("orders", ExchangeType.Direct, durable: true);
            await channel.QueueDeclareAsync("orders", durable: true, exclusive: false, autoDelete: false);
            await channel.QueueDeclareAsync("forwarded", durable: true, exclusive: false, autoDelete: false);
            await channel.QueueDeclareAsync("literal-values", durable: true, exclusive: false, autoDelete: false);
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
            await workspace.SaveSubscriptionRuleAsync("headers", "literal-values", new("", RuleFilterKind.HeadersBinding)
            { ToExchange = false, Arguments = new Dictionary<string, object?> { ["x-match"] = "all", ["trace"] = "undefined", ["region"] = "LITERAL" } }, false);

            SubscriptionRule? existing = null;
            for (var stage = 0; stage < 3; stage++)
            {
                var editor = new RuleEditorViewModel("headers", "orders", existing, RoutingService.RabbitMq,
                    bindingKind: RuleFilterKind.HeadersBinding);
                var rule = await SaveFromModalAsync(editor, changeMatch: stage == 2);
                try
                {
                    await workspace.SaveSubscriptionRuleAsync("headers", "orders", rule with { ToExchange = toExchange }, replace: existing is not null);
                }
                finally
                {
                    var rawBindings = await management.GetStringAsync($"api/exchanges/{vhost}/headers/bindings/source");
                    _bindingOutput.WriteLine($"Raw management GET after stage {stage}, exchange={toExchange}: {rawBindings}");
                    using var typedRequest = new HttpRequestMessage(HttpMethod.Get, $"api/exchanges/{vhost}/headers/bindings/source");
                    typedRequest.Headers.Accept.ParseAdd("application/bert");
                    using var typedResponse = await management.SendAsync(typedRequest);
                    _bindingOutput.WriteLine($"Typed management response {typedResponse.Content.Headers.ContentType}: " +
                        Convert.ToBase64String(await typedResponse.Content.ReadAsByteArrayAsync()));
                }
                var destination = Assert.Single(await workspace.GetTopicRulesAsync("headers"),
                    item => item.Subscription == "orders" && item.IsExchange == toExchange);
                var reloaded = Assert.Single(destination.Rules);
                Assert.Null(reloaded.Arguments["trace"]);
                Assert.Equal(stage == 2 ? "any" : "all", reloaded.Arguments["x-match"]);
                Assert.Equal(toExchange, reloaded.ToExchange);
                if (stage == 1) Assert.Equal(existing!.Name, reloaded.Name);
                if (stage == 2) Assert.NotEqual(existing!.Name, reloaded.Name);
                existing = reloaded;
                var literal = Assert.Single(await workspace.GetTopicRulesAsync("headers"), item => item.Subscription == "literal-values");
                Assert.Equal("undefined", Assert.IsType<string>(Assert.Single(literal.Rules).Arguments["trace"]));

                await AssertDeliveryAsync(new() { ["trace"] = "present", ["region"] = "EU" }, true);
                await AssertDeliveryAsync(new() { ["trace"] = "present", ["region"] = "US" }, stage == 2);
                await AssertDeliveryAsync(new() { ["region"] = "US" }, false);
                // No header binding was accidentally added to the other destination sharing this name.
                Assert.Null(await channel.BasicGetAsync(toExchange ? "orders" : "forwarded", autoAck: true));
            }

            // Whole-valued floating numbers must keep their AMQP type across both transports and identity lookup.
            foreach (var floating in new[] { false, true, true, false })
            {
                var editor = new RuleEditorViewModel("headers", "orders", existing, RoutingService.RabbitMq);
                var rule = await SaveFromModalAsync(editor, changeMatch: false,
                    headers: floating ? "trace = null\namount = 250.0" : "trace = null\namount = 250", matchIndex: 0);
                var previous = existing!;
                await workspace.SaveSubscriptionRuleAsync("headers", "orders", rule with { ToExchange = toExchange }, true);
                existing = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers"),
                    item => item.Subscription == "orders" && item.IsExchange == toExchange).Rules);
                Assert.Equal(floating ? typeof(double) : typeof(long), existing.Arguments["amount"]!.GetType());
                if (previous.Arguments.TryGetValue("amount", out var amount) && amount?.GetType() == existing.Arguments["amount"]!.GetType())
                    Assert.Equal(previous.Name, existing.Name);
                else Assert.NotEqual(previous.Name, existing.Name);
                await AssertDeliveryAsync(new() { ["trace"] = "present", ["amount"] = 250L }, !floating);
                await AssertDeliveryAsync(new() { ["trace"] = "present", ["amount"] = 250d }, floating);
            }

            foreach (var negative in new[] { false, true, true, false })
            {
                var editor = new RuleEditorViewModel("headers", "orders", existing, RoutingService.RabbitMq);
                var rule = await SaveFromModalAsync(editor, false,
                    headers: negative ? "trace = null\namount = -0.0" : "trace = null\namount = 0.0", matchIndex: 0);
                var previous = existing!;
                await workspace.SaveSubscriptionRuleAsync("headers", "orders", rule with { ToExchange = toExchange }, true);
                existing = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers"),
                    item => item.Subscription == "orders" && item.IsExchange == toExchange).Rules);
                var zeroBits = negative ? long.MinValue : 0L;
                Assert.Equal(zeroBits, BitConverter.DoubleToInt64Bits(Assert.IsType<double>(existing.Arguments["amount"])));
                if (previous.Arguments["amount"] is double old && BitConverter.DoubleToInt64Bits(old) == zeroBits)
                    Assert.Equal(previous.Name, existing.Name);
                else Assert.NotEqual(previous.Name, existing.Name);
                await AssertDeliveryAsync(new() { ["trace"] = "present", ["amount"] = 0d }, !negative);
                await AssertDeliveryAsync(new() { ["trace"] = "present", ["amount"] = BitConverter.Int64BitsToDouble(long.MinValue) }, negative);
            }

            // Broker-close the isolated workspace connection, wait for actual recovery, then inspect persisted
            // topology. Replacing/deleting a durable binding must not leave autorecovery records that recreate it.
            await RecoverWorkspaceConnectionAsync();
            var retained = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers"),
                item => item.Subscription == "orders" && item.IsExchange == toExchange).Rules);
            Assert.Equal(existing!.Name, retained.Name);
            await workspace.DeleteSubscriptionRuleAsync("headers", "orders", retained);
            await RecoverWorkspaceConnectionAsync();
            Assert.DoesNotContain(await workspace.GetTopicRulesAsync("headers"), item => item.Subscription == "orders");
            Assert.Single(await workspace.GetTopicRulesAsync("headers"), item => item.Subscription == "literal-values");

            async Task RecoverWorkspaceConnectionAsync()
            {
                var main = (IConnection)typeof(RabbitMqWorkspace).GetField("_connection", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(workspace)!;
                var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                Task OnRecovery(object sender, RabbitMQ.Client.Events.AsyncEventArgs args) { recovered.TrySetResult(); return Task.CompletedTask; }
                main.RecoverySucceededAsync += OnRecovery;
                try
                {
                    var connectionName = await FindWorkspaceConnectionAsync(main);
                    using var closed = await management.DeleteAsync($"api/connections/{Uri.EscapeDataString(connectionName)}");
                    closed.EnsureSuccessStatusCode();
                    await recovered.Task.WaitAsync(TimeSpan.FromSeconds(45));
                    _bindingOutput.WriteLine("Isolated workspace recovered after HTTP connection close; binding state verified.");
                }
                finally { main.RecoverySucceededAsync -= OnRecovery; }
            }

            async Task<string> FindWorkspaceConnectionAsync(IConnection main)
            {
                Assert.Equal("QueueLoom", main.ClientProvidedName);
                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
                try
                {
                    while (true)
                    {
                        using var json = JsonDocument.Parse(await management.GetStringAsync("api/connections", deadline.Token));
                        var candidates = json.RootElement.EnumerateArray().Where(item =>
                            item.TryGetProperty("vhost", out var hostValue) && hostValue.GetString() == vhost &&
                            item.TryGetProperty("client_properties", out var properties) &&
                            properties.TryGetProperty("connection_name", out var name) && name.GetString() == main.ClientProvidedName).ToArray();
                        if (candidates.Length > 0)
                            return Assert.Single(candidates).GetProperty("name").GetString()!;
                        // The management statistics registry is eventually consistent. Recheck the exact
                        // vhost/client identity until the deadline; never select a different connection.
                        await Task.Delay(200, deadline.Token);
                    }
                }
                catch (OperationCanceledException) when (deadline.IsCancellationRequested)
                {
                    throw new TimeoutException($"Management did not list the exact '{main.ClientProvidedName}' connection in isolated vhost '{vhost}' within 20 seconds.");
                }
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

    private static async Task<SubscriptionRule> SaveFromModalAsync(RuleEditorViewModel editor, bool changeMatch,
        string headers = "trace = null\nregion = 'EU'", int? matchIndex = null)
    {
        BindingErrors.Instance.Clear();
        var owner = new Window();
        var window = new RuleEditorWindow(editor);
        owner.Show();
        var result = window.ShowDialog<SubscriptionRule?>(owner);
        try
        {
            await SettleBindingDialogAsync();
            var input = window.GetVisualDescendants().OfType<TextBox>()
                .Single(control => AutomationProperties.GetName(control) == "Binding headers");
            input.Text = headers;
            if (changeMatch || matchIndex is not null)
            {
                var match = window.GetVisualDescendants().OfType<ComboBox>()
                    .Single(control => AutomationProperties.GetName(control) == "Header match");
                match.SelectedIndex = matchIndex ?? 1;
            }
            await SettleBindingDialogAsync();
            var confirm = window.GetVisualDescendants().OfType<Button>().Single(control => Equals(control.Content, editor.ConfirmLabel));
            confirm.RaiseEvent(new RoutedEventArgs(Button.ClickEvent));
            var saved = Assert.IsType<SubscriptionRule>(await result.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Null(saved.Arguments["trace"]);
            Assert.True(BindingErrors.Instance.Messages.Count == 0, string.Join(Environment.NewLine, BindingErrors.Instance.Messages));
            return saved;
        }
        finally { window.Close(); owner.Close(); }
    }

    private static async Task SettleBindingDialogAsync()
    {
        for (var attempt = 0; attempt < 3; attempt++) { Dispatcher.UIThread.RunJobs(); await Task.Delay(10); }
        Dispatcher.UIThread.RunJobs();
    }

    private sealed class RabbitBrokerTheoryAttribute : TheoryAttribute
    {
        public RabbitBrokerTheoryAttribute([CallerFilePath] string? sourceFilePath = null, [CallerLineNumber] int sourceLineNumber = 0)
            : base(sourceFilePath, sourceLineNumber)
        {
            if (string.IsNullOrWhiteSpace(Broker)) Skip = "QUEUELOOM_RABBITMQ is not configured; live binding persistence requires emulator CI.";
        }
    }
}
