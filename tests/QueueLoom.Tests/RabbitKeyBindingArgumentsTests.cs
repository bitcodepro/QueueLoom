using System.Net;
using System.Reflection;
using System.Text.Json;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

// A direct or topic binding is identified by its routing key and its arguments ("a 'name' for the binding composed of
// its routing key and a hash of its arguments", RabbitMQ HTTP API reference). Changing its key used to declare the
// replacement with no arguments at all, so whatever the binding carried was silently lost.
public sealed class RabbitKeyBindingArgumentsTests
{
    [Theory]
    [InlineData("direct")]
    [InlineData("topic")]
    public async Task ChangingTheKey_KeepsTheEditedBindingsArgumentsAndDeletesOnlyThatBinding(string exchangeType)
    {
        var broker = new KeyBindingBroker(exchangeType);
        broker.Bindings.Add(new("orders.created", "orders.created~keep", new() { ["x-trace"] = "on", ["x-priority"] = 5L }));
        // Same queue and key, other arguments: a different binding that must stay exactly as it is.
        broker.Bindings.Add(new("orders.created", "orders.created~other", new() { ["x-trace"] = "off" }));
        await using var workspace = broker.Workspace();
        var existing = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("events")).Rules, rule => rule.Name == "orders.created~keep");
        var editor = new RuleEditorViewModel("events", "orders", existing, RoutingService.RabbitMq) { BindingKey = "orders.updated" };
        var rule = Assert.IsType<SubscriptionRule>(editor.TryBuild()) with { ToExchange = false };

        await workspace.SaveSubscriptionRuleAsync("events", "orders", rule, replace: true);

        var posted = Assert.Single(broker.Posts);
        Assert.Equal("on", posted.GetProperty("arguments").GetProperty("x-trace").GetString());
        Assert.Equal(5, posted.GetProperty("arguments").GetProperty("x-priority").GetInt64());
        Assert.Equal(["orders.created~keep"], broker.Deleted);
        var rules = Assert.Single(await workspace.GetTopicRulesAsync("events")).Rules;
        Assert.Equal(2, rules.Count);
        var changed = Assert.Single(rules, item => item.Expression == "orders.updated");
        Assert.Equal("on", changed.Arguments["x-trace"]);
        Assert.Equal(5L, changed.Arguments["x-priority"]);
        Assert.Equal("off", Assert.Single(rules, item => item.Name == "orders.created~other").Arguments["x-trace"]);
    }

    [Fact]
    public async Task UnchangedKey_WithArgumentsChangesNothing()
    {
        var broker = new KeyBindingBroker("direct");
        broker.Bindings.Add(new("orders.created", "orders.created~keep", new() { ["x-trace"] = "on" }));
        await using var workspace = broker.Workspace();
        var existing = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("events")).Rules);
        var rule = Assert.IsType<SubscriptionRule>(new RuleEditorViewModel("events", "orders", existing, RoutingService.RabbitMq).TryBuild())
            with { ToExchange = false };

        await workspace.SaveSubscriptionRuleAsync("events", "orders", rule, replace: true);

        Assert.Empty(broker.Posts);
        Assert.Empty(broker.Deleted);
    }

    private sealed record Binding(string RoutingKey, string PropertiesKey, Dictionary<string, object?> Arguments);

    private sealed class KeyBindingBroker(string exchangeType) : HttpMessageHandler
    {
        public List<Binding> Bindings { get; } = [];
        public List<JsonElement> Posts { get; } = [];
        public List<string> Deleted { get; } = [];

        public RabbitMqWorkspace Workspace()
        {
            var workspace = new RabbitMqWorkspace(new DeepAuditCloudTests.EmptyVault());
            Set(workspace, "_management", new HttpClient(this) { BaseAddress = new Uri("http://broker.invalid/") });
            Set(workspace, "_index", new RabbitMqTopologyIndex(
                [new("orders", "classic", 0, 0, null, null, null, "running")], [new("events", exchangeType)], []));
            typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
                ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Test, new(AuthenticationKind.RabbitMqPassword),
                    accessMode: ProfileAccessMode.ReadWrite) with
                { Provider = MessagingProvider.RabbitMq, AllowQueueManagement = true, RabbitMq = new("broker.invalid", "guest") });
            return workspace;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;
            if (request.Method == HttpMethod.Post)
            {
                using var body = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken));
                Posts.Add(body.RootElement.Clone());
                var key = body.RootElement.GetProperty("routing_key").GetString()!;
                var arguments = body.RootElement.GetProperty("arguments").EnumerateObject().ToDictionary(item => item.Name,
                    item => item.Value.ValueKind == JsonValueKind.Number ? item.Value.GetInt64() : (object?)item.Value.GetString());
                var propertiesKey = arguments.Count == 0 ? key : $"{key}~{Posts.Count}";
                Bindings.Add(new(key, propertiesKey, arguments));
                var created = new HttpResponseMessage(HttpStatusCode.Created) { Content = new StringContent("") };
                created.Headers.Location = new Uri($"http://broker.invalid{path}/{Uri.EscapeDataString(propertiesKey)}");
                return created;
            }
            if (request.Method == HttpMethod.Delete)
            {
                var key = Uri.UnescapeDataString(path[(path.LastIndexOf('/') + 1)..]);
                Deleted.Add(key);
                Bindings.RemoveAll(binding => binding.PropertiesKey == key);
                return new HttpResponseMessage(HttpStatusCode.NoContent);
            }
            if (path.EndsWith("/events", StringComparison.Ordinal))
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent($$"""{"type":"{{exchangeType}}"}""") };
            var terms = Bindings.Select(binding => (object?)new Dictionary<string, object?>
            {
                ["source"] = "events", ["destination"] = "orders", ["destination_type"] = "queue",
                ["routing_key"] = binding.RoutingKey, ["properties_key"] = binding.PropertiesKey,
                ["arguments"] = new Dictionary<string, object?>(binding.Arguments)
            }).ToArray();
            var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(RabbitBindingEtfFixture.Encode(terms)) };
            response.Content.Headers.ContentType = new("application/bert");
            return response;
        }
    }

    private static void Set(object target, string name, object value) =>
        target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(target, value);
}
