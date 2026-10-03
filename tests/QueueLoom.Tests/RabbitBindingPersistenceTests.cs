using System.Net;
using System.Reflection;
using System.Text.Json;
using QueueLoom.Core.Abstractions;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using RabbitMQ.Client;
using Xunit;

namespace QueueLoom.Tests;

// Calls the real editor and workspace adapter. The protocol fake models the documented HTTP null rejection;
// real-broker creation, reload and delivery are covered separately by RabbitMqUiTests in emulator CI.
public sealed class RabbitBindingPersistenceTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData("new", false)]
    [InlineData("unchanged", false)]
    [InlineData("match", false)]
    [InlineData("new", true)]
    [InlineData("unchanged", true)]
    [InlineData("match", true)]
    public async Task Presence_EditorSaveAndReloadPreserveVoidAndDestination(string edit, bool toExchange)
    {
        using var broker = new BindingBoundary(toExchange, edit != "new");
        await using var workspace = broker.Workspace();
        var existing = edit == "new" ? null : Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        var editor = new RuleEditorViewModel("headers", "orders", existing, RoutingService.RabbitMq,
            bindingKind: RuleFilterKind.HeadersBinding) { Headers = "trace = null\nregion = 'EU'" };
        if (edit == "match") editor.HeadersMatch = RuleEditorViewModel.HeaderModes[1];
        var rule = Assert.IsType<SubscriptionRule>(editor.TryBuild()) with { ToExchange = toExchange };
        try { await workspace.SaveSubscriptionRuleAsync("headers", "orders", rule, replace: existing is not null); }
        finally { output.WriteLine(string.Join("\n", broker.Events)); }

        var reloaded = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        Assert.Null(reloaded.Arguments["trace"]);
        Assert.Equal(edit == "match" ? "any" : "all", reloaded.Arguments["x-match"]);
        Assert.Equal(toExchange, reloaded.ToExchange);
        Assert.DoesNotContain(broker.Events, item => item.StartsWith("POST", StringComparison.Ordinal));
        if (edit == "unchanged") Assert.Equal(existing!.Name, reloaded.Name);
        if (edit == "match")
        {
            Assert.NotEqual(existing!.Name, reloaded.Name);
            Assert.True(broker.Events.FindIndex(item => item.StartsWith("BIND", StringComparison.Ordinal)) <
                broker.Events.FindIndex(item => item.StartsWith("DELETE", StringComparison.Ordinal)));
        }
    }

    [Theory]
    [InlineData("bind")]
    [InlineData("lookup")]
    public async Task Presence_FailedCreateOrIdentityLookupNeverDeletesOldBinding(string failure)
    {
        using var broker = new BindingBoundary(false, existing: true) { Failure = failure };
        await using var workspace = broker.Workspace();
        var original = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        var edited = original with { Arguments = new Dictionary<string, object?> { ["x-match"] = "any", ["trace"] = null, ["region"] = "EU" } };
        await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.SaveSubscriptionRuleAsync("headers", "orders", edited, true));
        Assert.DoesNotContain(broker.Events, item => item.StartsWith("DELETE", StringComparison.Ordinal));
        Assert.Contains(broker.Bindings, item => item.Name == original.Name);
        Assert.Contains(broker.Events, item => item.StartsWith("BIND", StringComparison.Ordinal));
    }

    [Fact]
    public async Task LiteralNull_StillUsesHttpAndItsReturnedPropertiesKey()
    {
        using var broker = new BindingBoundary(false, existing: true);
        await using var workspace = broker.Workspace();
        var original = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        var editor = new RuleEditorViewModel("headers", "orders", original, RoutingService.RabbitMq) { Headers = "trace = 'null'" };
        await workspace.SaveSubscriptionRuleAsync("headers", "orders", Assert.IsType<SubscriptionRule>(editor.TryBuild()) with { ToExchange = false }, true);
        Assert.Contains(broker.Events, item => item.StartsWith("POST", StringComparison.Ordinal));
        Assert.DoesNotContain(broker.Events, item => item.StartsWith("BIND", StringComparison.Ordinal));
        Assert.Equal("null", Assert.Single(broker.Bindings).Arguments["trace"]);
    }

    private sealed class BindingBoundary(bool toExchange, bool existing) : HttpMessageHandler
    {
        public List<string> Events { get; } = [];
        public List<SubscriptionRule> Bindings { get; } = existing ? [Rule("old/key", "all")] : [];
        public string? Failure { get; init; }
        private static SubscriptionRule Rule(string name, string mode) => new(name, RuleFilterKind.HeadersBinding)
        {
            Expression = "", Arguments = new Dictionary<string, object?> { ["x-match"] = mode, ["trace"] = null, ["region"] = "EU" }
        };

        public RabbitMqWorkspace Workspace()
        {
            var channel = Proxy<IChannel>((method, args) =>
            {
                if (method.Name is "QueueBindAsync" or "ExchangeBindAsync")
                {
                    Assert.Equal(toExchange ? "ExchangeBindAsync" : "QueueBindAsync", method.Name);
                    Assert.Equal("orders", args![0]);
                    Assert.Equal("headers", args[1]);
                    Assert.Equal("", args[2]);
                    var arguments = Assert.IsType<Dictionary<string, object?>>(args[3]);
                    Assert.Null(arguments["trace"]);
                    Events.Add($"BIND {method.Name} {JsonSerializer.Serialize(arguments)}");
                    if (Failure == "bind") return Task.FromException(new InvalidOperationException("AMQP bind failed"));
                    if (Failure != "lookup") Bindings.Add(Rule("new/key", (string)arguments["x-match"]!));
                }
                return Done(method.ReturnType);
            });
            var connection = Proxy<IConnection>((method, _) => method.Name == "CreateChannelAsync" ? Task.FromResult(channel) : Done(method.ReturnType));
            var workspace = new RabbitMqWorkspace(new EmptyVault());
            Set(workspace, "_connection", connection);
            Set(workspace, "_management", new HttpClient(this) { BaseAddress = new Uri("http://broker.invalid/") });
            // Both kinds deliberately share a name: metadata must choose the correct native binding operation.
            Set(workspace, "_index", new RabbitMqTopologyIndex([new("orders", "classic", 0, 0, null, null, null, "running")],
                [new("headers", "headers"), new("orders", "direct")], []));
            typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
                ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Test, new(AuthenticationKind.RabbitMqPassword),
                    accessMode: ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.RabbitMq, AllowQueueManagement = true });
            return workspace;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            var path = request.RequestUri!.AbsolutePath;
            var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(token);
            Events.Add($"{request.Method} {path} {body}");
            if (request.Method == HttpMethod.Post)
            {
                using var json = JsonDocument.Parse(body);
                if (json.RootElement.GetProperty("arguments").EnumerateObject().Any(item => item.Value.ValueKind == JsonValueKind.Null))
                    return Reply(HttpStatusCode.BadRequest, """{"error":"bad_request","reason":"null_not_allowed"}""");
                var rule = RabbitMqWorkspace.ToRule("headers", JsonSerializer.SerializeToElement(new
                { properties_key = "http/key", routing_key = "", arguments = json.RootElement.GetProperty("arguments") }), 0, 1);
                Bindings.Add(rule);
                var response = Reply(HttpStatusCode.Created, "{}");
                response.Headers.Location = new Uri("http://broker.invalid" + path + "/http%2Fkey");
                return response;
            }
            if (request.Method == HttpMethod.Delete)
            {
                Assert.EndsWith("/old%2Fkey", path, StringComparison.OrdinalIgnoreCase);
                Bindings.RemoveAll(item => item.Name == "old/key");
                return Reply(HttpStatusCode.NoContent, "");
            }
            if (path.EndsWith("/headers", StringComparison.Ordinal)) return Reply(HttpStatusCode.OK, """{"type":"headers"}""");
            return Reply(HttpStatusCode.OK, JsonSerializer.Serialize(Bindings.Select(item => new
            {
                source = "headers", destination = "orders", destination_type = toExchange ? "exchange" : "queue",
                routing_key = item.Expression, properties_key = item.Name, arguments = item.Arguments
            })));
        }
        private static HttpResponseMessage Reply(HttpStatusCode status, string body) => new(status) { Content = new StringContent(body) };
    }

    private static void Set(object target, string name, object value) =>
        target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(target, value);
    private static T Proxy<T>(Func<MethodInfo, object?[]?, object?> call) where T : class
    {
        var proxy = DispatchProxy.Create<T, RabbitLifecycleAuditTests.InterfaceProxy>();
        ((RabbitLifecycleAuditTests.InterfaceProxy)(object)proxy).Call = call;
        return proxy;
    }
    private static object? Done(Type type) => type == typeof(void) ? null : type == typeof(Task) ? Task.CompletedTask :
        type == typeof(ValueTask) ? ValueTask.CompletedTask : type.IsValueType ? Activator.CreateInstance(type) : null;
    private sealed class EmptyVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken token = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
    }
}
