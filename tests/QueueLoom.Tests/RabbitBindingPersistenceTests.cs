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

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, true, false)]
    [InlineData(false, false, true)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, true, true)]
    public async Task Presence_NumberTypeEditPersistsAndSelectsItsOwnBinding(bool startsWithDouble, bool decoy, bool signedZero)
    {
        using var broker = new BindingBoundary(false, existing: true);
        var originalArguments = new Dictionary<string, object?>
        { ["x-match"] = "all", ["trace"] = null, ["amount"] = EditNumber(startsWithDouble, signedZero) };
        broker.Bindings[0] = broker.Bindings[0] with { Arguments = originalArguments };
        if (decoy)
            broker.Bindings.Add(new("decoy/key", RuleFilterKind.HeadersBinding)
            { Expression = "", Arguments = new Dictionary<string, object?>(originalArguments) { ["x-match"] = "any" } });
        await using var workspace = broker.Workspace();
        var existing = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules, item => item.Name == "old/key");
        var editor = new RuleEditorViewModel("headers", "orders", existing, RoutingService.RabbitMq)
        { Headers = "trace = null\namount = " + RoutingValue.Format(EditNumber(!startsWithDouble, signedZero)) };
        if (decoy) editor.HeadersMatch = RuleEditorViewModel.HeaderModes[1];
        var rule = Assert.IsType<SubscriptionRule>(editor.TryBuild()) with { ToExchange = false };
        AssertNumber(EditNumber(!startsWithDouble, signedZero), rule.Arguments["amount"]);
        try { await workspace.SaveSubscriptionRuleAsync("headers", "orders", rule, true); }
        finally { output.WriteLine(string.Join("\n", broker.Events)); }

        var rules = Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules;
        var saved = Assert.Single(rules, item => item.Name != "decoy/key");
        Assert.Equal("new/key", saved.Name);
        AssertNumber(EditNumber(!startsWithDouble, signedZero), saved.Arguments["amount"]);
        Assert.Null(saved.Arguments["trace"]);
        Assert.Contains(broker.Events, item => item.StartsWith("BIND", StringComparison.Ordinal));
        Assert.DoesNotContain(rules, item => item.Name == existing.Name);
        if (decoy)
        {
            var retained = Assert.Single(rules, item => item.Name == "decoy/key");
            AssertNumber(EditNumber(startsWithDouble, signedZero), retained.Arguments["amount"]);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Presence_UnchangedNumericTypeKeepsIdentityWithoutMutation(bool floating)
    {
        using var broker = new BindingBoundary(false, existing: true);
        broker.Bindings[0] = broker.Bindings[0] with
        { Arguments = new Dictionary<string, object?> { ["x-match"] = "all", ["trace"] = null, ["amount"] = Number(floating) } };
        await using var workspace = broker.Workspace();
        var original = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        var editor = new RuleEditorViewModel("headers", "orders", original, RoutingService.RabbitMq);
        await workspace.SaveSubscriptionRuleAsync("headers", "orders", Assert.IsType<SubscriptionRule>(editor.TryBuild()) with { ToExchange = false }, true);
        Assert.Equal("old/key", Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules).Name);
        Assert.DoesNotContain(broker.Events, item => item.StartsWith("BIND", StringComparison.Ordinal) ||
            item.StartsWith("POST", StringComparison.Ordinal) || item.StartsWith("DELETE", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NewBinding_WholeValuedDoubleKeepsFloatingTypeAcrossTransport(bool presence)
    {
        using var broker = new BindingBoundary(false, existing: false);
        await using var workspace = broker.Workspace();
        var editor = new RuleEditorViewModel("headers", "orders", service: RoutingService.RabbitMq, bindingKind: RuleFilterKind.HeadersBinding)
        { Headers = (presence ? "trace = null\n" : "") + "amount = 250.0" };
        await workspace.SaveSubscriptionRuleAsync("headers", "orders", Assert.IsType<SubscriptionRule>(editor.TryBuild()) with { ToExchange = false }, false);
        var reloaded = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        Assert.Equal(250d, Assert.IsType<double>(reloaded.Arguments["amount"]));
        Assert.Contains(broker.Events, item => item.StartsWith(presence ? "BIND" : "POST", StringComparison.Ordinal));
    }

    // Keep the boxing explicit: a conditional expression mixing long/double would itself promote to double.
    private static object Number(bool floating) => floating ? (object)250d : 250L;
    private static object EditNumber(bool variant, bool signedZero) => signedZero
        ? variant ? BitConverter.Int64BitsToDouble(long.MinValue) : 0d : Number(variant);
    private static void AssertNumber(object expected, object? actual)
    {
        Assert.Equal(expected.GetType(), actual!.GetType());
        if (expected is double floating)
            Assert.Equal(BitConverter.DoubleToInt64Bits(floating), BitConverter.DoubleToInt64Bits((double)actual));
        else Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Presence_UnchangedSignedZeroKeepsIdentityAndSign(bool negative)
    {
        using var broker = new BindingBoundary(false, existing: true);
        broker.Bindings[0] = broker.Bindings[0] with
        { Arguments = new Dictionary<string, object?> { ["x-match"] = "all", ["trace"] = null, ["amount"] = EditNumber(negative, true) } };
        await using var workspace = broker.Workspace();
        var original = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        var editor = new RuleEditorViewModel("headers", "orders", original, RoutingService.RabbitMq);
        await workspace.SaveSubscriptionRuleAsync("headers", "orders", Assert.IsType<SubscriptionRule>(editor.TryBuild()) with { ToExchange = false }, true);
        var saved = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        Assert.Equal("old/key", saved.Name);
        AssertNumber(EditNumber(negative, true), saved.Arguments["amount"]);
        Assert.DoesNotContain(broker.Events, item => item.StartsWith("BIND", StringComparison.Ordinal) || item.StartsWith("DELETE", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task Presence_CollidingZeroKeyUnbindsExactOldTerms(bool negative, bool toExchange)
    {
        using var broker = new BindingBoundary(toExchange, true) { CollidingKey = true };
        broker.Bindings[0] = broker.Bindings[0] with
        { Arguments = new Dictionary<string, object?> { ["x-match"] = "all", ["trace"] = null, ["amount"] = EditNumber(negative, true) } };
        await using var workspace = broker.Workspace();
        var original = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        var editor = new RuleEditorViewModel("headers", "orders", original, RoutingService.RabbitMq)
        { Headers = "trace = null\namount = " + RoutingValue.Format(EditNumber(!negative, true)) };
        await workspace.SaveSubscriptionRuleAsync("headers", "orders", Assert.IsType<SubscriptionRule>(editor.TryBuild()) with { ToExchange = toExchange }, true);
        var saved = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        Assert.Equal(original.Name, saved.Name);
        AssertNumber(EditNumber(!negative, true), saved.Arguments["amount"]);
        Assert.Contains(broker.Events, item => item.StartsWith("UNBIND", StringComparison.Ordinal));
        Assert.DoesNotContain(broker.Events, item => item.StartsWith("DELETE", StringComparison.Ordinal));
        Assert.True(broker.Events.FindIndex(item => item.StartsWith("BIND", StringComparison.Ordinal)) <
            broker.Events.FindIndex(item => item.StartsWith("UNBIND", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task Presence_AmbiguousOldKeyIsRejectedBeforeMutation()
    {
        using var broker = new BindingBoundary(false, true);
        broker.Bindings.Add(broker.Bindings[0] with { Arguments = new Dictionary<string, object?>
            { ["x-match"] = "all", ["trace"] = null, ["amount"] = 0d } });
        await using var workspace = broker.Workspace();
        var edited = broker.Bindings[0] with { ToExchange = false, Arguments = new Dictionary<string, object?>
            { ["x-match"] = "any", ["trace"] = null } };
        await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.SaveSubscriptionRuleAsync("headers", "orders", edited, true));
        Assert.DoesNotContain(broker.Events, item => item.StartsWith("BIND", StringComparison.Ordinal) ||
            item.StartsWith("UNBIND", StringComparison.Ordinal) || item.StartsWith("DELETE", StringComparison.Ordinal));
        Assert.Equal(2, broker.Bindings.Count);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task Http_CollidingZeroKeyUnbindsExactOldTerms(bool negative, bool toExchange)
    {
        using var broker = new BindingBoundary(toExchange, true) { CollidingKey = true };
        broker.Bindings[0] = broker.Bindings[0] with
        { Arguments = new Dictionary<string, object?> { ["x-match"] = "all", ["amount"] = EditNumber(negative, true) } };
        await using var workspace = broker.Workspace();
        var original = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        var editor = new RuleEditorViewModel("headers", "orders", original, RoutingService.RabbitMq)
        { Headers = "amount = " + RoutingValue.Format(EditNumber(!negative, true)) };
        await workspace.SaveSubscriptionRuleAsync("headers", "orders", Assert.IsType<SubscriptionRule>(editor.TryBuild()) with { ToExchange = toExchange }, true);
        var saved = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        Assert.Equal(original.Name, saved.Name);
        AssertNumber(EditNumber(!negative, true), saved.Arguments["amount"]);
        Assert.Contains(broker.Events, item => item.StartsWith("POST", StringComparison.Ordinal));
        Assert.Contains(broker.Events, item => item.StartsWith("UNBIND", StringComparison.Ordinal));
        Assert.DoesNotContain(broker.Events, item => item.StartsWith("DELETE", StringComparison.Ordinal));
        Assert.True(broker.Events.FindIndex(item => item.StartsWith("POST", StringComparison.Ordinal)) <
            broker.Events.FindIndex(item => item.StartsWith("UNBIND", StringComparison.Ordinal)));
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, true, false)]
    [InlineData(false, false, true)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, true, true)]
    public async Task Delete_CollidingZeroKeyRejectsBothOverloadsAndPreservesBothTerms(bool typed, bool toExchange, bool negative)
    {
        using var broker = new BindingBoundary(toExchange, true) { SharedDestination = false };
        broker.Bindings[0] = broker.Bindings[0] with
        { Arguments = new Dictionary<string, object?> { ["x-match"] = "all", ["amount"] = EditNumber(!negative, true) } };
        broker.Bindings.Add(broker.Bindings[0] with
        { Arguments = new Dictionary<string, object?> { ["x-match"] = "all", ["amount"] = EditNumber(negative, true) } });
        await using var workspace = broker.Workspace();
        var selected = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules,
            rule => BitConverter.DoubleToInt64Bits((double)rule.Arguments["amount"]!) == (negative ? long.MinValue : 0L));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => typed
            ? workspace.DeleteSubscriptionRuleAsync("headers", "orders", selected)
            : workspace.DeleteSubscriptionRuleAsync("headers", "orders", selected.Name));
        Assert.Contains("ambiguous", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(broker.Events, item => item.StartsWith("DELETE", StringComparison.Ordinal) ||
            item.StartsWith("UNBIND", StringComparison.Ordinal));
        var retained = Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules;
        Assert.Equal(2, retained.Count);
        Assert.Contains(retained, rule => BitConverter.DoubleToInt64Bits((double)rule.Arguments["amount"]!) == 0L);
        Assert.Contains(retained, rule => BitConverter.DoubleToInt64Bits((double)rule.Arguments["amount"]!) == long.MinValue);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task Delete_UniqueKeyStillDeletesThroughBothOverloads(bool typed, bool toExchange)
    {
        using var broker = new BindingBoundary(toExchange, true) { SharedDestination = false };
        await using var workspace = broker.Workspace();
        var selected = Assert.Single(Assert.Single(await workspace.GetTopicRulesAsync("headers")).Rules);
        if (typed) await workspace.DeleteSubscriptionRuleAsync("headers", "orders", selected);
        else await workspace.DeleteSubscriptionRuleAsync("headers", "orders", selected.Name);
        Assert.Empty(broker.Bindings);
        Assert.Single(broker.Events, item => item.StartsWith("DELETE", StringComparison.Ordinal));
        Assert.DoesNotContain(broker.Events, item => item.StartsWith("UNBIND", StringComparison.Ordinal));
    }

    private sealed class BindingBoundary(bool toExchange, bool existing) : HttpMessageHandler
    {
        public List<string> Events { get; } = [];
        public List<SubscriptionRule> Bindings { get; } = existing ? [Rule("old/key", "all")] : [];
        public string? Failure { get; init; }
        public bool CollidingKey { get; init; }
        public bool SharedDestination { get; init; } = true;
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
                    if (Failure != "lookup") Bindings.Add(new(CollidingKey ? "old/key" : "new/key", RuleFilterKind.HeadersBinding)
                    { Expression = (string)args[2]!, Arguments = new Dictionary<string, object?>(arguments) });
                }
                if (method.Name is "QueueUnbindAsync" or "ExchangeUnbindAsync")
                {
                    Assert.Equal(toExchange ? "ExchangeUnbindAsync" : "QueueUnbindAsync", method.Name);
                    var arguments = Assert.IsType<Dictionary<string, object?>>(args![3]);
                    Events.Add($"UNBIND {method.Name}");
                    var old = Assert.Single(Bindings, binding =>
                        RabbitMqBindingJson.Equal(JsonSerializer.SerializeToElement(binding.Arguments, RabbitMqBindingJson.Options),
                            JsonSerializer.SerializeToElement(arguments, RabbitMqBindingJson.Options)));
                    Bindings.Remove(old);
                }
                return Done(method.ReturnType);
            });
            var connection = Proxy<IConnection>((method, _) => method.Name == "CreateChannelAsync" ? Task.FromResult(channel) : Done(method.ReturnType));
            var workspace = new RabbitMqWorkspace(new EmptyVault(), (factory, _) =>
            {
                Assert.False(factory.AutomaticRecoveryEnabled);
                Assert.False(factory.TopologyRecoveryEnabled);
                Assert.Equal("broker.invalid", factory.HostName);
                return Task.FromResult(connection);
            });
            Set(workspace, "_connection", Proxy<IConnection>((method, _) =>
            {
                Assert.NotEqual("CreateChannelAsync", method.Name); // binding mutations cannot record topology here
                return Done(method.ReturnType);
            }));
            Set(workspace, "_management", new HttpClient(this) { BaseAddress = new Uri("http://broker.invalid/") });
            // Both kinds deliberately share a name: metadata must choose the correct native binding operation.
            Set(workspace, "_index", new RabbitMqTopologyIndex(
                SharedDestination || !toExchange ? [new("orders", "classic", 0, 0, null, null, null, "running")] : [],
                SharedDestination || toExchange ? [new("headers", "headers"), new("orders", "direct")] : [new("headers", "headers")], []));
            typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
                ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Test, new(AuthenticationKind.RabbitMqPassword),
                    accessMode: ProfileAccessMode.ReadWrite) with
                { Provider = MessagingProvider.RabbitMq, AllowQueueManagement = true, RabbitMq = new("broker.invalid", "guest") });
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
                { properties_key = CollidingKey ? "old/key" : "http/key", routing_key = "", arguments = json.RootElement.GetProperty("arguments") }), 0, 1);
                Bindings.Add(rule);
                var response = Reply(HttpStatusCode.Created, "{}");
                response.Headers.Location = new Uri("http://broker.invalid" + path + (CollidingKey ? "/old%2Fkey" : "/http%2Fkey"));
                return response;
            }
            if (request.Method == HttpMethod.Delete)
            {
                Assert.EndsWith("/old%2Fkey", path, StringComparison.OrdinalIgnoreCase);
                // Management HTTP deletion chooses the first hash match, even when distinct terms collide.
                var first = Bindings.FirstOrDefault(item => item.Name == "old/key");
                if (first is not null) Bindings.Remove(first);
                return Reply(HttpStatusCode.NoContent, "");
            }
            if (path.EndsWith("/headers", StringComparison.Ordinal)) return Reply(HttpStatusCode.OK, """{"type":"headers"}""");
            if (request.Headers.Accept.Any(value => value.MediaType == "application/bert"))
            {
                var terms = Bindings.Select(item => (object?)new Dictionary<string, object?>
                {
                    ["source"] = "headers", ["destination"] = "orders", ["destination_type"] = toExchange ? "exchange" : "queue",
                    ["routing_key"] = item.Expression, ["properties_key"] = item.Name, ["arguments"] = item.Arguments.ToDictionary()
                }).ToArray();
                var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(RabbitBindingEtfFixture.Encode(terms)) };
                response.Content.Headers.ContentType = new("application/bert");
                return response;
            }
            return Reply(HttpStatusCode.OK, JsonSerializer.Serialize(Bindings.Select(item => new
            {
                source = "headers", destination = "orders", destination_type = toExchange ? "exchange" : "queue",
                routing_key = item.Expression, properties_key = item.Name,
                // Broker JSON preserves the floating number token (Erlang float 250.0); the default .NET
                // serializer would erase that distinction in this fake and hide the provider regression.
                arguments = item.Arguments.ToDictionary(pair => pair.Key, pair => BrokerJsonValue(pair.Value))
            })));
        }
        private static object? BrokerJsonValue(object? value) => value is double number
            ? JsonDocument.Parse(RoutingValue.Format(number)).RootElement.Clone() : value;
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
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>("fixture");
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
    }
}
