using System.Text.Json;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

public sealed class RabbitBindingEditorAuditTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BindingAudit_ExistingPresenceHeaderSurvivesEditing(bool changeMatchMode)
    {
        using var json = JsonDocument.Parse("""{"properties_key":"presence-binding","routing_key":"","arguments":{"x-match":"all","trace":null}}""");
        var existing = RabbitMqWorkspace.ToRule("headers", json.RootElement, 0, 1);
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            new Dictionary<string, object?> { ["trace"] = "present-value" });
        Assert.Null(existing.Arguments["trace"]);
        Assert.Equal(RoutingOutcome.Receives, RabbitBindings.MatchHeaders(existing.Arguments, message).Outcome);
        var editor = new RuleEditorViewModel("by-header", "orders", existing, RoutingService.RabbitMq);
        if (changeMatchMode) editor.HeadersMatch = RuleEditorViewModel.HeaderModes[1];

        var saved = editor.TryBuild();

        Assert.NotNull(saved);
        Assert.Equal(RoutingOutcome.Receives, RabbitBindings.MatchHeaders(saved.Arguments, message).Outcome);
        Assert.Null(saved.Arguments["trace"]);
        Assert.Equal(changeMatchMode ? "any" : "all", saved.Arguments["x-match"]);
        Assert.Equal(existing.Name, saved.Name);
    }

    [Fact]
    public void BindingAudit_UnquotedNullCreatesAPresenceCondition()
    {
        var editor = Headers("trace = null");

        var saved = editor.TryBuild();

        Assert.NotNull(saved);
        Assert.Null(saved.Arguments["trace"]);
    }

    [Theory]
    [InlineData("region = 'EU'\nregion = 'US'")]
    [InlineData("region = 'EU'\n region = 'US'")]
    public void BindingAudit_DuplicateHeadersAreRejectedRatherThanOverwritten(string text)
    {
        var editor = Headers(text);

        Assert.Null(editor.TryBuild());
        Assert.Contains("duplicated", editor.Error, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("region", editor.Error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("x-match = 'any'\nregion = 'EU'")]
    [InlineData("region = 'EU'\nx-match = 250")]
    public void BindingAudit_HeaderTextCannotOverrideTheSelectedMatchMode(string text)
    {
        var editor = Headers(text);
        Assert.Equal("all", editor.HeadersMatch.Value);

        Assert.Null(editor.TryBuild());
        Assert.Contains("x-match", editor.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void BindingAudit_QuotedNullRemainsLiteralText()
    {
        var saved = Headers("trace = 'null'").TryBuild();

        Assert.NotNull(saved);
        Assert.Equal("null", Assert.IsType<string>(saved.Arguments["trace"]));
    }

    [Fact]
    public void BindingAudit_HeaderNamesRemainCaseSensitive()
    {
        var saved = Headers("region = 'EU'\nRegion = 'US'").TryBuild();

        Assert.NotNull(saved);
        Assert.Equal("EU", saved.Arguments["region"]);
        Assert.Equal("US", saved.Arguments["Region"]);
        Assert.Equal(3, saved.Arguments.Count);
    }

    private static RuleEditorViewModel Headers(string text) => new("by-header", "orders",
        service: RoutingService.RabbitMq, bindingKind: RuleFilterKind.HeadersBinding) { Headers = text };
}
