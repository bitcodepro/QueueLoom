using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Routing;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class RuleEditorFilterTypeTests
{
    [Fact]
    public void A_correlation_property_listed_twice_is_refused_instead_of_keeping_the_last()
    {
        var editor = new RuleEditorViewModel("t", "s")
        {
            Name = "tenants",
            Kind = RuleEditorViewModel.Kinds[1],
            CorrelationProperties = "tenant = 'acme'\ntenant = 'globex'"
        };

        Assert.Null(editor.TryBuild());
        Assert.Equal("Property 'tenant' is duplicated. List each property once.", editor.Error);
    }

    [Fact]
    public void Properties_differing_only_in_case_are_distinct_as_in_Service_Bus()
    {
        var editor = new RuleEditorViewModel("t", "s")
        {
            Name = "tenants",
            Kind = RuleEditorViewModel.Kinds[1],
            CorrelationProperties = "tenant = 'acme'\nTenant = 'globex'"
        };

        Assert.Equal(2, editor.TryBuild()!.Correlation!.Properties.Count);
    }

    public static TheoryData<RuleFilter> CatchAllFilters => new() { new TrueRuleFilter(), new FalseRuleFilter() };

    [Theory]
    [MemberData(nameof(CatchAllFilters))]
    public void Editing_only_the_action_keeps_a_true_or_false_filter(RuleFilter filter)
    {
        var read = AzureServiceBusWorkspace.ToRule(ServiceBusModelFactory.RuleProperties("$Default", filter, null));
        var editor = new RuleEditorViewModel("t", "s", read) { Action = "SET seen = 1" };

        var saved = editor.TryBuild()!;

        Assert.Equal(read.Kind, saved.Kind);
        Assert.IsType(filter.GetType(), AzureServiceBusWorkspace.ToFilter(saved));
        Assert.True(AzureServiceBusWorkspace.SameDefinition(read, saved with { Action = null }));
    }

    [Fact]
    public void Editing_the_expression_of_a_true_filter_makes_a_sql_filter()
    {
        var read = AzureServiceBusWorkspace.ToRule(ServiceBusModelFactory.RuleProperties("$Default", new TrueRuleFilter(), null));
        var editor = new RuleEditorViewModel("t", "s", read) { SqlExpression = "region = 'EU'" };

        var saved = editor.TryBuild()!;

        Assert.Equal(RuleFilterKind.Sql, saved.Kind);
        Assert.Equal("region = 'EU'", Assert.IsType<SqlRuleFilter>(AzureServiceBusWorkspace.ToFilter(saved)).SqlExpression);
    }
}
