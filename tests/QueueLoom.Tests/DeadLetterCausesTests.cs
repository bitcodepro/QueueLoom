using System.Text;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class DeadLetterCausePatternTests
{
    [Theory]
    [InlineData("Order 1042 was not found", "Order {n} was not found")]
    [InlineData("Order ORD-1042 was not found", "Order ORD-{n} was not found")]
    [InlineData("Customer 4f3c2a1b-0000-4000-8000-000000000001 is blocked", "Customer {id} is blocked")]
    [InlineData("Timeout at 2026-10-01T12:00:05Z after 30.5 s", "Timeout at {time} after {n} s")]
    [InlineData("Field 'amount' must be positive, got '-12'", "Field 'amount' must be positive, got '{value}'")]
    [InlineData("Hash 9f86d081884c7d659a2feaa0 does not match", "Hash {id} does not match")]
    [InlineData("Failed\n   at Orders.Handle()\n   at Program.Main()", "Failed")]
    [InlineData("   ", null)]
    public void Descriptions_keep_their_words_and_lose_what_varies(string description, string? expected) =>
        Assert.Equal(expected, DeadLetterCauses.Pattern(description));

    [Fact]
    public void Messages_are_grouped_by_reason_and_pattern()
    {
        var orders = ServiceBusEntityReference.Queue("orders");
        BrowsedMessage Dead(long number, string? reason, string? description) => new(
            orders, ServiceBusSubQueue.DeadLetter, number, Encoding.UTF8.GetBytes("x"), new EditableMessageProperties(MessageId: $"m-{number}"),
            deadLetterReason: reason, deadLetterErrorDescription: description);

        var causes = DeadLetterCauses.Group(
        [
            Dead(1, "ProcessingFailed", "Order 1 was not found"),
            Dead(2, "ProcessingFailed", "Order 2 was not found"),
            Dead(3, "ProcessingFailed", "Payment 7 declined"),
            Dead(4, "MaxDeliveryCountExceeded", null),
            Dead(5, null, null)
        ]);

        Assert.Equal(["ProcessingFailed: Order {n} was not found", "(no reason)", "MaxDeliveryCountExceeded", "ProcessingFailed: Payment {n} declined"],
            causes.Select(cause => cause.Label));
        Assert.Equal(2, causes[0].Count);
        Assert.Equal("Order 1 was not found", causes[0].Example);
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task Causes_split_one_reason_by_its_error_and_tick_the_messages_of_one()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var orders = ServiceBusEntityReference.Queue("orders");
        BrowsedMessage Dead(long number, string description) => new(
            orders, ServiceBusSubQueue.DeadLetter, number, Encoding.UTF8.GetBytes("order"),
            new EditableMessageProperties(MessageId: $"m-{number}"),
            enqueuedAt: DateTimeOffset.Parse("2026-08-12T10:00:00Z").AddMinutes(number),
            deadLetterReason: "ProcessingFailed", deadLetterErrorDescription: description);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 3)))]),
            SearchMatches =
            {
                [profile.Id] = [Dead(1, "Order 17 was not found"), Dead(2, "Payment 3 declined"), Dead(3, "Order 99 was not found")]
            }
        };
        workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow, [new DeadLetterEntitySnapshot(orders, 3)]);
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        viewModel.DeadLetterSearchQuery = "order";
        await viewModel.SearchDeadLettersCommand.ExecuteAsync();

        Assert.Equal(["ProcessingFailed · 3"], viewModel.DeadLetterReasons.Select(reason => reason.Label));
        Assert.True(viewModel.HasDeadLetterCauses);
        Assert.Equal("2 causes", viewModel.DeadLetterCausesSummary);
        Assert.Equal(["Order {n} was not found · 2", "Payment {n} declined · 1"], viewModel.DeadLetterCauses.Select(cause => cause.Label));
        Assert.Contains("For example: Order 17 was not found", viewModel.DeadLetterCauses[0].ToolTip, StringComparison.Ordinal);

        viewModel.SelectDeadLetterCauseCommand.Execute(viewModel.DeadLetterCauses[0]);
        Assert.Equal([1L, 3L], viewModel.Messages.Where(message => message.IsMarked).Select(message => message.SequenceNumber));
        Assert.True(viewModel.DeadLetterCauses[0].IsSelected);
        Assert.False(viewModel.DeadLetterReasons[0].IsSelected);

        viewModel.SelectDeadLetterCauseCommand.Execute(viewModel.DeadLetterCauses[0]);
        Assert.Equal(0, viewModel.MarkedMessageCount);
    }
}

/// <summary>
/// An apostrophe inside a word ("can't", "customer's", "customers'") is not a quote. Read as one, it swallowed every
/// word up to the next apostrophe whenever a number sat in between, so different causes merged into one group and
/// selecting that cause ticked messages that failed for another reason.
/// </summary>
public sealed class DeadLetterCauseApostropheTests
{
    [Theory]
    [InlineData("Customer's order 42 couldn't be found", "Customer's order {n} couldn't be found")]
    [InlineData("Can't ship order 42, it doesn't exist", "Can't ship order {n}, it doesn't exist")]
    [InlineData("The customers' invoice 7 isn't paid", "The customers' invoice {n} isn't paid")]
    [InlineData("It's 5 o'clock and order 9 isn't ready", "It's {n} o'clock and order {n} isn't ready")]
    [InlineData("Customer's field 'zip-90210' isn't valid", "Customer's field '{value}' isn't valid")]
    public void Apostrophes_inside_words_keep_the_words_around_them(string description, string expected) =>
        Assert.Equal(expected, DeadLetterCauses.Pattern(description));

    [Theory]
    [InlineData("Field 'amount' must be positive, got '-12'", "Field 'amount' must be positive, got '{value}'")]
    [InlineData("'42' is not a valid id", "'{value}' is not a valid id")]
    [InlineData("Header id=\"A1\" was rejected", "Header id=\"{value}\" was rejected")]
    [InlineData("Payload {\"code\":\"E42\"} was refused", "Payload {\"code\":\"{value}\"} was refused")]
    [InlineData("Schema `v2` is unknown", "Schema `{value}` is unknown")]
    [InlineData("Value ('x9') is out of range", "Value ('{value}') is out of range")]
    public void Real_quotes_holding_digits_are_still_replaced(string description, string expected) =>
        Assert.Equal(expected, DeadLetterCauses.Pattern(description));

    [Fact]
    public void Different_causes_with_contractions_stay_apart()
    {
        var orders = ServiceBusEntityReference.Queue("orders");
        BrowsedMessage Dead(long number, string description) => new(
            orders, ServiceBusSubQueue.DeadLetter, number, Encoding.UTF8.GetBytes("x"), new EditableMessageProperties(MessageId: $"m-{number}"),
            deadLetterReason: "ProcessingFailed", deadLetterErrorDescription: description);

        var causes = DeadLetterCauses.Group(
        [
            Dead(1, "Can't ship order 42, it doesn't exist"),
            Dead(2, "Can't ship order 43, it doesn't exist"),
            Dead(3, "Can't refund payment 7, it doesn't exist")
        ]);

        Assert.Equal(
            ["ProcessingFailed: Can't ship order {n}, it doesn't exist", "ProcessingFailed: Can't refund payment {n}, it doesn't exist"],
            causes.Select(cause => cause.Label));
        Assert.Equal([2, 1], causes.Select(cause => cause.Count));
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task Selecting_a_cause_with_contractions_ticks_only_its_own_messages()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var orders = ServiceBusEntityReference.Queue("orders");
        BrowsedMessage Dead(long number, string description) => new(
            orders, ServiceBusSubQueue.DeadLetter, number, Encoding.UTF8.GetBytes("order"),
            new EditableMessageProperties(MessageId: $"m-{number}"),
            enqueuedAt: DateTimeOffset.Parse("2026-08-12T10:00:00Z").AddMinutes(number),
            deadLetterReason: "ProcessingFailed", deadLetterErrorDescription: description);
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(deadLetter: 4)))]),
            SearchMatches =
            {
                [profile.Id] =
                [
                    Dead(1, "Customer's order 17 couldn't be found"),
                    Dead(2, "Customer's invoice 3 couldn't be found"),
                    Dead(3, "Customer's order 99 couldn't be found"),
                    Dead(4, "Payment 5 declined")
                ]
            }
        };
        workspace.Snapshots[profile.Id] = new DeadLetterSnapshot(profile.Id, DateTimeOffset.UtcNow, [new DeadLetterEntitySnapshot(orders, 4)]);
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        viewModel.DeadLetterSearchQuery = "e";
        await viewModel.SearchDeadLettersCommand.ExecuteAsync();

        // The invoice that could not be found is another failure: selecting the largest cause (the orders) must not tick
        // it for a resend or delete.
        viewModel.SelectDeadLetterCauseCommand.Execute(viewModel.DeadLetterCauses[0]);
        Assert.Equal([1L, 3L], viewModel.Messages.Where(message => message.IsMarked).Select(message => message.SequenceNumber));
        Assert.Equal("3 causes", viewModel.DeadLetterCausesSummary);
        Assert.Equal(
            ["Customer's order {n} couldn't be found · 2", "Customer's invoice {n} couldn't be found · 1", "Payment {n} declined · 1"],
            viewModel.DeadLetterCauses.Select(cause => cause.Label));
    }
}
