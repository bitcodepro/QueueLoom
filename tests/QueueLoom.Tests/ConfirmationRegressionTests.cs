using System.Text;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

/// <summary>
/// What the operator is told before and after a change must match what is done: the counts and entities in a
/// confirmation, the typed name Production asks for, and the buttons that offer an action.
/// </summary>
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task ResendButton_IsOfferedOnceWritesAreUnlockedAfterTicking()
    {
        var (viewModel, _, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadOnly);
        await using var __ = viewModel;
        viewModel.AreAllMessagesMarked = true;
        Assert.False(viewModel.ResendMarkedMessagesCommand.CanExecute(null));
        var resendChanges = 0;
        var deleteChanges = 0;
        viewModel.ResendMarkedMessagesCommand.CanExecuteChanged += (_, _) => resendChanges++;
        viewModel.DeleteMarkedMessagesCommand.CanExecuteChanged += (_, _) => deleteChanges++;

        await viewModel.UnlockWritesCommand.ExecuteAsync();

        Assert.True(viewModel.CanWrite);
        Assert.True(viewModel.ResendMarkedMessagesCommand.CanExecute(null));
        // The button asks again only when told to; Delete is told, Resend must be too.
        Assert.True(deleteChanges > 0);
        Assert.True(resendChanges > 0, "Resend never raised CanExecuteChanged, so its button stays disabled after unlocking writes.");
    }

    [Fact]
    public async Task Routing_DeletingOneOfSeveralRulesInProductionAsksForTheEnvironmentName()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Production, ProfileAccessMode.ReadWrite) with { AllowQueueManagement = true };
        var workspace = RoutingWorkspace();
        workspace.TopicRules["orders"][0] = new SubscriptionRules("billing",
        [
            new SubscriptionRule("eu", RuleFilterKind.Sql, "region = 'EU'"),
            new SubscriptionRule("us", RuleFilterKind.Sql, "region = 'US'")
        ]);
        var dialogs = new FakeDialogService
        {
            ConfirmResult = true,
            OnRouting = async routing =>
            {
                var billing = routing.Subscriptions.Single(item => item.Name == "billing");
                await routing.DeleteRuleCommand.ExecuteAsync(billing.Rules.First(rule => rule.Name == "eu"));
            }
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.IsTopic);

        await viewModel.OpenTopicRoutingCommand.ExecuteAsync();

        var confirmation = dialogs.Confirmations.Last();
        Assert.StartsWith("Delete rule", confirmation.Title, StringComparison.Ordinal);
        Assert.Equal("Orders", confirmation.RequiredText);
    }

    [Fact]
    public async Task Routing_OneOfSeveralRulesOutsideProductionNeedsNoTypedName()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { AllowQueueManagement = true };
        var workspace = RoutingWorkspace();
        workspace.TopicRules["orders"][0] = new SubscriptionRules("billing",
        [
            new SubscriptionRule("eu", RuleFilterKind.Sql, "region = 'EU'"),
            new SubscriptionRule("us", RuleFilterKind.Sql, "region = 'US'")
        ]);
        var dialogs = new FakeDialogService
        {
            ConfirmResult = true,
            OnRouting = async routing =>
            {
                var billing = routing.Subscriptions.Single(item => item.Name == "billing");
                await routing.DeleteRuleCommand.ExecuteAsync(billing.Rules.First(rule => rule.Name == "eu"));
            }
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.IsTopic);

        await viewModel.OpenTopicRoutingCommand.ExecuteAsync();

        // Outside Production one rule of several is deleted with a click, as before.
        Assert.Null(dialogs.Confirmations.Last().RequiredText);
        Assert.Contains("delete orders/billing/eu", workspace.RuleChanges);
    }

    [Fact]
    public async Task DeleteQueue_DoesNotClaimToDeleteASeparateDeadLetterQueueAndCountsDelayedMessages()
    {
        // SQS, RabbitMQ, Pub/Sub and Kafka keep dead letters in a queue (topic) of their own: deleting the queue leaves it.
        var separateDeadLetterQueue = new QueueManagementCapabilities("queue",
            QueueSettingFlags.MessageTimeToLive, QueueSettingFlags.MessageTimeToLive, CanCreateDeadLetterQueue: true);
        var (viewModel, workspace, dialogs) = await QueueToDeleteAsync(separateDeadLetterQueue,
            new ServiceBusMessageCounts(active: 3, deadLetter: 50, scheduled: 7));
        await using var _ = viewModel;

        await viewModel.DeleteQueueCommand.ExecuteAsync();

        var message = dialogs.Confirmations.Last().Message;
        Assert.Equal(["orders"], workspace.DeletedQueues);
        Assert.DoesNotContain("50 dead-lettered right now", message, StringComparison.Ordinal);
        Assert.Contains("7 scheduled", message, StringComparison.Ordinal);
        Assert.Contains("50 dead letters sit in a separate dead-letter queue, which is not deleted", message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeleteQueue_CountsScheduledAndTransferDeadLettersThatGoWithAnAzureQueue()
    {
        var builtInDeadLetterQueue = new QueueManagementCapabilities("queue",
            QueueSettingFlags.MessageTimeToLive, QueueSettingFlags.MessageTimeToLive, CanCreateDeadLetterQueue: false);
        var (viewModel, _, dialogs) = await QueueToDeleteAsync(builtInDeadLetterQueue,
            new ServiceBusMessageCounts(active: 3, scheduled: 500, transferDeadLetter: 20));
        await using var __ = viewModel;

        await viewModel.DeleteQueueCommand.ExecuteAsync();

        var message = dialogs.Confirmations.Last().Message;
        Assert.Contains("500 scheduled", message, StringComparison.Ordinal);
        Assert.Contains("20 in the transfer dead-letter queue", message, StringComparison.Ordinal);
        Assert.Contains("523", viewModel.Activity.First().Details, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeleteEnvironment_CancelsItsScheduledResendsAndSaysSo()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        viewModel.AreAllMessagesMarked = true;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddHours(1) };
        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();
        Assert.Single(viewModel.ScheduledResends);

        await viewModel.DeleteEnvironmentCommand.ExecuteAsync();

        Assert.Empty(viewModel.Profiles);
        var confirmation = dialogs.Confirmations.Last();
        Assert.Contains("1 scheduled resend", confirmation.Message, StringComparison.Ordinal);
        // Environment IDs are never reused (an import gets a new one), so the resend could never run again.
        Assert.Empty(viewModel.ScheduledResends);
        Assert.Empty(workspace.SentMessages);
    }

    [Fact]
    public async Task DeleteEnvironment_NamesTheEnvironmentsOwnServiceNotAzure()
    {
        var kafka = CreateProfile("Stream", EnvironmentKind.Development) with
        {
            Provider = MessagingProvider.Kafka,
            Kafka = new("broker.invalid:9092")
        };
        var dialogs = new FakeDialogService { ConfirmResult = false };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([kafka], kafka.Id), new FakeWorkspace(), dialogs);
        await viewModel.InitializeAsync();

        await viewModel.DeleteEnvironmentCommand.ExecuteAsync();

        var confirmation = Assert.Single(dialogs.Confirmations);
        Assert.DoesNotContain("Azure", confirmation.Message, StringComparison.Ordinal);
        Assert.Contains("Apache Kafka", confirmation.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ResendDialog_SaysHowManySourcesItDoesNotList()
    {
        var messages = Enumerable.Range(1, 11)
            .Select(index => new BrowsedMessage(ServiceBusEntityReference.Queue($"queue-{index:D2}"), ServiceBusSubQueue.DeadLetter,
                index, Encoding.UTF8.GetBytes("{}"), new EditableMessageProperties(MessageId: $"m-{index}")))
            .ToArray();
        var dialog = new ResendDialogViewModel(messages, [], "Orders", requiresTypedConfirmation: false);

        Assert.Contains("queue-08", dialog.Summary, StringComparison.Ordinal);
        Assert.DoesNotContain("queue-09", dialog.Summary, StringComparison.Ordinal);
        Assert.Contains("… and 3 more sources", dialog.Summary, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeleteMarked_ActivityDoesNotClaimEverythingWasDeletedWhenSomeWereNotFound()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var __ = viewModel;
        workspace.MissingSequenceNumbers.Add(3);
        viewModel.AreAllMessagesMarked = true;

        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        Assert.Contains("2 of 3 deleted", viewModel.StatusText, StringComparison.Ordinal);
        var entry = viewModel.Activity.First(item => item.Details.Contains("2 of 3 deleted", StringComparison.Ordinal));
        Assert.NotEqual("Selected dead letters backed up and deleted", entry.Action);
    }

    [Fact]
    public async Task ResendMarked_IsNotRecordedAsASuccessWhenItWasCancelledPartWay()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var __ = viewModel;
        viewModel.AreAllMessagesMarked = true;
        workspace.OnSend = () => viewModel.CancelCurrentOperationCommand.Execute(null);

        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();

        Assert.Contains("1 of 3 sent", viewModel.StatusText, StringComparison.Ordinal);
        Assert.Contains("not sent (cancelled)", viewModel.StatusText, StringComparison.Ordinal);
        var entry = viewModel.Activity.First(item => item.Details.Contains("1 of 3 sent", StringComparison.Ordinal));
        Assert.NotEqual("Success", entry.Level);
    }

    [Fact]
    public async Task ScheduledMove_SummaryCountsTheOriginalsItCouldNotRemove()
    {
        var (viewModel, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var _ = viewModel;
        viewModel.Messages.Single(message => message.SequenceNumber == 2).IsMarked = true;
        viewModel.Messages.Single(message => message.SequenceNumber == 4).IsMarked = true;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { Mode = ResendMode.Move, SendAt = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await viewModel.ResendMarkedMessagesCommand.ExecuteAsync();
        workspace.MissingSequenceNumbers.Add(4);

        await viewModel.RunDueScheduledResendsAsync();

        Assert.Equal(2, workspace.SentMessages.Count);
        Assert.Contains("1 originals removed", viewModel.StatusText, StringComparison.Ordinal);
        // The immediate resend says so; the scheduled one must too, or a kept original reads as removed.
        Assert.Contains("1 originals kept", viewModel.StatusText, StringComparison.Ordinal);
    }

    private static async Task<(MainWindowViewModel ViewModel, FakeWorkspace Workspace, FakeDialogService Dialogs)> QueueToDeleteAsync(
        QueueManagementCapabilities capabilities, ServiceBusMessageCounts counts)
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { AllowQueueManagement = true };
        var workspace = new FakeWorkspace
        {
            QueueManagement = capabilities,
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(counts), ServiceBusEntityStatus.Active)])
        };
        var dialogs = new FakeDialogService { ConfirmResult = true };
        var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        viewModel.SelectedEntity = viewModel.Entities.Single(entity => entity.Name == "orders");
        return (viewModel, workspace, dialogs);
    }
}
