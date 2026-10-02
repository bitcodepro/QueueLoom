using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("create")]
    [InlineData("update")]
    [InlineData("delete")]
    public async Task QueueManagement_ExpiryInsideDialogMakesZeroMutations(string operation)
    {
        var profile = CreateProfile("Expiry", EnvironmentKind.Development, ProfileAccessMode.ReadOnly) with { AllowQueueManagement = true };
        var workspace = new FakeWorkspace
        {
            QueueManagement = SqsLike,
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty)])
        };
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.UnlockWritesCommand.ExecuteAsync();
        vm.SelectedEntity = vm.Entities.Single(entity => entity.IsQueue);
        Assert.True(vm.CanWrite);
        dialogs.FillQueueDialog = dialog => { dialog.Name = "created"; ExpireManagementGrant(vm); };
        dialogs.BeforeConfirm = () => ExpireManagementGrant(vm);

        await (operation switch
        {
            "create" => vm.CreateQueueCommand.ExecuteAsync(),
            "update" => vm.EditQueueSettingsCommand.ExecuteAsync(),
            _ => vm.DeleteQueueCommand.ExecuteAsync()
        });

        Assert.False(vm.CanWrite);
        Assert.Equal(ProfileAccessMode.ReadWrite, workspace.ConnectedAccessMode); // The dialog still owns the gate; provider relock is pending.
        Assert.Empty(workspace.CreatedQueues);
        Assert.Empty(workspace.UpdatedQueues);
        Assert.Empty(workspace.DeletedQueues);
        Assert.Contains("write", vm.ErrorText, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("add")]
    [InlineData("replace")]
    [InlineData("delete")]
    public async Task Routing_ExpiryInsideEditorOrConfirmationMakesZeroMutations(string operation)
    {
        var profile = CreateProfile("Expiry", EnvironmentKind.Development, ProfileAccessMode.ReadOnly) with { AllowQueueManagement = true };
        var workspace = RoutingWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.UnlockWritesCommand.ExecuteAsync();
        vm.SelectedEntity = vm.Entities.Single(entity => entity.IsTopic);
        dialogs.RuleEdit = editor => { ExpireManagementGrant(vm); return new SubscriptionRule("expired", RuleFilterKind.Sql, "1=1"); };
        dialogs.BeforeConfirm = () => ExpireManagementGrant(vm);
        dialogs.OnRouting = async routing =>
        {
            Assert.True(routing.CanEdit);
            var item = routing.Subscriptions.Single(subscription => subscription.Name == "billing").Rules.Single();
            routing.Selected = routing.Subscriptions.Single(subscription => subscription.Name == "billing");
            await (operation switch
            {
                "add" => routing.AddRuleCommand.ExecuteAsync(),
                "replace" => routing.EditRuleCommand.ExecuteAsync(item),
                _ => routing.DeleteRuleCommand.ExecuteAsync(item)
            });
            Assert.Empty(workspace.RuleChanges);
            Assert.Contains("write", routing.Error, StringComparison.OrdinalIgnoreCase);
        };
        await vm.OpenTopicRoutingCommand.ExecuteAsync();
        Assert.False(vm.CanWrite);
        Assert.Empty(workspace.RuleChanges);
    }

    private static void ExpireManagementGrant(MainWindowViewModel vm) =>
        SetPrivate(vm, "_writeUnlockExpiresAt", DateTimeOffset.UtcNow.AddMinutes(-1));
}
