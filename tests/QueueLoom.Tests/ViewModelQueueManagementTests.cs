using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    private static readonly QueueManagementCapabilities SqsLike = new("queue",
        QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount | QueueSettingFlags.LockDuration,
        QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.LockDuration,
        CanCreateDeadLetterQueue: true);

    [Fact]
    public async Task QueueManagement_IsOfferedOnlyWhenTheEnvironmentAllowsItAndWritesAreOn()
    {
        var locked = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var workspace = new FakeWorkspace { QueueManagement = SqsLike, Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, []) };
        await using (var viewModel = CreateViewModel(new FakeProfileRepository([locked], locked.Id), workspace))
        {
            await viewModel.InitializeAsync();
            await viewModel.ConnectCommand.ExecuteAsync();
            Assert.False(viewModel.CanManageQueues);
            Assert.False(viewModel.CreateQueueCommand.CanExecute(null));
        }

        var allowed = locked with { AllowQueueManagement = true, AccessMode = ProfileAccessMode.ReadOnly };
        await using (var viewModel = CreateViewModel(new FakeProfileRepository([allowed], allowed.Id), workspace))
        {
            await viewModel.InitializeAsync();
            await viewModel.ConnectCommand.ExecuteAsync();
            Assert.True(viewModel.CanManageQueues);
            Assert.False(viewModel.CreateQueueCommand.CanExecute(null));
            Assert.StartsWith("Unlock write access", viewModel.ManageQueuesHint, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task QueueManagement_CreatesChangesAndDeletesWithTheNameTypedIn()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { AllowQueueManagement = true };
        var workspace = new FakeWorkspace
        {
            QueueManagement = SqsLike,
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow, []),
            CurrentQueueSettings = new QueueSettings(TimeSpan.FromDays(4), 5, TimeSpan.FromSeconds(30))
        };
        var dialogs = new FakeDialogService
        {
            ConfirmResult = true,
            FillQueueDialog = dialog =>
            {
                if (dialog.IsNew)
                {
                    dialog.Name = "invoices";
                    dialog.TimeToLive = 36;
                    dialog.TimeToLiveUnit = TimeUnit.Hours;
                    dialog.MaxDeliveryCount = 3;
                }
                else
                {
                    dialog.LockSeconds = 120;
                }
            }
        };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();

        await viewModel.CreateQueueCommand.ExecuteAsync();
        var created = Assert.Single(workspace.CreatedQueues);
        Assert.Equal(new QueueDefinition("invoices", new QueueSettings(TimeSpan.FromHours(36), 3), true), created);
        Assert.Equal("invoices", viewModel.SelectedEntity?.Name);

        await viewModel.EditQueueSettingsCommand.ExecuteAsync();
        var edit = dialogs.QueueDialogs[1];
        Assert.Equal((4d, TimeUnit.Days), (edit.TimeToLive, edit.TimeToLiveUnit));
        Assert.False(edit.ShowMaxDeliveryCount);
        Assert.Equal(("invoices", new QueueSettings(TimeSpan.FromDays(4), null, TimeSpan.FromSeconds(120))), Assert.Single(workspace.UpdatedQueues));

        await viewModel.DeleteQueueCommand.ExecuteAsync();
        var confirmation = dialogs.Confirmations.Last();
        Assert.Equal("invoices", confirmation.RequiredText);
        Assert.Contains("cannot be undone", confirmation.Message, StringComparison.Ordinal);
        Assert.Equal(["invoices"], workspace.DeletedQueues);
        Assert.DoesNotContain(viewModel.Entities, entity => entity.Name == "invoices");
    }

    [Theory]
    [InlineData("", "Enter a name.")]
    [InlineData("-orders", "Use letters, digits")]
    [InlineData("orders/eu", "Use letters, digits")]
    public void QueueDialog_ChecksTheName(string name, string error)
    {
        var dialog = new QueueDialogViewModel(SqsLike, "Orders") { Name = name };

        Assert.Null(dialog.TryBuildDefinition());
        Assert.StartsWith(error, dialog.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void QueueDialog_ForKafkaCannotShrinkPartitions()
    {
        var kafka = new QueueManagementCapabilities("topic", QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.Partitions,
            QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.Partitions, CanCreateDeadLetterQueue: true);

        Assert.Equal(3, new QueueDialogViewModel(kafka, "Events").Partitions);
        Assert.Equal("Also create the dead-letter topic shipments.DLT", new QueueDialogViewModel(kafka, "Events") { Name = "shipments" }.DeadLetterQueueLabel);
        var edit = new QueueDialogViewModel(kafka, "Events", "shipments", new QueueSettings(Partitions: 6)) { Partitions = 4 };
        Assert.Null(edit.TryBuildSettings());
        Assert.Contains("cannot remove partitions", edit.Error, StringComparison.Ordinal);
    }
}
