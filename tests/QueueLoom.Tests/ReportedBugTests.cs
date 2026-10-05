using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // Bug 1. Window B opens with no scheduled resends. Window A (same environment, write access on) then has job J.
    // The environment is deleted through B, which never knew J. A must not send J, and J must not stay pending.
    [Fact]
    public async Task AScheduledResendDoesNotRunAfterItsEnvironmentWasDeletedInAnotherWindow()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var profile = CreateProfile("Shared", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        using (var seed = new JsonProfileRepository(paths))
        {
            await seed.UpsertAsync(profile);
            await seed.SetSelectedProfileIdAsync(profile.Id);
        }
        using var profilesA = new JsonProfileRepository(paths);
        using var profilesB = new JsonProfileRepository(paths);
        var storeA = new JsonScheduledResendStore(paths);
        var storeB = new JsonScheduledResendStore(paths);
        var brokerA = new FakeWorkspace();
        var brokerB = new FakeWorkspace();

        await using var windowB = new MainWindowViewModel(profilesB, new FakeSecretVault(), brokerB,
            new FakeDialogService { ConfirmResult = true }, scheduledResends: storeB);
        await windowB.InitializeAsync();
        Assert.Empty(windowB.ScheduledResends);

        var job = DueJob(profile);
        storeA.Add(job);
        await using var windowA = new MainWindowViewModel(profilesA, new FakeSecretVault(), brokerA,
            new FakeDialogService(), scheduledResends: storeA);
        await windowA.InitializeAsync();
        await windowA.ConnectCommand.ExecuteAsync();
        Assert.Single(windowA.ScheduledResends);

        windowB.SelectedProfile = Assert.Single(windowB.Profiles);
        await windowB.DeleteEnvironmentCommand.ExecuteAsync();
        Assert.Empty(await profilesB.ListAsync());

        await windowA.RunDueScheduledResendsAsync();
        await windowA.RunScheduledNowAsync(windowA.ScheduledResends.FirstOrDefault() ?? CreateOrphanItem(windowA, job));

        Assert.Empty(brokerA.SentMessages);
        Assert.Empty(new JsonScheduledResendStore(paths).Load());
    }

    private static ScheduledResendItemViewModel CreateOrphanItem(MainWindowViewModel vm, ScheduledResend job) =>
        new(job, vm.RunScheduledResendCommand, vm.CancelScheduledResendCommand);
}
