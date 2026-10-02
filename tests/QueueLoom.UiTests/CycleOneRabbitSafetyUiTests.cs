using QueueLoom.App.ViewModels;

namespace QueueLoom.UiTests;

public sealed class CycleOneRabbitSafetyUiTests
{
    [Fact]
    public Task ReviewedRabbitDeadLettersOfferCopyAndPurgeButNoSelectedDeletionOrMove() => UiSession.RunAsync(async () =>
    {
        await using var fixture = await WindowFixture.OpenAsync(allClouds: true);
        var vm = fixture.ViewModel;
        vm.SelectedProfile = vm.Profiles.Single(p => p.Id == DemoData.RabbitStaging.Id);
        await vm.ConnectCommand.ExecuteAsync();
        await vm.ScanCurrentEnvironmentCommand.ExecuteAsync();
        await fixture.NavigateAsync("DeadLetters");
        await fixture.OpenDeadLettersAsync();
        vm.AreAllMessagesMarked = true;
        await fixture.SettleAsync();
        Assert.False(vm.CanDeleteSelectedMessages);
        Assert.False(vm.ShowDeleteMarkedMessages);
        Assert.False(vm.DeleteMarkedMessagesCommand.CanExecute(null));
        Assert.True(vm.ResendMarkedMessagesCommand.CanExecute(null));
        var dialog = new ResendDialogViewModel(vm.Messages.Select(m => m.Message).ToArray(), [], DemoData.RabbitStaging.Name,
            false, canRemoveOriginals: vm.CanDeleteSelectedMessages);
        Assert.False(dialog.CanMove);
        Assert.True(dialog.Copies);
        Assert.DoesNotContain("Kafka", dialog.MoveUnavailableReason, StringComparison.Ordinal);
    });
}
