using System.Reflection;
using QueueLoom.App.Services;
using QueueLoom.Core.Monitoring;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ScheduledStoreFailure_PreservesPendingJobAndPreventsSendOrCancelSuccess(bool cancel)
    {
        var (vm, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var lifetime = vm;
        vm.Messages[0].IsMarked = true;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        var item = Assert.Single(vm.ScheduledResends);
        SetPrivate(vm, "_scheduledStore", new ThrowingScheduledStore());
        if (cancel) await vm.CancelScheduledAsync(item);
        else await vm.RunDueScheduledResendsAsync();
        Assert.Empty(workspace.SentMessages);
        Assert.Same(item, Assert.Single(vm.ScheduledResends));
        Assert.DoesNotContain("cancelled; nothing", vm.StatusText, StringComparison.Ordinal);
        Assert.Contains("isolated simulated storage failure", vm.ErrorText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ScheduledSnapshot_CancelledSecondJobDoesNotSendOrMove()
    {
        var (vm, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var lifetime = vm;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddMinutes(-1), Mode = ResendMode.Move };
        vm.Messages[0].IsMarked = true;
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        vm.Messages[0].IsMarked = false;
        vm.Messages[1].IsMarked = true;
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        var cancelled = vm.ScheduledResends[1];
        var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        workspace.SendGate = async () => { started.TrySetResult(true); await release.Task; };
        var run = vm.RunDueScheduledResendsAsync();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await vm.CancelScheduledAsync(cancelled);
        release.SetResult(true);
        await run;
        Assert.Single(workspace.SentMessages);
        Assert.Single(workspace.DeleteRequests);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ScheduledJob_ProfileEditedToDifferentNamespaceDoesNotSend(bool changesNamespace)
    {
        var (vm, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var lifetime = vm;
        vm.Messages[0].IsMarked = true;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        var connected = (ServiceBusProfile)GetPrivate(vm, "_connectedProfile")!;
        dialogs.EditResult = new ProfileEditorResult(changesNamespace ? connected with { FullyQualifiedNamespace = "other.servicebus.windows.net" } : connected, null, false);
        await vm.EditEnvironmentCommand.ExecuteAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.RunDueScheduledResendsAsync();
        Assert.Empty(workspace.SentMessages);
        Assert.Single(vm.ScheduledResends);
    }

    [Fact]
    public async Task LegacyScheduledJob_CannotBypassConfigurationCheckUsingRunNow()
    {
        var (vm, workspace, dialogs) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        await using var lifetime = vm;
        vm.Messages[0].IsMarked = true;
        dialogs.ResendChoice = dialog => dialog.ToOptions() with { SendAt = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await vm.ResendMarkedMessagesCommand.ExecuteAsync();
        var resend = Assert.Single(vm.ScheduledResends).Resend with { ConfigurationIdentity = null };
        vm.ScheduledResends.Clear();
        var legacy = new ScheduledResendItemViewModel(resend, vm.RunScheduledResendCommand, vm.CancelScheduledResendCommand);
        vm.ScheduledResends.Add(legacy);
        await vm.RunScheduledNowAsync(legacy);
        Assert.Empty(workspace.SentMessages);
        Assert.Same(legacy, Assert.Single(vm.ScheduledResends));
        Assert.Contains("legacy schedule", vm.ErrorText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ScheduledStore_MigratesLegacyFileWithoutLeavingOldExecutableCopy()
    {
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        var paths = QueueLoom.Infrastructure.Persistence.QueueLoomPaths.ForRoot(directory.Path);
        var store = new QueueLoom.Infrastructure.Persistence.JsonScheduledResendStore(paths);
        var job = new ScheduledResend(Guid.NewGuid(), Guid.NewGuid(), "legacy", DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow, ResendMode.Copy, 0, "orders", []);
        store.Save([job]);
        var legacy = Path.Combine(paths.RootDirectory, "scheduled-resends.v1.json");
        File.Move(store.FilePath, legacy);
        Assert.Null(Assert.Single(store.Load()).ConfigurationIdentity);
        Assert.False(File.Exists(legacy));
        Assert.True(File.Exists(store.FilePath));
    }

    private static object? GetPrivate(MainWindowViewModel vm, string name) => typeof(MainWindowViewModel).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(vm);
    private static void SetPrivate(MainWindowViewModel vm, string name, object value) => typeof(MainWindowViewModel).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(vm, value);
    private sealed class ThrowingScheduledStore : IScheduledResendStore
    {
        public IReadOnlyList<ScheduledResend> Load() => throw new IOException("isolated simulated storage failure");
        public void Save(IReadOnlyList<ScheduledResend> resends) => throw new IOException("isolated simulated storage failure");
        public void Add(ScheduledResend resend) => Save([resend]);
        public bool TryRemove(ScheduledResend expected) => throw new IOException("isolated simulated storage failure");
    }
}
