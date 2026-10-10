using System.Reflection;
using QueueLoom.App.Services;
using QueueLoom.Core.Monitoring;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task PurgeCompleted_PreservesOtherEnvironmentRowAndPreviousCount()
    {
        var a = CreateProfile("A", EnvironmentKind.Development);
        var b = CreateProfile("B", EnvironmentKind.Development);
        var source = ServiceBusEntityReference.Queue("orders");
        var workspace = new FakeWorkspace();
        workspace.Snapshots[a.Id] = Snapshot(a.Id, new DeadLetterEntitySnapshot(source, 3));
        workspace.Snapshots[b.Id] = Snapshot(b.Id, new DeadLetterEntitySnapshot(source, 7));
        await using var vm = CreateViewModel(new FakeProfileRepository([a,b], a.Id), workspace);
        await vm.InitializeAsync();
        await vm.ScanAllEnvironmentsCommand.ExecuteAsync();
        var bRow = vm.DeadLetterSources.Single(row => row.ProfileId == b.Id);
        var counts = (Dictionary<string, DeadLetterMeasurement>)GetPurgePrivate(vm, "_previousDlqCounts")!;
        var key = $"{b.Id:N}|{source.Path}|{ServiceBusSubQueue.DeadLetter}";
        counts[key] = new DeadLetterMeasurement(7, false);
        var now = DateTimeOffset.UtcNow;
        typeof(MainWindowViewModel).GetMethod("ApplyCompletedPurgeToDeadLetterRows", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(vm, [new DeadLetterPurgeResult(a.Id, now, now, [new DeadLetterPurgeSourceResult(source, ServiceBusSubQueue.DeadLetter, 3)], Path.GetTempPath())]);
        Assert.Same(bRow, Assert.Single(vm.DeadLetterSources));
        Assert.Equal(new DeadLetterMeasurement(7, false), counts[key]);
    }

    private static object? GetPurgePrivate(MainWindowViewModel vm, string name) => typeof(MainWindowViewModel).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(vm);
}
