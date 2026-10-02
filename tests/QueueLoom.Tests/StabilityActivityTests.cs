using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("")]
    [InlineData("2026-10-")]
    [InlineData("not a timestamp")]
    public async Task CorruptActivityDisplayCutoffDoesNotBlockStartupOrHideRetainedJournal(string damaged)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test);
        var journal = new FileActivityJournal(directory.Path);
        var record = new ActivityRecord(Guid.NewGuid(), DateTimeOffset.UtcNow.AddMinutes(-1), "Info", "Retained operation", "evidence", profile.Id, profile.Name, null);
        journal.Append(record);
        var cutoffPath = Path.Combine(directory.Path, ".view-cutoff");
        File.WriteAllText(cutoffPath, damaged);
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(), activityJournal: journal);
        await vm.InitializeAsync();
        Assert.Single(vm.Profiles);
        Assert.Contains(vm.Activity, item => item.Action == record.Action);
        Assert.Equal(damaged, File.ReadAllText(cutoffPath));
        Assert.Equal(record, Assert.Single(journal.ReadRecent()));
        Assert.Single(Directory.GetFiles(directory.Path, "*.json", SearchOption.AllDirectories));
    }
}
