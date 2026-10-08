using System.Reflection;
using QueueLoom.App.ViewModels;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // Ordinary Activity entries are saved without waiting for the disk on the window's thread; the record written
    // before a destructive operation starts is still forced to disk. Both are readable afterwards.
    [Fact]
    public async Task OnlyTheRecordBeforeADestructiveOperationWaitsForTheDisk()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        await using var vm = CreateViewModel(new FakeProfileRepository([], null), new FakeWorkspace(), activityJournal: journal);
        var forced = 0;
        FileActivityJournal.ForcedToDisk.Value = () => forced++;
        try
        {
            for (var index = 0; index < 20; index++)
            {
                vm.ReportLocalDataProblem("Entry", $"ordinary {index}");
            }
            Assert.Equal(0, forced);

            typeof(MainWindowViewModel).GetMethod("RecordOperationIntent", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(vm, ["Delete dead letters started", "3 messages", null]);
            Assert.Equal(1, forced);
        }
        finally
        {
            FileActivityJournal.ForcedToDisk.Value = null;
        }

        // Ordinary entries are written by the journal's background writer: read them once it is done.
        await journal.WaitForPendingEntriesAsync().WaitAsync(TimeSpan.FromSeconds(30));
        var records = new FileActivityJournal(directory.Path).ReadRecent();
        Assert.Equal(20, records.Count(record => record.Action == "Entry"));
        Assert.Single(records, record => record.Action == "Delete dead letters started");
    }
}
