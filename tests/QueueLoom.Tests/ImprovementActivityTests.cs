using QueueLoom.Core.Abstractions;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class ImprovementActivityTests
{
    [Fact]
    public void PersistentClearPreservesNewerEntriesAllFilesAndSupportsRestoration()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        var cutoff = DateTimeOffset.UtcNow;
        var old = new ActivityRecord(Guid.NewGuid(), cutoff.AddSeconds(-1), "Info", "old", "", null, null, null);
        var newer = old with { Timestamp = cutoff.AddSeconds(1), Action = "new" };
        journal.Append(old);
        journal.SetClearViewCutoff(cutoff);
        journal.Append(newer);
        var reopened = new FileActivityJournal(directory.Path);
        Assert.Equal(newer, Assert.Single(reopened.ReadRecent()));
        Assert.Equal(2, Directory.GetFiles(directory.Path, "*.json", SearchOption.AllDirectories).Length);
        reopened.SetClearViewCutoff(null);
        Assert.Equal(2, reopened.ReadRecent().Count);
    }
}
