using System.Globalization;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests.Infrastructure;

public sealed class FileActivityJournalTests
{
    [Fact]
    public void ReopenPreservesOperationAndSubscriptionIdentity()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        var record = new ActivityRecord(Guid.NewGuid(), DateTimeOffset.UtcNow, "Warning", "Purge started", "limit 100",
            Guid.NewGuid(), "Test", ServiceBusEntityReference.Subscription("orders", "billing"));
        journal.Append(record);
        Assert.Equal(record, Assert.Single(new FileActivityJournal(directory.Path).ReadRecent()));
    }

    [Fact]
    public void PartialFileIsIgnoredAndRecentHistoryIsBounded()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        for (var i = 0; i < 3; i++) journal.Append(new ActivityRecord(Guid.NewGuid(), DateTimeOffset.UtcNow.AddSeconds(i), "Info", "Test", "", null, null, null));
        File.WriteAllText(Path.Combine(directory.Path, "unfinished.tmp"), "{");
        Assert.Equal(2, journal.ReadRecent(2).Count);
    }

    [Fact]
    public void DayFolderUsesTheGregorianCalendarWhateverTheCurrentCulture()
    {
        using var directory = new TemporaryDirectory();
        var previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo("th-TH");
        try
        {
            new FileActivityJournal(directory.Path).Append(new ActivityRecord(
                Guid.NewGuid(), new DateTimeOffset(2026, 9, 26, 12, 0, 0, TimeSpan.Zero), "Info", "Test", "", null, null, null));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }

        Assert.True(Directory.Exists(Path.Combine(directory.Path, "2026-09-26")));
    }
}
