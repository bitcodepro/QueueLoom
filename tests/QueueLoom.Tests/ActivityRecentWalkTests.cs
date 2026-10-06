using QueueLoom.Core.Abstractions;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class ActivityRecentWalkTests
{
    private static ActivityRecord Record(DateTimeOffset at, string action) =>
        new(Guid.NewGuid(), at, "Info", action, "", null, null, null);

    // The newest records are found across day folders, newest first, also when the limit falls inside a day.
    [Fact]
    public void TheNewestRecordsComeFromTheNewestDays()
    {
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        var start = new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);
        for (var day = 0; day < 3; day++)
        {
            for (var index = 0; index < 3; index++)
            {
                journal.Append(Record(start.AddDays(day).AddMinutes(index), $"d{day}-{index}"));
            }
        }

        Assert.Equal(["d2-2", "d2-1", "d2-0", "d1-2"], journal.ReadRecent(4).Select(record => record.Action));
        Assert.Equal(9, journal.ReadRecent().Count);
    }

    // An older day folder that cannot be read (another account's restore, a damaged disk) is not needed for the
    // newest records, and must not hide them: before, listing it failed the whole Activity history.
    [Fact]
    public void AnUnreadableOlderDayDoesNotHideTheNewestRecords()
    {
        if (OperatingSystem.IsWindows() || Environment.UserName == "root") return; // relies on POSIX permissions
        using var directory = new TemporaryDirectory();
        var journal = new FileActivityJournal(directory.Path);
        var start = new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);
        journal.Append(Record(start, "old"));
        journal.Append(Record(start.AddDays(1), "new-1"));
        journal.Append(Record(start.AddDays(1).AddMinutes(1), "new-2"));
        var oldDay = Path.Combine(directory.Path, "2026-10-01");
        File.SetUnixFileMode(oldDay, UnixFileMode.None);
        try
        {
            Assert.Equal(["new-2", "new-1"], journal.ReadRecent(2).Select(record => record.Action));
        }
        finally
        {
            File.SetUnixFileMode(oldDay, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
    }
}
