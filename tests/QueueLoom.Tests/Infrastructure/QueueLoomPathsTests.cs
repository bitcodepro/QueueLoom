using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests.Infrastructure;

public sealed class QueueLoomPathsTests
{
    [Fact]
    public void CreateDefault_PlacesBackupsNextToRunningExecutable()
    {
        var paths = QueueLoomPaths.CreateDefault();

        Assert.Equal(
            Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "backups")),
            paths.BackupsDirectory);
        Assert.NotEqual(paths.RootDirectory, paths.BackupsDirectory);
    }
}
