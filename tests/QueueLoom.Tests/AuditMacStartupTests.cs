using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task Review_InvalidBundleOverrideFallsBackAndWarnsWithoutStoppingStartup()
    {
        using var directory = new TemporaryDirectory();
        var bundle = Path.Combine(directory.Path, "Applications", "QueueLoom.app");
        var executable = Path.Combine(bundle, "Contents", "MacOS");
        Directory.CreateDirectory(executable);
        var paths = QueueLoomPaths.ForProgram(QueueLoomPaths.ForRoot(Path.Combine(directory.Path, "data")), executable, bundle);
        Assert.Equal(Path.Combine(directory.Path, "Applications", "backups"), paths.BackupsDirectory);
        Assert.NotNull(paths.BackupDirectoryWarning);
        await using var vm = new MainWindowViewModel(new FakeProfileRepository([], null), new FakeSecretVault(), new FakeWorkspace(),
            new FakeDialogService(), backupMigration: new LegacyBackupMigration(paths, executable, bundle));
        await vm.InitializeAsync();
        await vm.InitializeAsync();
        Assert.Single(vm.Activity, item => item.Action == "Backup directory changed" && item.Level == "Warning");
        Assert.False(vm.HasError);
    }

    [Fact]
    public async Task Review_StartupMigrationFailureKeepsSourcesAndCanBeRetried()
    {
        using var directory = new TemporaryDirectory();
        var executable = Path.Combine(directory.Path, "Applications", "QueueLoom.app", "Contents", "MacOS");
        var legacy = Path.Combine(executable, "backups");
        Directory.CreateDirectory(legacy);
        File.WriteAllText(Path.Combine(legacy, "sentinel.json"), "original backup");
        var destination = Path.Combine(directory.Path, "backups");
        File.WriteAllText(destination, "blocked destination");
        var paths = QueueLoomPaths.ForRoot(Path.Combine(directory.Path, "data")) with { BackupsDirectory = destination };
        await using var vm = new MainWindowViewModel(new FakeProfileRepository([], null), new FakeSecretVault(), new FakeWorkspace(),
            new FakeDialogService(), backupMigration: new LegacyBackupMigration(paths, executable));
        await vm.InitializeAsync();
        Assert.Single(vm.Activity, item => item.Action == "Legacy backups not migrated");
        Assert.Equal("original backup", File.ReadAllText(Path.Combine(legacy, "sentinel.json")));
        File.Move(destination, destination + ".kept");
        await vm.InitializeAsync();
        Assert.Equal("original backup", File.ReadAllText(Path.Combine(destination, "sentinel.json")));
        Assert.Equal("original backup", File.ReadAllText(Path.Combine(legacy, "sentinel.json")));
        Assert.Single(vm.Activity, item => item.Action == "Legacy backups preserved");
    }

    [Fact]
    public async Task Review_StartupMakesExistingBundleBackupRestorableAndReportsOnlyNewCopies()
    {
        using var directory = new TemporaryDirectory();
        var bundle = Path.Combine(directory.Path, "Applications", "QueueLoom.app");
        var executableDirectory = Path.Combine(bundle, "Contents", "MacOS");
        var legacy = Path.Combine(executableDirectory, "backups");
        var paths = QueueLoomPaths.ForRoot(Path.Combine(directory.Path, "data")) with
        { BackupsDirectory = Path.Combine(directory.Path, "Applications", "backups") };
        var profile = CreateProfile("Legacy backup", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var session = await new DeadLetterJsonBackupStore(paths with { BackupsDirectory = legacy })
            .CreateSessionAsync(profile, DateTimeOffset.UtcNow, CancellationToken.None);
        await session.BackupAsync(new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter,
            1, "preserved message"u8.ToArray(), new EditableMessageProperties(MessageId: "sentinel")), CancellationToken.None);
        var repository = new JsonDeadLetterBackupRepository(paths);
        await using var vm = new MainWindowViewModel(new FakeProfileRepository([], null), new FakeSecretVault(), new FakeWorkspace(),
            new FakeDialogService(), backupRepository: repository, backupMigration: new LegacyBackupMigration(paths, executableDirectory));

        await vm.InitializeAsync();

        var backup = Assert.Single(await repository.ListAsync());
        var restored = await repository.LoadAsync(backup);
        Assert.Equal("preserved message", System.Text.Encoding.UTF8.GetString(restored.Body.Span));
        Assert.Equal("sentinel", restored.Properties.MessageId);
        Assert.Single(vm.Activity, item => item.Action == "Legacy backups preserved");
        Assert.NotEmpty(Directory.GetFiles(legacy, "*.json", SearchOption.AllDirectories));
        await vm.InitializeAsync();
        Assert.Single(vm.Activity, item => item.Action == "Legacy backups preserved");
    }
}
