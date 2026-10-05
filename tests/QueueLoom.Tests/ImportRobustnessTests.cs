using QueueLoom.Core.Profiles;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // Saving the second of three imported environments fails: the first one stays saved, is listed right away, and the
    // error says how far the import got instead of reading as if nothing was imported.
    [Fact]
    public async Task AnImportThatStopsPartWayListsWhatWasSavedAndSaysSo()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(file, EnvironmentTransfer.Export(
        [
            CreateProfile("Alpha", EnvironmentKind.Development),
            CreateProfile("Beta", EnvironmentKind.Test),
            CreateProfile("Gamma", EnvironmentKind.Test)
        ]));
        var repository = new FakeProfileRepository([], null);
        var dialogs = new FakeDialogService { OpenFilePath = file };
        await using var vm = CreateViewModel(repository, new FakeWorkspace(), dialogs);
        await vm.InitializeAsync();
        repository.FailUpsertsAfter = 1;

        await vm.ImportEnvironmentsCommand.ExecuteAsync();

        Assert.Single(await repository.ListAsync());
        Assert.Single(vm.Profiles);
        Assert.Contains("Imported 1 environment(s)", vm.ErrorText, StringComparison.Ordinal);
        Assert.Contains("could not be written", vm.ErrorText, StringComparison.Ordinal);
    }

    // Windows: the export's replacement fails half-way and the original cannot be put back either. The error the
    // operator sees (and the activity entry) says where both versions are, rather than only the innermost cause.
    [Fact]
    public async Task AFailedExportReplacementTellsWhereBothVersionsAre()
    {
        if (!OperatingSystem.IsWindows()) return;
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(file, "previous export");
        var repository = new FakeProfileRepository([CreateProfile("Alpha", EnvironmentKind.Development)], null);
        var dialogs = new FakeDialogService { SaveFilePath = file };
        await using var vm = CreateViewModel(repository, new FakeWorkspace(), dialogs);
        await vm.InitializeAsync();
        QueueLoom.App.Services.SafeFileWriter.ReplaceOverride.Value = (_, destination, backup) =>
        {
            File.Move(destination, backup);
            Directory.CreateDirectory(destination);
            throw new IOException("Unable to move the replacement file to the file to be replaced.", unchecked((int)0x80070498));
        };
        try
        {
            await vm.ExportEnvironmentsCommand.ExecuteAsync();
        }
        finally
        {
            QueueLoom.App.Services.SafeFileWriter.ReplaceOverride.Value = null;
        }

        Assert.Contains("could not be put back", vm.ErrorText, StringComparison.Ordinal);
        Assert.Contains(".previous", vm.ErrorText, StringComparison.Ordinal);
        Assert.Contains("the new version is at", vm.ErrorText, StringComparison.Ordinal);
    }

    // The same storage problem also stops the list from being refreshed: the summary still says one environment was
    // imported, with the original cause, not the refresh error.
    [Fact]
    public async Task APartialImportIsReportedEvenWhenTheListCannotBeRefreshed()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(file, EnvironmentTransfer.Export(
        [
            CreateProfile("Alpha", EnvironmentKind.Development),
            CreateProfile("Beta", EnvironmentKind.Test),
            CreateProfile("Gamma", EnvironmentKind.Test)
        ]));
        var repository = new FakeProfileRepository([], null);
        var dialogs = new FakeDialogService { OpenFilePath = file };
        await using var vm = CreateViewModel(repository, new FakeWorkspace(), dialogs);
        await vm.InitializeAsync();
        repository.FailUpsertsAfter = 1;
        var listings = 0;
        repository.ListGate = _ => ++listings > 1
            ? Task.FromException(new IOException("The profiles folder is unavailable."))
            : Task.CompletedTask;

        await vm.ImportEnvironmentsCommand.ExecuteAsync();

        repository.ListGate = null;
        Assert.Single(await repository.ListAsync());
        Assert.Contains("Imported 1 environment(s)", vm.ErrorText, StringComparison.Ordinal);
        Assert.Contains("could not be written", vm.ErrorText, StringComparison.Ordinal);
        Assert.DoesNotContain("folder is unavailable", vm.ErrorText, StringComparison.Ordinal);
    }
}
