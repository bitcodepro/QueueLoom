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
    // operator sees and the activity entry give both retained files' complete paths, even for a 200-character export
    // path, although the visible summary is cut at 600 characters.
    [Fact]
    public async Task AFailedExportReplacementTellsWhereBothVersionsAre()
    {
        if (!OperatingSystem.IsWindows()) return;
        using var directory = new TemporaryDirectory();
        var name = new string('e', 200 - directory.Path.Length - 1 - ".json".Length) + ".json";
        var file = Path.Combine(directory.Path, name);
        Assert.Equal(200, file.Length);
        await File.WriteAllTextAsync(file, "previous export");
        var repository = new FakeProfileRepository([CreateProfile("Alpha", EnvironmentKind.Development)], null);
        var dialogs = new FakeDialogService { SaveFilePath = file };
        await using var vm = CreateViewModel(repository, new FakeWorkspace(), dialogs);
        await vm.InitializeAsync();
        string? temporary = null, previous = null;
        QueueLoom.App.Services.SafeFileWriter.ReplaceOverride.Value = (replacement, destination, backup) =>
        {
            (temporary, previous) = (replacement, backup);
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

        Assert.NotNull(temporary);
        Assert.NotNull(previous);
        Assert.True(File.Exists(temporary));
        Assert.True(File.Exists(previous));
        var activity = vm.Activity.First(item => item.Level == "Error");
        foreach (var shown in new[] { vm.ErrorText, activity.Details })
        {
            Assert.Contains($"'{previous}'", shown, StringComparison.Ordinal);
            Assert.Contains($"'{temporary}'", shown, StringComparison.Ordinal);
        }
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
