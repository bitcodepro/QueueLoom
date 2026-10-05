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
}
