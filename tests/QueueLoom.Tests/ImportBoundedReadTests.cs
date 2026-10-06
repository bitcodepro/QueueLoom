using QueueLoom.Core.Profiles;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    // The environments file is a link (its own size is tiny) to a large file: the 5 MB limit applies to what is read,
    // so it is refused, and nothing is imported.
    [Fact]
    public async Task ALinkToALargeFileIsNotImported()
    {
        using var directory = new TemporaryDirectory();
        var target = Path.Combine(directory.Path, "large.json");
        using (var large = File.Create(target))
        {
            large.SetLength(6L * 1024 * 1024);
        }
        var link = Path.Combine(directory.Path, "environments.json");
        try
        {
            File.CreateSymbolicLink(link, target);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            return; // No permission to create links here (Windows without developer mode).
        }
        var repository = new FakeProfileRepository([], null);
        await using var vm = CreateViewModel(repository, new FakeWorkspace(), new FakeDialogService { OpenFilePath = link });
        await vm.InitializeAsync();

        await vm.ImportEnvironmentsCommand.ExecuteAsync();

        Assert.Contains("too large to be an environments file", vm.ErrorText, StringComparison.Ordinal);
        Assert.Empty(await repository.ListAsync());
    }

    // A file within the limit, with a byte order mark, still imports as before.
    [Fact]
    public async Task AnEnvironmentsFileWithAByteOrderMarkStillImports()
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "environments.json");
        await File.WriteAllTextAsync(file, EnvironmentTransfer.Export([CreateProfile("Alpha", EnvironmentKind.Development)]),
            new System.Text.UTF8Encoding(encoderShouldEmitUTF8Identifier: true));
        var repository = new FakeProfileRepository([], null);
        await using var vm = CreateViewModel(repository, new FakeWorkspace(), new FakeDialogService { OpenFilePath = file });
        await vm.InitializeAsync();

        await vm.ImportEnvironmentsCommand.ExecuteAsync();

        Assert.Single(await repository.ListAsync());
    }
}
