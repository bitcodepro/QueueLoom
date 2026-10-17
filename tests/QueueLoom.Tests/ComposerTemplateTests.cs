using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// A message typed by hand in the Composer could not be kept: each test message was retyped. Templates save it by name.
public sealed class ComposerTemplateStoreTests
{
    [Fact]
    public async Task ATemplateIsSavedListedReplacedByNameWithoutCaseAndDeleted()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonComposerTemplateStore(directory.Path);
        var order = new ComposerTemplate("  Order created ", "{\"id\":1}", MessageBodyFormat.Json, "application/json", Subject: "order.created",
            ApplicationProperties: "{\"tenant\":{\"type\":\"String\",\"value\":\"acme\"}}");

        await store.SaveAsync(order);
        await store.SaveAsync(new ComposerTemplate("Another", "x", MessageBodyFormat.Text));
        var saved = await store.ListAsync();

        Assert.Equal(["Another", "Order created"], saved.Select(template => template.Name));
        Assert.Equal(order with { Name = "Order created" }, saved[1]);

        await store.SaveAsync(order with { Name = "ORDER CREATED", Body = "{\"id\":2}" });
        var replaced = Assert.Single(await store.ListAsync(), template => template.Name.Equals("order created", StringComparison.OrdinalIgnoreCase));
        Assert.Equal("{\"id\":2}", replaced.Body);
        Assert.Equal(2, Directory.GetFiles(directory.Path).Length);

        await store.DeleteAsync("order CREATED");
        Assert.Equal("Another", Assert.Single(await store.ListAsync()).Name);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("line\nbreak")]
    public async Task ANameThatIsEmptyOrHasControlCharactersIsRefused(string name)
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonComposerTemplateStore(directory.Path);

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.SaveAsync(new ComposerTemplate(name, "x", MessageBodyFormat.Text)));
        Assert.Throws<InvalidOperationException>(() => ComposerTemplate.NormalizeName(new string('n', ComposerTemplate.MaximumNameLength + 1)));
    }

    [Fact]
    public async Task ABodyLargerThanTheLimitIsRefused()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonComposerTemplateStore(directory.Path);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.SaveAsync(new ComposerTemplate("big", new string('x', JsonComposerTemplateStore.MaximumTemplateBytes), MessageBodyFormat.Text)));

        Assert.Contains("1 MiB", error.Message, StringComparison.Ordinal);
        Assert.Empty(await store.ListAsync());
    }

    // One damaged file, or a template file copied under another file name, must not hide or shadow the others.
    [Fact]
    public async Task DamagedAndMisplacedFilesAreSkipped()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonComposerTemplateStore(directory.Path);
        await store.SaveAsync(new ComposerTemplate("Kept", "x", MessageBodyFormat.Text));
        var kept = Assert.Single(Directory.GetFiles(directory.Path));
        File.WriteAllText(Path.Combine(directory.Path, "damaged.json"), "{ not json");
        File.Copy(kept, Path.Combine(directory.Path, "copy.json"));
        File.WriteAllText(Path.Combine(directory.Path, "foreign.json"), "{\"name\":\"Foreign\",\"body\":\"x\",\"bodyFormat\":\"Nope\"}");

        Assert.Equal("Kept", Assert.Single(await store.ListAsync()).Name);
    }

    [Fact]
    public async Task NoFolderMeansNoTemplates()
    {
        using var directory = new TemporaryDirectory();

        Assert.Empty(await new JsonComposerTemplateStore(Path.Combine(directory.Path, "missing")).ListAsync());
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task ComposerTemplate_SavesTheDraftAndStartsANewMessageFromIt()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonComposerTemplateStore(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        await using var vm = new QueueLoom.App.ViewModels.MainWindowViewModel(new FakeProfileRepository([profile], profile.Id),
            new FakeSecretVault(), new FakeWorkspace(), new FakeDialogService { ConfirmResult = true }, templates: store);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.NewMessageCommand.Execute(null);
        vm.DraftBody = "{\"orderId\":42}";
        vm.DraftSubject = "order.created";
        vm.DraftCorrelationId = "corr-1";
        vm.DraftTimeToLiveSeconds = "60";
        vm.DraftScheduledEnqueueTime = "2030-01-01T00:00:00Z";
        vm.DraftApplicationProperties = "{\"tenant\":{\"type\":\"String\",\"value\":\"acme\"}}";
        vm.TemplateName = "Order 42";

        await vm.SaveComposerTemplateCommand.ExecuteAsync();

        Assert.Equal("Order 42", vm.SelectedComposerTemplate?.Name);
        var firstId = vm.DraftMessageId;
        vm.NewMessageCommand.Execute(null);
        Assert.NotEqual("{\"orderId\":42}", vm.DraftBody);

        vm.SelectedComposerTemplate = Assert.Single(vm.ComposerTemplates);
        vm.UseComposerTemplateCommand.Execute(null);

        Assert.Equal("{\"orderId\":42}", vm.DraftBody);
        Assert.Equal("order.created", vm.DraftSubject);
        Assert.Equal("corr-1", vm.DraftCorrelationId);
        Assert.Equal("60", vm.DraftTimeToLiveSeconds);
        Assert.Contains("acme", vm.DraftApplicationProperties, StringComparison.Ordinal);
        // A message made from a template is a new message: new ID, no scheduled time.
        Assert.NotEqual(firstId, vm.DraftMessageId);
        Assert.Empty(vm.DraftScheduledEnqueueTime);
        Assert.Contains("template 'Order 42'", vm.DraftOriginNotice, StringComparison.Ordinal);

        // Another window (or a later start) sees it.
        await using var reopened = new QueueLoom.App.ViewModels.MainWindowViewModel(new FakeProfileRepository([profile], profile.Id),
            new FakeSecretVault(), new FakeWorkspace(), new FakeDialogService { ConfirmResult = true }, templates: store);
        await reopened.InitializeAsync();
        Assert.Equal("Order 42", Assert.Single(reopened.ComposerTemplates).Name);

        await vm.DeleteComposerTemplateCommand.ExecuteAsync();
        Assert.Empty(vm.ComposerTemplates);
        Assert.Empty(await store.ListAsync());
    }

    [Fact]
    public async Task ComposerTemplate_NeedsAConnectionToStartAMessage()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonComposerTemplateStore(directory.Path);
        await store.SaveAsync(new ComposerTemplate("T", "x", MessageBodyFormat.Text));
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        await using var vm = new QueueLoom.App.ViewModels.MainWindowViewModel(new FakeProfileRepository([profile], profile.Id),
            new FakeSecretVault(), new FakeWorkspace(), new FakeDialogService(), templates: store);

        await vm.InitializeAsync();
        vm.SelectedComposerTemplate = Assert.Single(vm.ComposerTemplates);

        Assert.False(vm.UseComposerTemplateCommand.CanExecute(null));
        await vm.ConnectCommand.ExecuteAsync();
        Assert.True(vm.UseComposerTemplateCommand.CanExecute(null));
    }
}
