using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// A message typed by hand in the Composer could not be kept: each test message was retyped. Templates save it by name.
public sealed class ComposerTemplateStoreTests
{
    /// <summary>
    /// Waits, bounded, until every started task has finished. When the test body already failed, that failure is kept
    /// and a drain timeout is ignored; otherwise a timeout fails the test, so nothing waits without end afterwards.
    /// Tasks' own results are not observed here.
    /// </summary>
    internal static async Task DrainAsync(bool bodyCompleted, TimeSpan safety, params Task[] tasks)
    {
        var finished = Task.WhenAll(tasks).ContinueWith(_ => { }, TaskScheduler.Default);
        if (bodyCompleted)
        {
            await finished.WaitAsync(safety);
            return;
        }
        try { await finished.WaitAsync(safety); }
        catch (TimeoutException) { }
    }

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
        Assert.Equal(2, Directory.GetFiles(directory.Path, "*.json").Length);

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
        var kept = Assert.Single(Directory.GetFiles(directory.Path, "*.json"));
        File.WriteAllText(Path.Combine(directory.Path, "damaged.json"), "{ not json");
        File.Copy(kept, Path.Combine(directory.Path, "copy.json"));
        File.WriteAllText(Path.Combine(directory.Path, "foreign.json"), "{\"name\":\"Foreign\",\"body\":\"x\",\"bodyFormat\":\"Nope\"}");

        Assert.Equal("Kept", Assert.Single(await store.ListAsync()).Name);
    }

    // On a case-sensitive file system a copy can differ from the real file name only in case; saving and deleting use
    // the real name, so the copy must not count either.
    [Fact]
    public async Task ACopyUnderAFileNameThatDiffersOnlyInCaseIsSkipped()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonComposerTemplateStore(directory.Path);
        await store.SaveAsync(new ComposerTemplate("Kept", "x", MessageBodyFormat.Text));
        var kept = Assert.Single(Directory.GetFiles(directory.Path, "*.json"));
        var upper = Path.Combine(directory.Path, Path.GetFileNameWithoutExtension(kept).ToUpperInvariant() + ".json");
        if (File.Exists(upper))
        {
            return; // A case-insensitive file system cannot hold the two names apart.
        }
        File.Copy(kept, upper);

        Assert.Equal("Kept", Assert.Single(await store.ListAsync()).Name);
        await store.DeleteAsync("Kept");
        Assert.Empty(await store.ListAsync());
    }

    // The count and the write were separate: two windows saving different names at 199 templates both saw room.
    [Fact]
    public async Task TwoSavesAtTheLastFreePlaceCannotBothBeAdmitted()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonComposerTemplateStore(directory.Path);
        for (var index = 0; index < JsonComposerTemplateStore.MaximumTemplates - 1; index++)
        {
            await store.SaveAsync(new ComposerTemplate($"T{index}", "x", MessageBodyFormat.Text));
        }
        var safety = TimeSpan.FromSeconds(30);
        var writing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var contended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task first = Task.CompletedTask, second = Task.CompletedTask;
        var bodyCompleted = false;
        try
        {
            // The first window is held inside its write, after it was admitted.
            var previousWriting = AtomicFile.ContentWriting.Value;
            AtomicFile.ContentWriting.Value = _ => { writing.TrySetResult(); release.Task.Wait(); };
            try
            {
                first = Task.Run(() => new JsonComposerTemplateStore(directory.Path).SaveAsync(new ComposerTemplate("First", "x", MessageBodyFormat.Text)));
            }
            finally
            {
                AtomicFile.ContentWriting.Value = previousWriting;
            }
            await writing.Task.WaitAsync(safety);

            // Another window: it signals when it finds the lock taken. Without the lock it would be admitted and
            // finish instead, which the test reports at once.
            var previousContended = CrossProcessFileLock.Contended.Value;
            CrossProcessFileLock.Contended.Value = _ => contended.TrySetResult();
            try
            {
                second = Task.Run(() => new JsonComposerTemplateStore(directory.Path).SaveAsync(new ComposerTemplate("Second", "x", MessageBodyFormat.Text)));
            }
            finally
            {
                CrossProcessFileLock.Contended.Value = previousContended;
            }
            var reached = await Task.WhenAny(contended.Task, second).WaitAsync(safety);
            Assert.True(ReferenceEquals(reached, contended.Task), "The second save was not held at the lock while the first one wrote.");

            release.TrySetResult();
            await first.WaitAsync(safety);
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => second.WaitAsync(safety));
            Assert.Contains($"At most {JsonComposerTemplateStore.MaximumTemplates}", error.Message, StringComparison.Ordinal);
            Assert.Equal(JsonComposerTemplateStore.MaximumTemplates, (await store.ListAsync()).Count);
            bodyCompleted = true;
        }
        finally
        {
            // Every started writer is released and drained before the folder goes, also after a failed assertion.
            release.TrySetResult();
            await DrainAsync(bodyCompleted, safety, first, second);
        }
    }

    // Every *.json file counted toward the limit, also the ones the list skips, so junk the app cannot show or delete
    // refused every save.
    [Fact]
    public async Task FilesTheListSkipsDoNotCountTowardTheLimit()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonComposerTemplateStore(directory.Path);
        for (var index = 0; index < JsonComposerTemplateStore.MaximumTemplates; index++)
        {
            File.WriteAllText(Path.Combine(directory.Path, $"damaged{index}.json"), "{ not json");
        }

        await store.SaveAsync(new ComposerTemplate("New", "x", MessageBodyFormat.Text));

        Assert.Equal("New", Assert.Single(await store.ListAsync()).Name);
        Assert.Equal(JsonComposerTemplateStore.MaximumTemplates + 1, Directory.GetFiles(directory.Path, "*.json").Length);
    }

    // Review of the count: a scan that stopped after 2,000 files counted only the templates among them, so behind
    // enough skipped files every new save was admitted and the limit could be passed without end.
    [Fact]
    public async Task ManySkippedFilesDoNotLetSavesPassTheLimit()
    {
        using var directory = new TemporaryDirectory();
        // The skipped files are listed first, whatever order the file system uses, so a scan bounded by files read
        // would reach none of the templates.
        var store = new JsonComposerTemplateStore(directory.Path)
        {
            EnumerateFiles = folder => Directory.EnumerateFiles(folder, "*.json")
                .OrderBy(file => Path.GetFileName(file).StartsWith('!') ? 0 : 1)
                .ThenBy(file => file, StringComparer.Ordinal)
                .ToArray()
        };
        for (var index = 0; index < JsonComposerTemplateStore.MaximumTemplates; index++)
        {
            await store.SaveAsync(new ComposerTemplate($"T{index}", "x", MessageBodyFormat.Text));
        }
        for (var index = 0; index < 10 * JsonComposerTemplateStore.MaximumTemplates + 100; index++)
        {
            File.WriteAllText(Path.Combine(directory.Path, $"!damaged{index:D5}.json"), "{ not json");
        }

        for (var attempt = 0; attempt < 3; attempt++)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.SaveAsync(new ComposerTemplate($"New{attempt}", "x", MessageBodyFormat.Text)));
        }
        await store.SaveAsync(new ComposerTemplate("T0", "replaced", MessageBodyFormat.Text));

        var templates = await store.ListAsync();
        Assert.Equal(JsonComposerTemplateStore.MaximumTemplates, templates.Count);
        Assert.Equal("replaced", templates.Single(template => template.Name == "T0").Body);
    }

    [Fact]
    public async Task AtTheLimitANewNameIsRefusedButAnExistingOneIsReplaced()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonComposerTemplateStore(directory.Path);
        for (var index = 0; index < JsonComposerTemplateStore.MaximumTemplates; index++)
        {
            await store.SaveAsync(new ComposerTemplate($"T{index}", "x", MessageBodyFormat.Text));
        }

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.SaveAsync(new ComposerTemplate("New", "x", MessageBodyFormat.Text)));
        Assert.Equal($"At most {JsonComposerTemplateStore.MaximumTemplates} templates can be saved. Delete one first.", error.Message);
        await store.SaveAsync(new ComposerTemplate("t0", "replaced", MessageBodyFormat.Text));

        var templates = await store.ListAsync();
        Assert.Equal(JsonComposerTemplateStore.MaximumTemplates, templates.Count);
        Assert.Equal("replaced", Assert.Single(templates, template => template.Name == "t0").Body);
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
        vm.DraftContentType = "application/vnd.order+json";
        vm.DraftTo = "billing";
        vm.DraftReplyTo = "replies";
        vm.DraftSessionId = "cart-7";
        vm.DraftReplyToSessionId = "reply-7";
        vm.DraftPartitionKey = "cart-7";
        vm.DraftTransactionPartitionKey = "tx-7";
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
        Assert.Equal("application/vnd.order+json", vm.DraftContentType);
        Assert.Equal("billing", vm.DraftTo);
        Assert.Equal("replies", vm.DraftReplyTo);
        Assert.Equal("cart-7", vm.DraftSessionId);
        Assert.Equal("reply-7", vm.DraftReplyToSessionId);
        Assert.Equal("cart-7", vm.DraftPartitionKey);
        Assert.Equal("tx-7", vm.DraftTransactionPartitionKey);
        Assert.Equal(MessageBodyFormat.Json, vm.DraftBodyFormat);
        // A message made from a template is a new message: new ID, no scheduled time.
        Assert.NotEqual(firstId, vm.DraftMessageId);
        Assert.Empty(vm.DraftScheduledEnqueueTime);
        Assert.Contains("template 'Order 42'", vm.DraftOriginNotice, StringComparison.Ordinal);

        // Another window (or a later start) sees it.
        await using var reopened = new QueueLoom.App.ViewModels.MainWindowViewModel(new FakeProfileRepository([profile], profile.Id),
            new FakeSecretVault(), new FakeWorkspace(), new FakeDialogService { ConfirmResult = true }, templates: store);
        await reopened.InitializeAsync();
        var kept = Assert.Single(reopened.ComposerTemplates);
        Assert.Equal("Order 42", kept.Name);
        Assert.Equal(("tx-7", "reply-7", "billing", "replies"), (kept.TransactionPartitionKey, kept.ReplyToSessionId, kept.To, kept.ReplyTo));

        await vm.DeleteComposerTemplateCommand.ExecuteAsync();
        Assert.Empty(vm.ComposerTemplates);
        Assert.Empty(await store.ListAsync());
    }

    // An older list published after a newer Delete brought the deleted template back and selected it. Loading, saving
    // and deleting now run one at a time: the older operation is held after it read its list, Delete is started, and
    // the hold is released only once Delete waits for the gate.
    [Theory]
    [InlineData("save")]
    [InlineData("load")]
    public async Task ComposerTemplate_AnOlderListDoesNotBringBackADeletedTemplate(string older)
    {
        var safety = TimeSpan.FromSeconds(30);
        using var directory = new TemporaryDirectory();
        var store = new HeldListStore(new JsonComposerTemplateStore(directory.Path));
        await store.SaveAsync(new ComposerTemplate("A", "x", MessageBodyFormat.Text));
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        await using var vm = new QueueLoom.App.ViewModels.MainWindowViewModel(new FakeProfileRepository([profile], profile.Id),
            new FakeSecretVault(), new FakeWorkspace(), new FakeDialogService { ConfirmResult = true }, templates: store);
        await vm.InitializeAsync().WaitAsync(safety);
        await vm.ConnectCommand.ExecuteAsync();
        vm.SelectedComposerTemplate = Assert.Single(vm.ComposerTemplates);

        var held = store.HoldNextList();
        Task olderOperation = Task.CompletedTask, deleting = Task.CompletedTask;
        var bodyCompleted = false;
        try
        {
            olderOperation = older == "save" ? vm.SaveComposerTemplateCommand.ExecuteAsync() : vm.LoadComposerTemplatesAsync();
            await held.Entered.WaitAsync(safety);
            var deletesBefore = store.Deletes;
            deleting = vm.DeleteComposerTemplateCommand.ExecuteAsync();
            var deadline = DateTime.UtcNow + safety;
            while (vm.ComposerTemplateWaiters == 0 && store.Deletes == deletesBefore)
            {
                Assert.True(DateTime.UtcNow < deadline, "Delete neither waited for the older operation nor deleted.");
                await Task.Delay(1);
            }
            Assert.Equal(deletesBefore, store.Deletes); // Delete did not run while the older list was held.
            bodyCompleted = true;
        }
        finally
        {
            // Released and drained on every path (the direct load is not owned by a command), keeping a failure above.
            held.Release();
            await ComposerTemplateStoreTests.DrainAsync(bodyCompleted, safety, olderOperation, deleting);
        }
        // Both have finished (the drain saw to it): their own failures surface here.
        await Task.WhenAll(olderOperation, deleting);

        Assert.Empty(await store.ListAsync());
        Assert.Empty(vm.ComposerTemplates);
        Assert.Null(vm.SelectedComposerTemplate);
        Assert.False(vm.UseComposerTemplateCommand.CanExecute(null));
        Assert.False(vm.DeleteComposerTemplateCommand.CanExecute(null));
    }

    // Waiting for the template gate ignored cancellation: a start-up queued behind a held save stayed busy after Esc.
    [Fact]
    public async Task ComposerTemplate_ACancelledStartUpDoesNotWaitForAHeldSave()
    {
        var safety = TimeSpan.FromSeconds(30);
        using var directory = new TemporaryDirectory();
        var store = new HeldListStore(new JsonComposerTemplateStore(directory.Path));
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        await using var vm = new QueueLoom.App.ViewModels.MainWindowViewModel(new FakeProfileRepository([profile], profile.Id),
            new FakeSecretVault(), new FakeWorkspace(), new FakeDialogService { ConfirmResult = true }, templates: store);
        vm.TemplateName = "Held";
        using var cancellation = new CancellationTokenSource();
        var held = store.HoldNextList();
        Task saving = Task.CompletedTask, starting = Task.CompletedTask;
        var bodyCompleted = false;
        try
        {
            saving = vm.SaveComposerTemplateCommand.ExecuteAsync();
            await held.Entered.WaitAsync(safety);
            var listsBefore = store.Lists;
            starting = vm.InitializeAsync(cancellation.Token);
            var deadline = DateTime.UtcNow + safety;
            while (vm.ComposerTemplateWaiters == 0)
            {
                Assert.True(DateTime.UtcNow < deadline, "Start-up never waited for the template gate.");
                await Task.Delay(1);
            }

            cancellation.Cancel();
            await starting.ContinueWith(_ => { }, TaskScheduler.Default).WaitAsync(safety);

            // Ended while the save is still held, without reading or publishing templates.
            Assert.False(saving.IsCompleted);
            Assert.False(vm.IsBusy);
            Assert.Equal(listsBefore, store.Lists);
            Assert.Equal(0, vm.ComposerTemplateWaiters);
            bodyCompleted = true;
        }
        finally
        {
            held.Release();
            await ComposerTemplateStoreTests.DrainAsync(bodyCompleted, safety, saving, starting);
        }
        await saving;

        // The gate was not taken by the cancelled wait: later operations still get it.
        await vm.LoadComposerTemplatesAsync().WaitAsync(safety);
        Assert.Equal("Held", Assert.Single(vm.ComposerTemplates).Name);
    }

    /// <summary>A store whose next list can be held after it has read the templates.</summary>
    private sealed class HeldListStore(IComposerTemplateStore inner) : IComposerTemplateStore
    {
        public sealed class Hold
        {
            private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public Task Entered => _entered.Task;
            public void Release() => _released.TrySetResult();
            internal async Task WaitAsync() { _entered.TrySetResult(); await _released.Task; }
        }

        private Hold? _next;
        private int _deletes;
        public int Deletes => Volatile.Read(ref _deletes);
        public Hold HoldNextList() => _next = new Hold();

        private int _lists;
        /// <summary>How many lists were read.</summary>
        public int Lists => Volatile.Read(ref _lists);

        public async Task<IReadOnlyList<ComposerTemplate>> ListAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _lists);
            var templates = await inner.ListAsync(cancellationToken);
            if (Interlocked.Exchange(ref _next, null) is { } hold) await hold.WaitAsync();
            return templates;
        }

        public Task SaveAsync(ComposerTemplate template, CancellationToken cancellationToken = default) => inner.SaveAsync(template, cancellationToken);

        public async Task DeleteAsync(string name, CancellationToken cancellationToken = default)
        {
            await inner.DeleteAsync(name, cancellationToken);
            Interlocked.Increment(ref _deletes);
        }
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
