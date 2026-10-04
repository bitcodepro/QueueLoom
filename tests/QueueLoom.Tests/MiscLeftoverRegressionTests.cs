using System.Reflection;
using System.Text;
using System.Text.Json.Nodes;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

/// <summary>Cycle 7: dialogs, statuses and activity titles that said something other than what happened.</summary>
public sealed partial class ViewModelStateTests
{
    // ---- Item 5: "Replay loaded messages" ignored the ticks --------------------------------------------------------

    // The confirmation said "All selected messages are copied" but every loaded message was replayed, ticked or not.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ReplayLoadedMessages_ReplaysTheTickedMessagesAndSaysSo(bool tick)
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var source = ServiceBusEntityReference.Queue("source");
        var workspace = new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("target", new ServiceBusEntityRuntime(new ServiceBusMessageCounts()))])
        };
        var store = new BatchReplayStore(directory.Path);
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs, replayStore: store);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        vm.ReplayDestination = Assert.Single(vm.Destinations);
        foreach (var number in new long[] { 1, 2, 3 })
        {
            vm.Messages.Add(new MessageItemViewModel(new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, number,
                Encoding.UTF8.GetBytes($"body-{number}"), new EditableMessageProperties(MessageId: $"m-{number}")), profile.Id, profile.Name));
        }
        if (tick)
        {
            vm.Messages[0].IsMarked = true;
            vm.Messages[2].IsMarked = true;
        }

        Assert.True(vm.ReplayLoadedMessagesCommand.CanExecute(null));
        await vm.ReplayLoadedMessagesCommand.ExecuteAsync();

        var expected = tick ? new[] { "m-1", "m-3" } : ["m-1", "m-2", "m-3"];
        var message = Assert.Single(dialogs.Confirmations).Message;
        Assert.Contains($"Batch size: {expected.Length}", message, StringComparison.Ordinal);
        Assert.Contains(tick ? "The 2 messages you ticked are copied" : "All 3 messages in the list are copied", message, StringComparison.Ordinal);
        Assert.DoesNotContain("All selected messages", message, StringComparison.Ordinal);
        Assert.Equal(expected.Length, workspace.SentMessages.Count);
        var plan = Assert.Single(store.List());
        Assert.Equal(expected, store.ReadHistory(plan).Items.Select(item => item.Origin.Split(" / ")[^1]));
    }

    // ---- Item 8: activity titles that ignored cancelled or partial results ----------------------------------------

    [Fact]
    public async Task PendingRemoval_CancelledRunIsNotTitledRemoved()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var workspace = PendingWorkspace();
        workspace.PendingOutcome = message => message.SequenceNumber == 3
            ? DeadLetterMessageDeletionOutcome.Cancelled
            : DeadLetterMessageDeletionOutcome.Deleted;
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.BrowseSelectedActiveCommand.ExecuteAsync();
        viewModel.SelectDeadLetterReasonCommand.Execute(viewModel.DeadLetterReasons.Single(reason => reason.Reason == "Scheduled"));

        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        var entry = viewModel.Activity.First(item => item.Details.Contains("cancelled or removed", StringComparison.Ordinal));
        Assert.Equal("Removal of scheduled or deferred messages cancelled", entry.Action);
        Assert.Equal("Warning", entry.Level);
        Assert.Contains("1 of 2 cancelled or removed", entry.Details, StringComparison.Ordinal);
        Assert.Contains("1 not processed (cancelled)", entry.Details, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PendingRemoval_CompleteRunIsStillTitledRemoved()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var workspace = PendingWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.BrowseSelectedActiveCommand.ExecuteAsync();
        viewModel.SelectDeadLetterReasonCommand.Execute(viewModel.DeadLetterReasons.Single(reason => reason.Reason == "Scheduled"));

        await viewModel.DeleteMarkedMessagesCommand.ExecuteAsync();

        var entry = viewModel.Activity.First(item => item.Details.Contains("cancelled or removed", StringComparison.Ordinal));
        Assert.Equal("Scheduled or deferred messages removed", entry.Action);
        Assert.Contains("2 of 2 cancelled or removed", entry.Details, StringComparison.Ordinal);
    }

    // Recovery from the operation history always logged "Info / Operation recovery stopped" with only sent and failed
    // counts, even when the operator cancelled it half way.
    [Fact]
    public async Task OperationRecovery_CancelledRunIsLoggedAsCancelledWithItsCounts()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(directory.Path);
        await PrepareReplayRegression(store, profile);
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs, replayStore: store);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.RefreshOperationHistoryCommand.ExecuteAsync();
        Assert.Equal(2, vm.OperationItems.Count);
        foreach (var item in vm.OperationItems) item.IsMarked = true;
        workspace.OnSend = () => vm.CancelCurrentOperationCommand.Execute(null);

        await vm.ContinueOperationCommand.ExecuteAsync();

        Assert.Single(workspace.SentMessages);
        var entry = vm.Activity.First(item => item.Action.StartsWith("Operation recovery", StringComparison.Ordinal) &&
                                              item.Action != "Operation recovery started");
        Assert.Equal("Operation recovery cancelled", entry.Action);
        Assert.Equal("Warning", entry.Level);
        Assert.Contains("1 of 2 acknowledged", entry.Details, StringComparison.Ordinal);
        Assert.Contains("1 not sent (cancelled)", entry.Details, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OperationRecovery_CompleteRunIsLoggedAsCompleted()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(directory.Path);
        await PrepareReplayRegression(store, profile);
        var workspace = new FakeWorkspace();
        var dialogs = new FakeDialogService { ConfirmResult = true };
        await using var vm = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace, dialogs, replayStore: store);
        await vm.InitializeAsync();
        await vm.ConnectCommand.ExecuteAsync();
        await vm.RefreshOperationHistoryCommand.ExecuteAsync();
        foreach (var item in vm.OperationItems) item.IsMarked = true;

        await vm.ContinueOperationCommand.ExecuteAsync();

        Assert.Equal(2, workspace.SentMessages.Count);
        var entry = vm.Activity.First(item => item.Action.StartsWith("Operation recovery", StringComparison.Ordinal) &&
                                              item.Action != "Operation recovery started");
        Assert.Equal("Operation recovery completed", entry.Action);
        Assert.Equal("Success", entry.Level);
        Assert.Contains("2 of 2 acknowledged", entry.Details, StringComparison.Ordinal);
    }

    private static FakeWorkspace PendingWorkspace()
    {
        var orders = ServiceBusEntityReference.Queue("orders");
        BrowsedMessage Active(long number, ServiceBusMessageState state) => new(
            orders, ServiceBusSubQueue.Active, number, Encoding.UTF8.GetBytes("order"),
            new EditableMessageProperties(MessageId: $"m-{number}",
                ScheduledEnqueueTime: state == ServiceBusMessageState.Scheduled ? DateTimeOffset.Parse("2026-10-01T09:00:00Z") : null),
            state: state);
        return new FakeWorkspace
        {
            Topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
                [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 4)))]),
            BrowseMessages =
            [
                Active(1, ServiceBusMessageState.Active), Active(2, ServiceBusMessageState.Scheduled),
                Active(3, ServiceBusMessageState.Scheduled), Active(4, ServiceBusMessageState.Deferred)
            ]
        };
    }

    // ---- Item 9: the "…" of the delete-backups confirmation counted queues, not the listed groups ------------------

    [Fact]
    public async Task DeleteVisibleBackups_OverflowMarkerCountsTheListedProfileAndSourceGroups()
    {
        var orders = ServiceBusEntityReference.Queue("orders");
        var profiles = Enumerable.Range(1, 9).Select(index => CreateProfile($"Env {index}", EnvironmentKind.Test)).ToArray();
        var backups = new ListBackupRepository(profiles.Select((profile, index) => new DeadLetterBackupSummary(
            Path.Combine(Path.GetTempPath(), "backups", $"{profile.Id:N}-{index}.json"), profile.Id, profile.Name, "Test", null, orders,
            ServiceBusSubQueue.DeadLetter, index, $"m-{index}", null, null, null, DateTimeOffset.UnixEpoch.AddMinutes(index), 10)).ToArray());
        var dialogs = new FakeDialogService { ConfirmResult = false };
        await using var viewModel = CreateViewModel(new FakeProfileRepository(profiles, profiles[0].Id), new FakeWorkspace(), dialogs, backups);
        await viewModel.InitializeAsync();
        await viewModel.RefreshBackupsCommand.ExecuteAsync();
        Assert.Equal(9, viewModel.FilteredBackupMessages.Count);

        await viewModel.DeleteVisibleBackupsCommand.ExecuteAsync();

        var lines = Assert.Single(dialogs.Confirmations).Message.Split('\n');
        Assert.Equal(8, lines.Count(line => line.StartsWith("• Env ", StringComparison.Ordinal)));
        Assert.Contains(lines, line => line.StartsWith("• …", StringComparison.Ordinal));
    }

    // ---- Item 10: a reason chip left compare-only ticks behind its "Ticked N" ----------------------------------------

    [Fact]
    public async Task ReasonChip_TicksExactlyItsMessagesSoTheStatusCountAndTheTicksAgree()
    {
        var profile = CreateProfile("Orders", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        var workspace = PendingWorkspace();
        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), workspace);
        await viewModel.InitializeAsync();
        await viewModel.ConnectCommand.ExecuteAsync();
        await viewModel.BrowseSelectedActiveCommand.ExecuteAsync();
        // An active message ticked to compare or export: it can be ticked, but not deleted or resent.
        var active = viewModel.Messages.Single(message => message.SequenceNumber == 1);
        Assert.False(active.CanDelete);
        active.IsMarked = true;

        viewModel.SelectDeadLetterReasonCommand.Execute(viewModel.DeadLetterReasons.Single(reason => reason.Reason == "Scheduled"));

        Assert.StartsWith("Ticked 2 scheduled message(s)", viewModel.StatusText, StringComparison.Ordinal);
        Assert.Equal(2, viewModel.MarkedMessageCount);
        Assert.Equal([2L, 3L], viewModel.Messages.Where(message => message.IsMarked).Select(message => message.SequenceNumber));
        Assert.True(viewModel.HasMarkedMessagesForChanges);
        Assert.Equal("Cancel 2 scheduled…", viewModel.DeleteMarkedMessagesLabel);
    }

    // ---- Item 18: an empty backup session folder younger than an hour was never swept later --------------------------

    [Fact]
    public async Task StartUp_SweepsQuietEmptyBackupSessionsButNeverSessionsWithFiles()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var profile = CreateProfile("Test", EnvironmentKind.Test);
        var store = new DeadLetterJsonBackupStore(paths);
        var quiet = DateTime.UtcNow.AddHours(-2);

        async Task<string> Session(int daysAgo, int messages)
        {
            var session = await store.CreateSessionAsync(profile, DateTimeOffset.UtcNow.AddDays(-daysAgo), CancellationToken.None);
            for (var index = 1; index <= messages; index++)
            {
                await session.BackupAsync(SearchMessage(ServiceBusEntityReference.Queue("orders"), index, "2026-09-01T10:00:00Z"),
                    CancellationToken.None);
            }
            return session.RootDirectory;
        }

        void Age(string session)
        {
            File.SetLastWriteTimeUtc(Path.Combine(session, "session.json"), quiet);
            foreach (var folder in Directory.EnumerateDirectories(session, "*", SearchOption.AllDirectories).Append(session))
                Directory.SetLastWriteTimeUtc(folder, quiet);
        }

        // Its last backup was deleted while the session was young, so the folder kept only session.json.
        var finished = await Session(10, 1);
        var repository = new JsonDeadLetterBackupRepository(paths);
        await repository.DeleteAsync(Assert.Single(await repository.ListAsync()));
        Assert.True(File.Exists(Path.Combine(finished, "session.json")));
        Age(finished);
        var young = await Session(9, 0);
        var withMessage = await Session(8, 1);
        Age(withMessage);
        var writing = await Session(7, 0);
        var entity = Directory.CreateDirectory(Path.Combine(writing, "q-orders", "dlq")).FullName;
        await File.WriteAllTextAsync(Path.Combine(entity, ".abcdef.tmp"), "{");
        Age(writing);

        await using var viewModel = CreateViewModel(new FakeProfileRepository([profile], profile.Id), new FakeWorkspace(),
            backupRepository: new JsonDeadLetterBackupRepository(paths));
        await viewModel.InitializeAsync();

        Assert.False(Directory.Exists(finished));
        Assert.False(Directory.Exists(Path.GetDirectoryName(finished)));
        Assert.True(File.Exists(Path.Combine(young, "session.json")));
        Assert.True(File.Exists(Path.Combine(withMessage, "session.json")));
        Assert.Single(await repository.ListAsync());
        Assert.True(File.Exists(Path.Combine(entity, ".abcdef.tmp")));
        Assert.True(File.Exists(Path.Combine(writing, "session.json")));
    }
}

// ---- Item 11: Teams rendered markdown in broker names ------------------------------------------------------------------

public sealed class TeamsWebhookMarkdownTests
{
    // Adaptive Cards TextBlock renders a Markdown subset (bold, italic, lists and [Title](url) hyperlinks), so a RabbitMQ
    // queue or environment named "[click](https://evil.example)" became a link in the Teams card. RichTextBlock "does not
    // support markdown" (https://learn.microsoft.com/adaptive-cards/authoring-cards/text-features).
    [Fact]
    public void TeamsCardShowsBrokerNamesLiterallyNeverAsMarkdown()
    {
        var alert = new MonitorAlert("[click](https://evil.example)", "**orders** _x_ (DLQ)", 3, null);

        var payload = MonitorAlertService.BuildWebhookPayload("https://prod-01.westeurope.logic.azure.com/workflows/abc", alert);

        var body = payload["attachments"]![0]!["content"]!["body"]!.AsArray();
        Assert.DoesNotContain(body, element => element!["type"]!.GetValue<string>() == "TextBlock" &&
                                               (element["text"]!.GetValue<string>().Contains("evil", StringComparison.Ordinal) ||
                                                element["text"]!.GetValue<string>().Contains("orders", StringComparison.Ordinal)));
        var runs = body.Where(element => element!["type"]!.GetValue<string>() == "RichTextBlock")
            .SelectMany(element => element!["inlines"]!.AsArray())
            .Select(run => run!["text"]!.GetValue<string>());
        Assert.Contains(alert.Text, runs);
    }
}

// ---- Item 6: SQS dead-letter queue created meanwhile by someone else ----------------------------------------------------

public sealed class SqsDeadLetterOwnershipTests
{
    // Another operator (Terraform, a script, an older QueueLoom: all keep dead letters 14 days) creates "orders-dlq"
    // between QueueLoom's GetQueueUrl check and its CreateQueue. SQS answers with that queue's URL when the attributes
    // match, and the new "orders" then dead-lettered into someone else's queue.
    [Fact]
    public async Task ADeadLetterQueueAnotherOperatorCreatedMeanwhileIsRefusedBeforeTheQueueIsCreated()
    {
        using var sqs = new RacingSqs { Racer = ("orders-dlq", new Dictionary<string, string> { ["MessageRetentionPeriod"] = "1209600" }) };
        await using var workspace = Workspace(sqs);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workspace.CreateQueueAsync(new QueueDefinition("orders", new QueueSettings(MaxDeliveryCount: 3), CreateDeadLetterQueue: true)));

        Assert.Contains("orders-dlq", error.Message, StringComparison.Ordinal);
        Assert.False(sqs.Queues.ContainsKey("orders"));
        Assert.Equal(new Dictionary<string, string> { ["MessageRetentionPeriod"] = "1209600" }, sqs.Queues["orders-dlq"]);
        Assert.Empty(sqs.Deleted);
    }

    // SQS-compatible servers (and any SQS behaviour change) may return an existing queue whatever the attributes; the
    // retention read back right after CreateQueue then shows the queue is not the one just created.
    [Fact]
    public async Task AnExistingQueueReturnedDespiteDifferentAttributesIsNeverAdoptedOrChanged()
    {
        using var sqs = new RacingSqs { Lenient = true, Racer = ("orders-dlq", new Dictionary<string, string> { ["MessageRetentionPeriod"] = "345600" }) };
        await using var workspace = Workspace(sqs);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workspace.CreateQueueAsync(new QueueDefinition("orders", new QueueSettings(MaxDeliveryCount: 3), CreateDeadLetterQueue: true)));

        Assert.Contains("orders-dlq", error.Message, StringComparison.Ordinal);
        Assert.False(sqs.Queues.ContainsKey("orders"));
        Assert.Equal(new Dictionary<string, string> { ["MessageRetentionPeriod"] = "345600" }, sqs.Queues["orders-dlq"]);
        Assert.Empty(sqs.Deleted);
    }

    [Fact]
    public async Task ANewDeadLetterQueueKeepsMessagesFourteenDaysAndIsConnected()
    {
        using var sqs = new RacingSqs();
        await using var workspace = Workspace(sqs);

        await workspace.CreateQueueAsync(new QueueDefinition("orders", new QueueSettings(MaxDeliveryCount: 3), CreateDeadLetterQueue: true));

        Assert.Equal("1209600", sqs.Queues["orders-dlq"]["MessageRetentionPeriod"]);
        Assert.Contains("arn:aws:sqs:us-east-1:111111111111:orders-dlq", sqs.Queues["orders"]["RedrivePolicy"], StringComparison.Ordinal);
    }

    private static AwsSqsSnsWorkspace Workspace(RacingSqs sqs)
    {
        var workspace = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(AwsSqsSnsWorkspace).GetField("_sqs", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, sqs);
        typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace,
            ViewModelStateTests.CreateProfile("isolated", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with
            {
                Provider = MessagingProvider.AmazonSqsSns, AllowQueueManagement = true
            });
        return workspace;
    }

    /// <summary>
    /// SQS as CreateQueue documents it: an existing name returns its URL when every requested attribute matches and fails
    /// with QueueNameExists "only if the request includes attributes whose values differ from those of the existing queue".
    /// </summary>
    private sealed class RacingSqs() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public Dictionary<string, Dictionary<string, string>> Queues { get; } = new(StringComparer.Ordinal);
        public List<string> Deleted { get; } = [];
        /// <summary>Another operator creates this queue, with these attributes, right before QueueLoom's CreateQueue of it.</summary>
        public (string Name, Dictionary<string, string> Attributes)? Racer { get; init; }
        /// <summary>Returns an existing queue whatever the requested attributes, as some SQS-compatible servers do.</summary>
        public bool Lenient { get; init; }

        private static string Url(string name) => "https://sqs.us-east-1.amazonaws.com/111111111111/" + name;
        private static string NameOf(string url) => url.TrimEnd('/').Split('/')[^1];

        public override Task<GetQueueUrlResponse> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken = default) =>
            GetQueueUrlAsync(new GetQueueUrlRequest { QueueName = queueName }, cancellationToken);

        public override Task<GetQueueUrlResponse> GetQueueUrlAsync(GetQueueUrlRequest request, CancellationToken cancellationToken = default) =>
            Queues.ContainsKey(request.QueueName)
                ? Task.FromResult(new GetQueueUrlResponse { QueueUrl = Url(request.QueueName) })
                : throw new QueueDoesNotExistException("The specified queue does not exist.") { StatusCode = System.Net.HttpStatusCode.BadRequest };

        public override Task<CreateQueueResponse> CreateQueueAsync(CreateQueueRequest request, CancellationToken cancellationToken = default)
        {
            var attributes = request.Attributes ?? [];
            if (Racer is { } racer && racer.Name == request.QueueName && !Queues.ContainsKey(racer.Name))
            {
                Queues[racer.Name] = new Dictionary<string, string>(racer.Attributes, StringComparer.Ordinal);
            }
            if (Queues.TryGetValue(request.QueueName, out var existing))
            {
                if (Lenient || attributes.All(pair => existing.GetValueOrDefault(pair.Key) == pair.Value))
                    return Task.FromResult(new CreateQueueResponse { QueueUrl = Url(request.QueueName) });
                throw new QueueNameExistsException("A queue already exists with the same name and a different value for attribute MessageRetentionPeriod")
                    { StatusCode = System.Net.HttpStatusCode.BadRequest };
            }
            Queues[request.QueueName] = new Dictionary<string, string>(attributes, StringComparer.Ordinal);
            return Task.FromResult(new CreateQueueResponse { QueueUrl = Url(request.QueueName) });
        }

        public override Task<GetQueueAttributesResponse> GetQueueAttributesAsync(GetQueueAttributesRequest request,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new GetQueueAttributesResponse
            {
                Attributes = new Dictionary<string, string>(Queues[NameOf(request.QueueUrl)], StringComparer.Ordinal)
                {
                    ["QueueArn"] = "arn:aws:sqs:us-east-1:111111111111:" + NameOf(request.QueueUrl)
                }
            });

        public override Task<SetQueueAttributesResponse> SetQueueAttributesAsync(SetQueueAttributesRequest request,
            CancellationToken cancellationToken = default)
        {
            foreach (var (name, value) in request.Attributes) Queues[NameOf(request.QueueUrl)][name] = value;
            return Task.FromResult(new SetQueueAttributesResponse());
        }

        public override Task<DeleteQueueResponse> DeleteQueueAsync(string queueUrl, CancellationToken cancellationToken = default) =>
            DeleteQueueAsync(new DeleteQueueRequest { QueueUrl = queueUrl }, cancellationToken);

        public override Task<DeleteQueueResponse> DeleteQueueAsync(DeleteQueueRequest request, CancellationToken cancellationToken = default)
        {
            Deleted.Add(NameOf(request.QueueUrl));
            Queues.Remove(NameOf(request.QueueUrl));
            return Task.FromResult(new DeleteQueueResponse());
        }
    }
}
