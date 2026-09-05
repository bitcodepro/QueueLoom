using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Monitoring;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.Security;

// Deliberately fixed to loopback and synthetic ql-* entities. No cloud credentials.
const string connection = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
var mode = args.FirstOrDefault() ?? "seed";
var runId = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
var ct = timeout.Token;
var admin = new ServiceBusAdministrationClient(connection.Replace("localhost;", "localhost:5300;"));
var ns = await admin.GetNamespacePropertiesAsync(ct);
Console.WriteLine($"PASS management: {ns.Value.Name}");
await using var client = new ServiceBusClient(connection);

if (mode == "seed")
{
    await using var orders = client.CreateSender("ql-orders");
    await using var events = client.CreateSender("ql-events");
    for (var i = 0; i < 12; i++)
    {
        await orders.SendMessageAsync(Message("order", i), ct);
        await events.SendMessageAsync(Message("event", i), ct);
    }
    // Controlled consumer failures; leave active messages for non-destructive UI Peek.
    await using var receiver = client.CreateReceiver("ql-orders");
    for (var i = 0; i < 4; i++)
    {
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct)
            ?? throw new Exception("Queue consumer received no message");
        await receiver.DeadLetterMessageAsync(received, "ValidationFailed", "Synthetic order missing a customer reference", ct);
    }
    await using var billing = client.CreateReceiver("ql-events", "billing");
    for (var i = 0; i < 3; i++)
    {
        var received = await billing.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct)
            ?? throw new Exception("Billing consumer received no message");
        await billing.DeadLetterMessageAsync(received, "BillingUnavailable", "Synthetic downstream failure", ct);
    }
    await using var analytics = client.CreateReceiver("ql-events", "analytics");
    for (var i = 0; i < 5; i++)
    {
        var received = await analytics.ReceiveMessageAsync(TimeSpan.FromSeconds(10), ct)
            ?? throw new Exception("Analytics consumer received no message");
        await analytics.CompleteMessageAsync(received, ct);
    }
    Console.WriteLine("PASS consumers: 4 queue DLQ, 3 billing DLQ, 5 analytics completed; active messages remain");
}

var profile = new ServiceBusProfile(Guid.Parse("24d30bd6-14ca-4e46-932f-0c2d74fe3b3b"), "Local emulator", EnvironmentKind.Test, null,
    "localhost", AuthenticationSettings.ConnectionString(), ProfileAccessMode.ReadWrite);
var labRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "lab-data"));
var paths = QueueLoomPaths.ForRoot(labRoot);
if (mode == "install-profile")
{
    var appPaths = QueueLoomPaths.CreateDefault();
    using var profiles = new JsonProfileRepository(appPaths);
    using var vault = new EncryptedFileSecretVault(appPaths);
    await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), connection, ct);
    await profiles.UpsertAsync(profile, ct);
    await profiles.SetSelectedProfileIdAsync(profile.Id, ct);
    if (Directory.Exists(paths.BackupsDirectory))
    {
        foreach (var file in Directory.EnumerateFiles(paths.BackupsDirectory, "*", SearchOption.AllDirectories))
        {
            var destination = Path.Combine(appPaths.BackupsDirectory, "lab-demo", Path.GetRelativePath(paths.BackupsDirectory, file));
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            if (!File.Exists(destination)) File.Copy(file, destination);
        }
    }
    Console.WriteLine("PASS saved Local emulator test profile in the current user's QueueLoom vault");
}
await using var workspace = new AzureServiceBusWorkspace(new LabVault(connection), backupStore: new DeadLetterJsonBackupStore(paths));
await workspace.ConnectAsync(profile, ct);
Console.WriteLine("PASS QueueLoom workspace connected");
var topology = await workspace.GetTopologyAsync(true, ct);
Console.WriteLine($"PASS topology: {topology.Queues.Count} queues / {topology.Topics.Count} topics");
foreach (var source in new[] { ServiceBusEntityReference.Queue("ql-orders"), ServiceBusEntityReference.Subscription("ql-events", "billing") })
{
    foreach (var subQueue in new[] { ServiceBusSubQueue.Active, ServiceBusSubQueue.DeadLetter })
    {
        var messages = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, subQueue), ct);
        Console.WriteLine($"PASS Peek {source.Path} {subQueue}: {messages.Count}");
        if (messages.Count == 0) throw new Exception($"Expected seeded messages in {source.Path} {subQueue}");
        if (subQueue == ServiceBusSubQueue.DeadLetter && messages.Any(m => string.IsNullOrEmpty(m.DeadLetterReason)))
            throw new Exception("DLQ reason was not mapped");
    }
}
var snapshot = await workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All, ct);
if (snapshot.HasFailures) throw new Exception("DLQ scan had source failures");
Console.WriteLine($"PASS DLQ scan: {snapshot.TotalCount} messages");
var search = await workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest("synthetic",
    new[] { new DeadLetterSearchTarget(ServiceBusEntityReference.Queue("ql-orders"), ServiceBusSubQueue.DeadLetter, 0),
        new DeadLetterSearchTarget(ServiceBusEntityReference.Subscription("ql-events", "billing"), ServiceBusSubQueue.DeadLetter, 0) }), ct);
if (search.HasFailures || search.MatchCount == 0) throw new Exception("DLQ search failed");
Console.WriteLine($"PASS DLQ search body/properties: {search.MatchCount} matches; {search.ScannedMessageCount} inspected");

if (mode == "roundtrip")
{
    var source = ServiceBusEntityReference.Queue("ql-orders");
    var before = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter), ct);
    var purge = await workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest(
        new[] { new DeadLetterPurgeTarget(source, ServiceBusSubQueue.DeadLetter) }, batchSize: 2, maximumMessagesPerSubQueue: 2), ct);
    if (purge.DeletedCount != 2) throw new Exception($"Expected bounded deletion of 2, got {purge.DeletedCount}");
    var after = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter), ct);
    if (after.Count != before.Count - 2) throw new Exception("Bounded purge removed an unexpected number");
    var repository = new JsonDeadLetterBackupRepository(paths);
    var backups = (await repository.ListAsync(ct)).Where(b => b.FilePath.StartsWith(purge.BackupDirectory, StringComparison.OrdinalIgnoreCase)).ToArray();
    if (backups.Length != 2) throw new Exception("Expected two durable backups");
    var drafts = new List<(MessageDraft, string)>();
    foreach (var backup in backups) drafts.Add(((await repository.LoadAsync(backup, ct)).CreateDraft(), backup.FilePath));
    var store = new BatchReplayStore(Path.Combine(labRoot, "replay"));
    var plan = await store.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("ql-replay"), drafts, false, 10, ct);
    var result = await store.RunAsync(plan, workspace, () => true, null, ct);
    var replayed = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("ql-replay")), ct);
    foreach (var draft in drafts)
        if (!replayed.Any(m => m.Body.Span.SequenceEqual(draft.Item1.Body.GetBytes()))) throw new Exception("Restored body differs from backup");
    if (result.Sent != 2) throw new Exception("Restore count mismatch");
    Console.WriteLine($"PASS bounded purge + backup + restore: {result.Sent} full bodies verified; remaining DLQ {after.Count}");
    var again = await new BatchReplayStore(Path.Combine(labRoot, "replay")).RunAsync(plan, workspace, () => true, null, ct);
    var final = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("ql-replay")), ct);
    if (final.Count != replayed.Count) throw new Exception("Resume duplicated acknowledged messages");
    Console.WriteLine("PASS reopening/resuming completed replay sends no duplicates");
    Console.WriteLine($"Artifacts: {labRoot}");
}
Console.WriteLine($"PASS lab run {runId}");

ServiceBusMessage Message(string kind, int index)
{
    var message = new ServiceBusMessage(JsonSerializer.Serialize(new { kind, index, runId, customer = "synthetic", amount = index * 10 }))
    {
        MessageId = $"ql-{runId}-{kind}-{index}", CorrelationId = $"ql-{runId}", Subject = $"{kind}.created", ContentType = "application/json"
    };
    message.ApplicationProperties["lab"] = true;
    message.ApplicationProperties["index"] = index;
    return message;
}

sealed class LabVault(string connection) : ISecretVault
{
    public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult<string?>(connection);
    public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult(true);
    public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => throw new NotSupportedException();
}
