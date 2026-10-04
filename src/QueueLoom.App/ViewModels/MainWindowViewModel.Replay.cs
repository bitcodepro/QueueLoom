using QueueLoom.Core.Abstractions;
using QueueLoom.App.Commands;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed partial class MainWindowViewModel
{
    private readonly IBatchReplayStore? _replayStore;
    private DestinationItemViewModel? _replayDestination;
    private bool _preserveReplayMessageIds;
    private int _replayRate = 5;
    private string _replayStatus = "Choose a destination. Replay sends copies; original messages and backups remain unchanged.";
    public DestinationItemViewModel? ReplayDestination
    {
        get => _replayDestination;
        set { SetProperty(ref _replayDestination, value); NotifyReplayFeatures(); }
    }
    public bool PreserveReplayMessageIds { get => _preserveReplayMessageIds; set => SetProperty(ref _preserveReplayMessageIds, value); }
    public int ReplayRate { get => _replayRate; set => SetProperty(ref _replayRate, value); }
    public string ReplayStatus { get => _replayStatus; private set => SetProperty(ref _replayStatus, value); }
    public AsyncRelayCommand RestoreFilteredBackupsCommand { get; private set; } = null!;
    public AsyncRelayCommand ReplayLoadedMessagesCommand { get; private set; } = null!;
    public AsyncRelayCommand ResumeReplayCommand { get; private set; } = null!;
    private bool CanPrepareReplay => !IsBusy && CanWrite && _replayStore is not null && ReplayDestination is not null &&
        Destinations.Contains(ReplayDestination);

    private void InitializeReplayFeatures()
    {
        InitializeOperationHistory();
        RestoreFilteredBackupsCommand = _commands.Create(
            token => RunWorkspaceOperationAsync("Restoring filtered backups", ct => PrepareReplayAsync(true, ct), token),
            () => CanPrepareReplay && FilteredBackupMessages.Count is > 0 and <= 1000);
        ReplayLoadedMessagesCommand = _commands.Create(
            token => RunWorkspaceOperationAsync("Replaying loaded messages", ct => PrepareReplayAsync(false, ct), token),
            () => CanPrepareReplay && Messages.Count is > 0 and <= 1000);
        ResumeReplayCommand = _commands.Create(
            token => RunWorkspaceOperationAsync("Resuming latest batch", ResumeReplayAsync, token),
            () => !IsBusy && CanWrite && _replayStore is not null);
        FilteredBackupMessages.CollectionChanged += (_, _) => NotifyReplayFeatures();
        Destinations.CollectionChanged += (_, _) => NotifyReplayFeatures();
    }

    private void NotifyReplayFeatures()
    {
        RestoreFilteredBackupsCommand?.NotifyCanExecuteChanged();
        ReplayLoadedMessagesCommand?.NotifyCanExecuteChanged(); ResumeReplayCommand?.NotifyCanExecuteChanged();
    }

    private bool HasBrokerAssignedIdentity(Guid profileId) =>
        Profiles.FirstOrDefault(profile => profile.Id == profileId)?.Provider is
            MessagingProvider.AzureServiceBus or MessagingProvider.AmazonSqsSns or MessagingProvider.GooglePubSub or MessagingProvider.Kafka;

    private async Task PrepareReplayAsync(bool backups, CancellationToken token)
    {
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect first.");
        var destination = ReplayDestination?.Reference ?? throw new InvalidOperationException("Choose a destination.");
        var preserve = PreserveReplayMessageIds;
        var rate = ReplayRate;
        var inputs = new List<(MessageDraft Draft, string Origin)>();
        long totalBytes = 0;
        if (backups)
        {
            // A failed settlement retried later leaves two backups of one message; restore it once. That identity is
            // only trusted where the broker assigns it (Service Bus sequence numbers, SQS and Pub/Sub message IDs,
            // Kafka offsets). RabbitMQ derives it from the publisher's Message ID, which independent deliveries can
            // share, so those backups, and those of environments no longer listed, are all restored. The namespace is
            // part of the identity: an environment can be repointed to another namespace under the same profile id,
            // and a backup that does not record its namespace is never merged.
            var unique = FilteredBackupMessages
                .DistinctBy(item => string.IsNullOrEmpty(item.Summary.MessageId) ||
                                    string.IsNullOrEmpty(item.Summary.FullyQualifiedNamespace) ||
                                    !HasBrokerAssignedIdentity(item.Summary.ProfileId)
                    ? (object)item.Summary.FilePath
                    : (item.Summary.ProfileId, item.Summary.FullyQualifiedNamespace.ToUpperInvariant(), item.Summary.Source,
                        item.Summary.SubQueue, item.Summary.SequenceNumber, item.Summary.MessageId))
                .ToArray();
            foreach (var item in unique)
            {
                var message = await _backupRepository!.LoadAsync(item.Summary, token).ConfigureAwait(true);
                totalBytes += message.Body.Length;
                if (totalBytes > 32 * 1024 * 1024) throw new InvalidOperationException("Narrow the backup filter to at most 32 MiB.");
                inputs.Add((message.CreateDraft(), $"{item.ProfileName} / {item.SourceDisplay} / {item.MessageId}"));
            }
        }
        else
        {
            foreach (var item in Messages)
                inputs.Add((item.Message.CreateDraft(), $"{item.ProfileName} / {item.Message.Source.Path} / {item.Message.Properties.MessageId}"));
        }
        if (!await ConfirmReplayAsync(profile, destination, inputs.Count, preserve, rate, token).ConfigureAwait(true)) return;
        if (!CanWrite || ConnectedProfileId != profile.Id) throw new InvalidOperationException("Environment or write access changed.");
            var plan = await _replayStore!.CreateAsync(profile.Id, destination, inputs, preserve, rate, token, profile.EndpointDisplay,
                ScheduledResend.IdentityFor(profile)).ConfigureAwait(true);
        await ExecuteReplayPlanAsync(plan, token).ConfigureAwait(true);
    }

    private async Task ResumeReplayAsync(CancellationToken token)
    {
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect first.");
        var plan = _replayStore!.Latest(profile.Id) ?? throw new InvalidOperationException("No saved batch for this environment.");
        if (!await ConfirmReplayAsync(profile, plan.Destination, plan.Count, plan.PreserveMessageIds, plan.MessagesPerSecond, token).ConfigureAwait(true)) return;
        await ExecuteReplayPlanAsync(plan, token).ConfigureAwait(true);
    }

    private Task<bool> ConfirmReplayAsync(ServiceBusProfile profile, ServiceBusEntityReference destination,
        int count, bool preserve, int rate, CancellationToken token) => _dialogs.ConfirmAsync("Review batch replay",
        $"Destination environment: {profile.Name}\n{profile.Provider.DisplayName()}: {profile.EndpointDisplay}\nDestination: {destination.Path}\n" +
        $"Batch size: {count} (resume skips acknowledged items)\nRate: {rate}/second\n" +
        $"Message IDs: {(preserve ? "preserved; duplicate detection may suppress delivery" : "new stable IDs assigned to the batch")}\n\n" +
        "All selected messages are copied to this destination, including messages from other environments. " +
        "Originals remain unchanged. Scheduled time is cleared; TTL is retained. " +
        (destination.Kind == ServiceBusEntityKind.Topic ? "The topic can fan out to every matching subscription. " : "") +
        "A local replay snapshot contains full message bodies. Uncertain deliveries block automatic resume.",
        isDangerous: true, requiredText: profile.Environment == EnvironmentKind.Production ? destination.Name : null,
        cancellationToken: token);

    private async Task ExecuteReplayPlanAsync(ReplayPlan plan, CancellationToken token)
    {
        RecordOperationIntent("Batch replay started", $"Batch {plan.Id:N} · {plan.Count} copies to {plan.Destination.Path}", plan.Destination);
        ReplayStatus = $"Batch {plan.Id:N} · starting";
        var progressFinished = false;
        try
        {
            var result = await _replayStore!.RunAsync(plan, _workspace, () => CanWrite,
                new Progress<ReplayProgress>(p => { if (!progressFinished) ReplayStatus = $"{p.Sent}/{p.Total} acknowledged · {p.Status}"; }), token).ConfigureAwait(true);
            ReplayStatus = $"Batch {plan.Id:N}: {result.Sent}/{result.Total} acknowledged. Originals retained.";
            AddActivity("Success", "Batch replay completed", ReplayStatus, plan.Destination);
        }
        catch (Exception exception)
        {
            ReplayStatus = $"Batch {plan.Id:N} stopped: {SanitizeException(exception)}. Progress saved in {_replayStore!.RootDirectory}";
            AddActivity("Warning", "Batch replay stopped", ReplayStatus, plan.Destination);
            throw;
        }
        finally { progressFinished = true; RefreshOperationHistory(); }
    }
}
