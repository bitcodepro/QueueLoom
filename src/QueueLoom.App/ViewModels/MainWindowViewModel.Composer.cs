using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using QueueLoom.App.Models;
using QueueLoom.App.Serialization;
using QueueLoom.App.Services;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;

namespace QueueLoom.App.ViewModels;

/// <summary>The message draft and sending.</summary>
public sealed partial class MainWindowViewModel
{
    private readonly Dictionary<(Guid ProfileId, string MessageId), (string Fingerprint, bool IsMove)> _composerSendAttempts = [];

    public DestinationItemViewModel? SelectedDestination
    {
        get => _selectedDestination;
        set
        {
            if (SetProperty(ref _selectedDestination, value))
            {
                NotifyCommandStates();
                OnPropertyChanged(nameof(SubjectHint));
                OnPropertyChanged(nameof(HasSubjectHint));
            }
        }
    }

    /// <summary>What Subject means for the chosen destination, where it is more than a label.</summary>
    public string SubjectHint => (ConnectedProvider, SelectedDestination?.Reference.Kind) switch
    {
        (MessagingProvider.RabbitMq, ServiceBusEntityKind.Topic) => "Used as the routing key of the exchange.",
        (MessagingProvider.RabbitMq, ServiceBusEntityKind.Queue) => "RabbitMQ reaches a queue by its name, so the subject is not sent.",
        _ => string.Empty
    };

    public bool HasSubjectHint => SubjectHint.Length > 0;

    public string DraftBody
    {
        get => _draftBody;
        set
        {
            if (SetProperty(ref _draftBody, value))
            {
                OnPropertyChanged(nameof(DraftSizeText));
            }
        }
    }

    public MessageBodyFormat DraftBodyFormat
    {
        get => _draftBodyFormat;
        set
        {
            if (SetProperty(ref _draftBodyFormat, value))
            {
                OnPropertyChanged(nameof(DraftSizeText));
                OnPropertyChanged(nameof(IsDraftBodyJson));
            }
        }
    }

    public bool IsDraftBodyJson => DraftBodyFormat == MessageBodyFormat.Json;

    public string DraftMessageId { get => _draftMessageId; set => SetProperty(ref _draftMessageId, value); }
    public string DraftCorrelationId { get => _draftCorrelationId; set => SetProperty(ref _draftCorrelationId, value); }
    public string DraftSubject { get => _draftSubject; set => SetProperty(ref _draftSubject, value); }
    public string DraftContentType { get => _draftContentType; set => SetProperty(ref _draftContentType, value); }
    public string DraftSessionId { get => _draftSessionId; set => SetProperty(ref _draftSessionId, value); }
    public string DraftTo { get => _draftTo; set => SetProperty(ref _draftTo, value); }
    public string DraftReplyTo { get => _draftReplyTo; set => SetProperty(ref _draftReplyTo, value); }
    public string DraftReplyToSessionId { get => _draftReplyToSessionId; set => SetProperty(ref _draftReplyToSessionId, value); }
    public string DraftPartitionKey { get => _draftPartitionKey; set => SetProperty(ref _draftPartitionKey, value); }
    public string DraftTransactionPartitionKey { get => _draftTransactionPartitionKey; set => SetProperty(ref _draftTransactionPartitionKey, value); }
    public string DraftScheduledEnqueueTime { get => _draftScheduledEnqueueTime; set => SetProperty(ref _draftScheduledEnqueueTime, value); }
    public string DraftTimeToLiveSeconds { get => _draftTimeToLiveSeconds; set => SetProperty(ref _draftTimeToLiveSeconds, value); }
    public string DraftApplicationProperties { get => _draftApplicationProperties; set => SetProperty(ref _draftApplicationProperties, value); }

    public string DraftOriginNotice
    {
        get => _draftOriginNotice;
        private set
        {
            // The notice is set whenever the draft's origin changes, so the resend choice starts over with it,
            // even when two dead-letter drafts share the same notice text.
            SetProperty(ref _draftOriginNotice, value);
            DraftMovesOriginal = false;
            OnPropertyChanged(nameof(CanMoveDraftOriginal));
        }
    }

    /// <summary>The draft came from a dead-letter queue, so sending can also remove the original.</summary>
    public bool CanMoveDraftOriginal => _draftSourceMessage is { IsDeadLetter: true } && !_draftSourceIsLocalBackup && CanDeleteSelectedMessages;

    /// <summary>
    /// False: send a copy and keep the original (the default). True: send, then back up the original and remove it
    /// from the dead-letter queue.
    /// </summary>
    public bool DraftMovesOriginal
    {
        get => _draftMovesOriginal;
        set
        {
            if (SetProperty(ref _draftMovesOriginal, value))
            {
                OnPropertyChanged(nameof(DraftKeepsOriginal));
                OnPropertyChanged(nameof(SendDraftLabel));
            }
        }
    }

    public bool DraftKeepsOriginal
    {
        get => !DraftMovesOriginal;
        set => DraftMovesOriginal = !value;
    }

    public string SendDraftLabel => CanMoveDraftOriginal && DraftMovesOriginal ? "Send and remove original" : "Send message";

    // Compared with the operator's environment, as drafts are bound: a monitor check that temporarily connects to
    // another environment changes neither the warning nor the Send button. The actual connection is checked again at
    // the send itself, which waits for such a check to give the connection back.
    public bool HasDraftEnvironmentMismatch =>
        IsConnected && _draftProfileId != _connectedProfile?.Id;

    public string DraftEnvironmentWarning => HasDraftEnvironmentMismatch
        ? _draftProfileId.HasValue
            ? $"This draft belongs to '{_draftProfileName ?? "another environment"}'. Reconnect that environment or start a new message before choosing a destination."
            : "This draft is not pinned to an environment. Start a new message before choosing a destination."
        : string.Empty;

    public string DraftSizeText
    {
        get
        {
            try
            {
                var length = DraftBodyFormat is MessageBodyFormat.Text or MessageBodyFormat.Json
                    ? Encoding.UTF8.GetByteCount(DraftBody)
                    : new EditableMessageBody(DraftBody, DraftBodyFormat).GetBytes().Length;
                return $"{length:N0} bytes";
            }
            catch
            {
                return "Invalid body encoding";
            }
        }
    }

    private void NewMessage()
    {
        _draftSourceMessage = null;
        _draftSourceProperties = null;
        _draftSourceIsLocalBackup = false;
        BindDraftToConnectedEnvironment();
        DraftBody = "{\n  \"event\": \"example\"\n}";
        DraftBodyFormat = MessageBodyFormat.Json;
        DraftMessageId = Guid.NewGuid().ToString("N");
        DraftCorrelationId = string.Empty;
        DraftSubject = string.Empty;
        DraftContentType = "application/json";
        DraftSessionId = string.Empty;
        DraftTo = string.Empty;
        DraftReplyTo = string.Empty;
        DraftReplyToSessionId = string.Empty;
        DraftPartitionKey = string.Empty;
        DraftTransactionPartitionKey = string.Empty;
        DraftScheduledEnqueueTime = string.Empty;
        DraftTimeToLiveSeconds = string.Empty;
        DraftApplicationProperties = "{}";
        DraftOriginNotice = "New message · destination is pinned to the connected environment";
        NavigateTo(NavigationPage.Composer);
    }

    private void OpenSelectedMessageAsDraft()
    {
        var selectedItem = SelectedMessage ?? throw new InvalidOperationException("Select a message first.");
        OpenMessageAsDraft(selectedItem, null, isLocalBackup: false);
    }

    private void OpenBackupAsDraft()
    {
        var selectedItem = SelectedBackupMessage
            ?? throw new InvalidOperationException("Load a backup message first.");
        OpenMessageAsDraft(
            selectedItem,
            "Local backup draft · Send creates a new copy. The backup JSON remains unchanged.",
            isLocalBackup: true);
    }

    private void OpenMessageAsDraft(
        MessageItemViewModel selectedItem,
        string? originNotice,
        bool isLocalBackup)
    {
        // Monitors temporarily switch the broker connection; drafts stay pinned to the operator's environment.
        if (selectedItem.ProfileId is { } profileId && profileId != _connectedProfile?.Id)
        {
            throw new InvalidOperationException(
                "This message belongs to another environment. Connect to that environment before opening it as a draft.");
        }
        var selected = selectedItem.Message;
        var draft = selected.CreateDraft();
        _draftSourceMessage = selected;
        _draftSourceProperties = selected.Properties;
        _draftSourceIsLocalBackup = isLocalBackup;
        BindDraftToConnectedEnvironment();
        DraftBody = draft.Body.Content;
        DraftBodyFormat = draft.Body.Format;
        DraftMessageId = draft.Properties.MessageId ?? Guid.NewGuid().ToString("N");
        DraftCorrelationId = draft.Properties.CorrelationId ?? string.Empty;
        // An SNS notification read without raw delivery may carry its subject only natively (no Subject attribute).
        DraftSubject = draft.Properties.Subject ?? draft.Properties.NativeSubject ?? string.Empty;
        DraftContentType = draft.Properties.ContentType ?? string.Empty;
        DraftSessionId = draft.Properties.SessionId ?? string.Empty;
        DraftTo = draft.Properties.To ?? string.Empty;
        DraftReplyTo = draft.Properties.ReplyTo ?? string.Empty;
        DraftReplyToSessionId = draft.Properties.ReplyToSessionId ?? string.Empty;
        DraftPartitionKey = draft.Properties.PartitionKey ?? string.Empty;
        DraftTransactionPartitionKey = draft.Properties.TransactionPartitionKey ?? string.Empty;
        DraftScheduledEnqueueTime = draft.Properties.ScheduledEnqueueTime?.ToString("O", CultureInfo.InvariantCulture)
            ?? string.Empty;
        DraftTimeToLiveSeconds = FormatTimeToLiveSeconds(draft.Properties.TimeToLive);
        DraftApplicationProperties = ApplicationPropertiesJson.Serialize(draft.ApplicationProperties);

        var destination = selected.Source.Kind == ServiceBusEntityKind.Subscription
            ? ServiceBusEntityReference.Topic(selected.Source.TopicName!)
            : ServiceBusEntityReference.Queue(selected.Source.Name);
        SelectedDestination = Destinations.FirstOrDefault(item => item.Reference == destination);
        DraftOriginNotice = originNotice ?? (selected.IsDeadLetter
            ? "DLQ draft · choose below whether the original stays in the DLQ."
            : "Peeked active-message draft · Send creates a new copy and leaves the original message unchanged.");
        if (destination.Kind == ServiceBusEntityKind.Topic)
        {
            DraftOriginNotice += " The suggested destination is a topic and may fan out to every matching subscription.";
        }
        NavigateTo(NavigationPage.Composer);
    }

    private async Task SendDraftAsync(CancellationToken cancellationToken)
    {
        var destination = SelectedDestination
            ?? throw new InvalidOperationException("Choose a queue or topic destination.");
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect to an environment first.");
        if (HasDraftEnvironmentMismatch)
        {
            throw new InvalidOperationException(
                "This draft belongs to a different environment. Reconnect it or start a new message before sending.");
        }
        // Under the workspace gate the connection is the operator's again; never send if it is not the draft's.
        if (_workspace.ConnectedProfileId != _draftProfileId)
        {
            throw new InvalidOperationException(
                "The connection is not on this draft's environment right now. Try sending again.");
        }
        var draft = BuildDraft();
        // The destination service's limits, before the confirmation and before anything reaches the service.
        if (MessageDraftValidator.Validate(draft, profile.Provider) is { IsValid: false } invalid)
        {
            throw new InvalidOperationException(string.Join(" ", invalid.Errors.Select(error => error.Message)));
        }

        var warning = _draftSourceIsLocalBackup
            ? "Send a copy reconstructed from the local backup? The backup JSON remains unchanged."
            : _draftSourceMessage switch
        {
            { IsDeadLetter: true } when DraftMovesOriginal =>
                "Send this message, then back up the original and remove it from the DLQ? " +
                "If the send fails, the original stays where it is.",
            { IsDeadLetter: true } =>
                "Send the edited copy? The original message stays in DLQ.",
            not null =>
                "Send an edited copy of the peeked active message? The original message is not changed.",
            _ => "Send this new message to the selected entity?"
        };
        if (destination.Reference.Kind == ServiceBusEntityKind.Topic)
        {
            warning += "\n\nThe selected destination is a topic. This copy may fan out to every matching subscription.";
        }
        if (_draftSourceMessage is not null && draft.Properties.MessageId == _draftSourceMessage.Properties.MessageId)
        {
            warning += "\n\nThe original MessageId is currently preserved. With duplicate detection enabled, Azure and SQS/SNS FIFO may accept the send but suppress the duplicate; change MessageId when a distinct delivery is required.";
        }
        var isMove = _draftSourceMessage is { IsDeadLetter: true } && DraftMovesOriginal && !_draftSourceIsLocalBackup;
        // Keep the same identity for an unchanged retry, including an accepted send whose response was lost.
        // A copy or an edited move is a different operation and must not reuse its attempted duplicate detection ID.
        var operationFingerprint = profile.Provider is MessagingProvider.AzureServiceBus or MessagingProvider.AmazonSqsSns ? Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
        {
            Configuration = ScheduledResend.IdentityFor(profile), Destination = destination.Reference,
            Original = _draftSourceMessage is null ? null : new
            {
                _draftSourceMessage.Source, _draftSourceMessage.SubQueue,
                _draftSourceMessage.SequenceNumber, _draftSourceMessage.Properties.MessageId
            }, Draft = draft
        })))) : string.Empty;
        var attemptKey = (profile.Id, draft.Properties.MessageId!);
        if ((profile.Provider is MessagingProvider.AzureServiceBus or MessagingProvider.AmazonSqsSns) && isMove &&
            _composerSendAttempts.TryGetValue(attemptKey, out var previous) &&
            (!previous.IsMove || previous.Fingerprint != operationFingerprint))
            throw new InvalidOperationException("This MessageId was already attempted by another composer operation. Choose a new MessageId and review the move before removing the original.");
        if (_draftSourceMessage is { IsDeadLetter: true } originalForMove && isMove)
            DeadLetterResender.EnsureSafeMessageIds(profile.Provider, [new ResendItem(originalForMove, destination.Reference, draft)], ResendMode.Move);
        var confirmed = await _dialogs.ConfirmAsync(
            $"Send to {destination.Name}",
            $"Environment: {profile.Name}\nDestination: {destination.Reference.DisplayName}\nMessageId: {draft.Properties.MessageId}\n\n{warning}",
            isDangerous: profile.Environment == EnvironmentKind.Production,
            requiredText: profile.Environment == EnvironmentKind.Production ? destination.Reference.Name : null,
            cancellationToken: cancellationToken)
            .ConfigureAwait(true);
        if (!confirmed)
        {
            return;
        }

        if (!CanWrite)
        {
            throw new InvalidOperationException(
                "Write access expired while the confirmation was open. Unlock writes again and review the send.");
        }

        if (profile.Provider is MessagingProvider.AzureServiceBus or MessagingProvider.AmazonSqsSns)
            RecordComposerSendAttempt(attemptKey, operationFingerprint, isMove);

        if (_draftSourceMessage is not null && _draftSourceMessage.IsDeadLetter && !_draftSourceIsLocalBackup)
        {
            var original = _draftSourceMessage;
            var mode = DraftMovesOriginal ? ResendMode.Move : ResendMode.Copy;
            var result = await DeadLetterResender.ResendAsync(
                    _workspace,
                    [new ResendItem(original, destination.Reference, draft)],
                    mode,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(true);
            var item = result.Items[0];
            switch (item.Outcome)
            {
                case ResendOutcome.Failed:
                    throw new InvalidOperationException(item.Detail ?? "The message could not be sent.");
                case ResendOutcome.Cancelled:
                    StatusText = "Send cancelled";
                    return;
                case ResendOutcome.Moved:
                    RemoveResentOriginals(result);
                    // The original is gone; sending the draft again is a new message, not another move.
                    _draftSourceMessage = null;
                    DraftOriginNotice = "Sent · the original was backed up and removed. Sending again creates a new message.";
                    StatusText = "Message sent · original backed up and removed from the DLQ" + OperationWarnings(result.Warnings);
                    AddActivity(result.Warnings.Count == 0 ? "Success" : "Warning", "DLQ message moved",
                        $"{profile.Name} · {original.Source.DisplayName} → {destination.Reference.DisplayName} · backup {result.BackupDirectory}" + OperationWarnings(result.Warnings),
                        destination.Reference);
                    break;
                case ResendOutcome.SentOriginalKept:
                    StatusText = "Message sent · the original is still in the DLQ";
                    AddActivity("Warning", "DLQ message sent, original kept",
                        $"{profile.Name} · {destination.Reference.DisplayName} · {item.Detail}", destination.Reference);
                    await _dialogs.ShowMessageAsync("Original not removed",
                        $"The message was sent to {destination.Reference.DisplayName}, but the original could not be removed.\n\n{item.Detail}",
                        isError: true, cancellationToken: CancellationToken.None).ConfigureAwait(true);
                    break;
                default:
                    StatusText = "Copy accepted · original remains in DLQ";
                    AddActivity(
                        "Success",
                        "DLQ copy send accepted",
                        $"{profile.Name} · {destination.Reference.DisplayName}",
                        destination.Reference);
                    break;
            }
        }
        else
        {
            var diagnosticSend = Diagnostics.Begin("Sending message", profile.Provider, profile.EndpointDisplay, destination.Reference.DisplayName);
            await _workspace.SendMessageAsync(
                new SendMessageRequest(destination.Reference, draft),
                cancellationToken).ConfigureAwait(true);
            Diagnostics.Record(diagnosticSend, DiagnosticStage.Completed, DiagnosticOutcome.Confirmed);
            StatusText = "Message accepted";
            AddActivity(
                "Success",
                "Message send accepted",
                $"{profile.Name} · {destination.Reference.DisplayName}",
                destination.Reference);
        }
    }

    /// <summary>
    /// Shows a TTL as seconds that parse back to the same positive value; rounding to milliseconds turned a
    /// sub-millisecond TTL into "0", which the composer then refused to send.
    /// </summary>
    internal static string FormatTimeToLiveSeconds(TimeSpan? timeToLive) =>
        timeToLive?.TotalSeconds.ToString("R", CultureInfo.InvariantCulture) ?? string.Empty;

    internal int ComposerSendAttemptCount => _composerSendAttempts.Count;

    /// <summary>
    /// Remembers a send attempt for the rest of the session. Entries are never evicted: they are safety evidence for
    /// broker duplicate detection (up to 7 days on Azure), so a forgotten MessageId would let an edited move reuse it,
    /// be acknowledged but discarded as a duplicate, and delete the original. One small entry per manual send is cheap.
    /// </summary>
    internal void RecordComposerSendAttempt((Guid ProfileId, string MessageId) key, string fingerprint, bool isMove) =>
        _composerSendAttempts[key] = (fingerprint, isMove);

    private MessageDraft BuildDraft()
    {
        TimeSpan? timeToLive = null;
        if (!string.IsNullOrWhiteSpace(DraftTimeToLiveSeconds))
        {
            // No thousands separators: "1.5" or "1,5" must never become 15 seconds.
            if (!(double.TryParse(DraftTimeToLiveSeconds, NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds) ||
                  double.TryParse(DraftTimeToLiveSeconds, NumberStyles.Float, CultureInfo.CurrentCulture, out seconds)) ||
                !double.IsFinite(seconds) || seconds <= 0)
            {
                throw new InvalidOperationException("TTL must be a positive number of seconds.");
            }
            try
            {
                timeToLive = TimeSpan.FromSeconds(seconds);
            }
            catch (Exception exception) when (exception is OverflowException or ArgumentOutOfRangeException)
            {
                // Finite and positive is not enough: "1e15" seconds does not fit in a TimeSpan.
                throw new InvalidOperationException("TTL is too long to be a time span; enter a number of seconds a broker can keep.");
            }
        }

        DateTimeOffset? scheduledEnqueueTime = null;
        if (!string.IsNullOrWhiteSpace(DraftScheduledEnqueueTime))
        {
            if (!DateTimeOffset.TryParse(
                    DraftScheduledEnqueueTime,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeUniversal,
                    out var scheduledAt))
            {
                throw new InvalidOperationException(
                    "Scheduled enqueue time must be an ISO 8601 timestamp, for example 2026-08-11T14:30:00Z.");
            }
            scheduledEnqueueTime = scheduledAt;
        }

        IReadOnlyList<MessageApplicationProperty> applicationProperties;
        try
        {
            applicationProperties = ApplicationPropertiesJson.Deserialize(DraftApplicationProperties);
        }
        catch (JsonException exception)
        {
            throw new InvalidOperationException("Application properties must be valid typed JSON.", exception);
        }

        var sourceProperties = _draftSourceMessage?.Properties ?? EditableMessageProperties.Empty;
        var properties = sourceProperties with
        {
            MessageId = NormalizeDraftProperty(DraftMessageId, sourceProperties.MessageId) ?? Guid.NewGuid().ToString("N"),
            CorrelationId = NormalizeDraftProperty(DraftCorrelationId, sourceProperties.CorrelationId),
            ContentType = NormalizeDraftProperty(DraftContentType, sourceProperties.ContentType),
            Subject = NormalizeDraftProperty(DraftSubject, sourceProperties.Subject),
            To = NormalizeDraftProperty(DraftTo, sourceProperties.To),
            ReplyTo = NormalizeDraftProperty(DraftReplyTo, sourceProperties.ReplyTo),
            SessionId = NormalizeDraftProperty(DraftSessionId, sourceProperties.SessionId),
            ReplyToSessionId = NormalizeDraftProperty(DraftReplyToSessionId, sourceProperties.ReplyToSessionId),
            PartitionKey = NormalizeDraftProperty(DraftPartitionKey, sourceProperties.PartitionKey),
            TransactionPartitionKey = NormalizeDraftProperty(DraftTransactionPartitionKey, sourceProperties.TransactionPartitionKey),
            TimeToLive = timeToLive,
            ScheduledEnqueueTime = scheduledEnqueueTime
        };
        // The AMQP envelope has no field of its own, so it comes from the message the draft was opened from, also after a
        // move cleared the original: sending the draft again must not publish a gzip body without its encoding.
        if (_draftSourceProperties is { } source)
        {
            properties = properties with
            {
                AmqpType = source.AmqpType,
                AmqpAppId = source.AmqpAppId,
                AmqpContentEncoding = source.AmqpContentEncoding,
                AmqpPriority = source.AmqpPriority
            };
        }
        // Where the subject came from outlives the original: after a move the original is gone, but sending the draft
        // again must still publish a native-only subject natively, not as a new Subject attribute.
        if (_draftSourceProperties is { NativeSubject: { } nativeSubject } subjectSource)
        {
            // The Subject field shows the attribute, or the native SNS subject when there is no attribute. An edit is
            // what the operator wants published, natively too; untouched, the native subject is published as it was
            // and a native-only subject never turns into a Subject attribute.
            var shown = subjectSource.Subject ?? nativeSubject;
            var edited = (DraftSubject ?? string.Empty) != shown;
            properties = properties with
            {
                Subject = subjectSource.Subject is null ? null : properties.Subject,
                NativeSubject = edited ? NullIfWhiteSpace(DraftSubject) : nativeSubject
            };
        }
        return new MessageDraft(
            new EditableMessageBody(DraftBody ?? string.Empty, DraftBodyFormat),
            properties,
            applicationProperties) { KafkaEnvelope = _draftSourceMessage?.KafkaEnvelope };
    }

    private string? NormalizeDraftProperty(string? value, string? original)
    {
        if (_draftSourceMessage?.KafkaEnvelope is null) return NullIfWhiteSpace(value);

        // The editor shows null as empty text. Preserve the original projection when untouched,
        // including empty/null and whitespace, so the mapper can retain raw Kafka metadata.
        if (value == (original ?? string.Empty)) return original;
        return string.IsNullOrEmpty(value) ? null : value;
    }

    private void BindDraftToConnectedEnvironment()
    {
        // The environment the operator connected to: a monitor check may have the workspace on another one for a moment.
        _draftProfileId = _connectedProfile?.Id;
        _draftProfileName = _connectedProfile?.Name;
        OnPropertyChanged(nameof(HasDraftEnvironmentMismatch));
        OnPropertyChanged(nameof(DraftEnvironmentWarning));
        SendDraftCommand.NotifyCanExecuteChanged();
    }
}
