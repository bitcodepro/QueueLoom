using System.Globalization;
using System.Text;
using System.Text.Json;
using QueueLoom.App.Models;
using QueueLoom.App.Serialization;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>The message draft and sending.</summary>
public sealed partial class MainWindowViewModel
{
    public DestinationItemViewModel? SelectedDestination
    {
        get => _selectedDestination;
        set
        {
            if (SetProperty(ref _selectedDestination, value))
            {
                NotifyCommandStates();
            }
        }
    }

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
        private set => SetProperty(ref _draftOriginNotice, value);
    }

    public bool HasDraftEnvironmentMismatch =>
        IsConnected && _draftProfileId != _workspace.ConnectedProfileId;

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
        if (selectedItem.ProfileId is { } profileId && profileId != ConnectedProfileId)
        {
            throw new InvalidOperationException(
                "This message belongs to another environment. Connect to that environment before opening it as a draft.");
        }
        var selected = selectedItem.Message;
        var draft = selected.CreateDraft();
        _draftSourceMessage = selected;
        _draftSourceIsLocalBackup = isLocalBackup;
        BindDraftToConnectedEnvironment();
        DraftBody = draft.Body.Content;
        DraftBodyFormat = draft.Body.Format;
        DraftMessageId = draft.Properties.MessageId ?? Guid.NewGuid().ToString("N");
        DraftCorrelationId = draft.Properties.CorrelationId ?? string.Empty;
        DraftSubject = draft.Properties.Subject ?? string.Empty;
        DraftContentType = draft.Properties.ContentType ?? string.Empty;
        DraftSessionId = draft.Properties.SessionId ?? string.Empty;
        DraftTo = draft.Properties.To ?? string.Empty;
        DraftReplyTo = draft.Properties.ReplyTo ?? string.Empty;
        DraftReplyToSessionId = draft.Properties.ReplyToSessionId ?? string.Empty;
        DraftPartitionKey = draft.Properties.PartitionKey ?? string.Empty;
        DraftTransactionPartitionKey = draft.Properties.TransactionPartitionKey ?? string.Empty;
        DraftScheduledEnqueueTime = draft.Properties.ScheduledEnqueueTime?.ToString("O", CultureInfo.InvariantCulture)
            ?? string.Empty;
        DraftTimeToLiveSeconds = draft.Properties.TimeToLive?.TotalSeconds.ToString("0.###") ?? string.Empty;
        DraftApplicationProperties = ApplicationPropertiesJson.Serialize(draft.ApplicationProperties);

        var destination = selected.Source.Kind == ServiceBusEntityKind.Subscription
            ? ServiceBusEntityReference.Topic(selected.Source.TopicName!)
            : ServiceBusEntityReference.Queue(selected.Source.Name);
        SelectedDestination = Destinations.FirstOrDefault(item => item.Reference == destination);
        DraftOriginNotice = originNotice ?? (selected.IsDeadLetter
            ? "DLQ draft · resend sends a copy. Original remains in DLQ."
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
        var draft = BuildDraft();

        var warning = _draftSourceIsLocalBackup
            ? "Send a copy reconstructed from the local backup? The backup JSON remains unchanged."
            : _draftSourceMessage switch
        {
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
        if (_draftSourceMessage is not null)
        {
            warning += "\n\nThe original MessageId is currently preserved. With duplicate detection enabled, Azure may accept the send but suppress the duplicate; change MessageId when a distinct delivery is required.";
        }
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

        if (_draftSourceMessage is not null && _draftSourceMessage.IsDeadLetter)
        {
            await _workspace.ResubmitDeadLetterAsync(
                new ResubmitDeadLetterRequest(
                    _draftSourceMessage.Source,
                    _draftSourceMessage.SequenceNumber,
                    destination.Reference,
                    draft,
                    DeadLetterDisposition.KeepOriginal),
                cancellationToken).ConfigureAwait(true);
            StatusText = "Copy accepted by Service Bus · original remains in DLQ";
            AddActivity(
                "Success",
                "DLQ copy send accepted",
                $"{profile.Name} · {destination.Reference.DisplayName}",
                destination.Reference);
        }
        else
        {
            await _workspace.SendMessageAsync(
                new SendMessageRequest(destination.Reference, draft),
                cancellationToken).ConfigureAwait(true);
            StatusText = "Message accepted by Service Bus";
            AddActivity(
                "Success",
                "Message send accepted",
                $"{profile.Name} · {destination.Reference.DisplayName}",
                destination.Reference);
        }
    }

    private MessageDraft BuildDraft()
    {
        TimeSpan? timeToLive = null;
        if (!string.IsNullOrWhiteSpace(DraftTimeToLiveSeconds))
        {
            if (!double.TryParse(DraftTimeToLiveSeconds, out var seconds) || seconds <= 0)
            {
                throw new InvalidOperationException("TTL must be a positive number of seconds.");
            }
            timeToLive = TimeSpan.FromSeconds(seconds);
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
            MessageId = NullIfWhiteSpace(DraftMessageId) ?? Guid.NewGuid().ToString("N"),
            CorrelationId = NullIfWhiteSpace(DraftCorrelationId),
            ContentType = NullIfWhiteSpace(DraftContentType),
            Subject = NullIfWhiteSpace(DraftSubject),
            To = NullIfWhiteSpace(DraftTo),
            ReplyTo = NullIfWhiteSpace(DraftReplyTo),
            SessionId = NullIfWhiteSpace(DraftSessionId),
            ReplyToSessionId = NullIfWhiteSpace(DraftReplyToSessionId),
            PartitionKey = NullIfWhiteSpace(DraftPartitionKey),
            TransactionPartitionKey = NullIfWhiteSpace(DraftTransactionPartitionKey),
            TimeToLive = timeToLive,
            ScheduledEnqueueTime = scheduledEnqueueTime
        };
        return new MessageDraft(
            new EditableMessageBody(DraftBody ?? string.Empty, DraftBodyFormat),
            properties,
            applicationProperties);
    }

    private void BindDraftToConnectedEnvironment()
    {
        _draftProfileId = _workspace.ConnectedProfileId;
        _draftProfileName = _connectedProfile?.Name;
        OnPropertyChanged(nameof(HasDraftEnvironmentMismatch));
        OnPropertyChanged(nameof(DraftEnvironmentWarning));
        SendDraftCommand.NotifyCanExecuteChanged();
    }
}
