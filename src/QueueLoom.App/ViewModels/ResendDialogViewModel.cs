using System.Globalization;
using System.Text.RegularExpressions;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>A destination choice in the resend dialog; no reference means "back to where each message came from".</summary>
public sealed record ResendDestinationOption(string Label, ServiceBusEntityReference? Reference);

/// <param name="Rewrite">Find and replace applied to every message before it is sent; null for none.</param>
/// <param name="SendAt">When to send; null sends now.</param>
public sealed record ResendOptions(
    ServiceBusEntityReference? Destination,
    ResendMode Mode,
    int MessagesPerSecond,
    MessageRewrite? Rewrite = null,
    DateTimeOffset? SendAt = null,
    bool PreserveMessageIds = false);

/// <summary>Options for resending the ticked messages: destination, copy or move, and pace.</summary>
public sealed class ResendDialogViewModel : ObservableObject
{
    public const int DefaultMessagesPerSecond = 10;

    private ResendDestinationOption _destination;
    private bool _moves;
    private int _messagesPerSecond = DefaultMessagesPerSecond;
    private string _typedConfirmation = string.Empty;
    private string _findText = string.Empty;
    private string _replaceText = string.Empty;
    private bool _replaceInBody = true;
    private bool _replaceInProperties;
    private bool _matchCase = true;
    private bool _sendLater;
    private bool _preserveMessageIds;
    private string _sendAtText = "30m";
    private readonly Func<DateTimeOffset> _now;

    public ResendDialogViewModel(
        IReadOnlyList<BrowsedMessage> messages,
        IEnumerable<ServiceBusEntityReference> destinations,
        string environmentName,
        bool requiresTypedConfirmation,
        bool canRemoveOriginals = true,
        Func<DateTimeOffset>? now = null,
        bool requiresNewIdsForMove = false)
    {
        _now = now ?? (() => DateTimeOffset.Now);
        CanRemoveOriginals = canRemoveOriginals;
        RequiresNewIdsForMove = requiresNewIdsForMove;
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            throw new ArgumentException("Choose at least one message.", nameof(messages));
        }

        Messages = messages;
        EnvironmentName = environmentName;
        RequiresTypedConfirmation = requiresTypedConfirmation;
        DestinationOptions =
        [
            new ResendDestinationOption("Back to where each message came from", null),
            .. destinations.Select(reference => new ResendDestinationOption(
                $"{(reference.Kind == ServiceBusEntityKind.Topic ? "Topic" : "Queue")} · {reference.DisplayName}", reference))
        ];
        _destination = DestinationOptions[0];
    }

    public IReadOnlyList<BrowsedMessage> Messages { get; }

    public string EnvironmentName { get; }
    public bool RequiresNewIdsForMove { get; }
    public bool PreserveMessageIds
    {
        get => _preserveMessageIds;
        set
        {
            if (SetProperty(ref _preserveMessageIds, value))
            {
                OnPropertyChanged(nameof(Summary));
                OnPropertyChanged(nameof(CanConfirm));
            }
        }
    }

    public string Title => Messages.Count == 1 ? "Resend 1 message" : $"Resend {Messages.Count:N0} messages";

    public IReadOnlyList<ResendDestinationOption> DestinationOptions { get; }

    public ResendDestinationOption Destination
    {
        get => _destination;
        set
        {
            if (value is not null && SetProperty(ref _destination, value))
            {
                OnPropertyChanged(nameof(Summary));
            }
        }
    }

    /// <summary>False where the service cannot remove single messages (Kafka).</summary>
    public bool CanRemoveOriginals { get; }

    /// <summary>Only dead-lettered messages can be removed after sending, and only where the service allows it.</summary>
    public bool CanMove => CanRemoveOriginals && Messages.All(message => message.IsDeadLetter);

    public string MoveUnavailableReason => CanRemoveOriginals
        ? "Only dead-lettered messages can be moved; untick active messages to enable it."
        : "Kafka keeps every message until its retention ends, so originals cannot be removed one by one.";

    public bool Moves
    {
        get => _moves;
        set
        {
            if (SetProperty(ref _moves, value && CanMove))
            {
                OnPropertyChanged(nameof(Copies));
                OnPropertyChanged(nameof(Summary));
                OnPropertyChanged(nameof(ConfirmLabel));
                OnPropertyChanged(nameof(CanConfirm));
            }
        }
    }

    public bool Copies
    {
        get => !Moves;
        set => Moves = !value;
    }

    public int MessagesPerSecond
    {
        get => _messagesPerSecond;
        set => SetProperty(ref _messagesPerSecond, Math.Clamp(value, 1, 100));
    }

    public string FindText
    {
        get => _findText;
        set => SetRewrite(ref _findText, value ?? string.Empty);
    }

    public string ReplaceText
    {
        get => _replaceText;
        set => SetRewrite(ref _replaceText, value ?? string.Empty);
    }

    public bool ReplaceInBody
    {
        get => _replaceInBody;
        set => SetRewrite(ref _replaceInBody, value);
    }

    public bool ReplaceInProperties
    {
        get => _replaceInProperties;
        set => SetRewrite(ref _replaceInProperties, value);
    }

    public bool MatchCase
    {
        get => _matchCase;
        set => SetRewrite(ref _matchCase, value);
    }

    /// <summary>The find and replace to apply, or null when nothing is to be replaced.</summary>
    public MessageRewrite? Rewrite
    {
        get
        {
            var rewrite = new MessageRewrite(FindText, ReplaceText, ReplaceInBody, ReplaceInProperties, MatchCase);
            return rewrite.IsEmpty ? null : rewrite;
        }
    }

    /// <summary>"Changes 3 of 5 messages", with a warning when a JSON body would no longer be valid JSON.</summary>
    public string RewritePreview
    {
        get
        {
            if (Rewrite is not { } rewrite)
            {
                return "Optional. Replaces text in the bodies and text properties of every message before it is sent.";
            }
            var changed = 0;
            var broken = 0;
            foreach (var message in Messages.Where(message => !message.IsBodyTruncated))
            {
                var draft = message.CreateDraft();
                var after = rewrite.Apply(draft, out var didChange);
                changed += didChange ? 1 : 0;
                broken += MessageRewrite.BreaksJson(draft, after) ? 1 : 0;
            }
            return $"Changes {changed:N0} of {Messages.Count:N0} messages." +
                   (broken > 0 ? $" {broken:N0} JSON bodies would no longer be valid JSON." : string.Empty);
        }
    }

    public bool SendNow
    {
        get => !_sendLater;
        set => SendLater = !value;
    }

    public bool SendLater
    {
        get => _sendLater;
        set
        {
            if (SetProperty(ref _sendLater, value))
            {
                OnPropertyChanged(nameof(SendNow));
                NotifySchedule();
            }
        }
    }

    /// <summary>"30m", "2h", "03:00" (the next time the clock shows it) or a date and time.</summary>
    public string SendAtText
    {
        get => _sendAtText;
        set
        {
            if (SetProperty(ref _sendAtText, value ?? string.Empty))
            {
                NotifySchedule();
            }
        }
    }

    public DateTimeOffset? SendAt => SendLater ? ParseWhen(SendAtText, _now()) : null;

    public string SendAtDescription => !SendLater
        ? string.Empty
        : SendAt is { } at
            ? $"Sends {at.ToLocalTime():ddd d MMM HH:mm} ({Until(at - _now())}). QueueLoom must be open then, connected to {EnvironmentName} with write access on; until then the resend waits. It is listed on Activity, where it can be run early or cancelled."
            : "Enter how long to wait (30m, 2h), a time (03:00) or a date and time (2026-10-01 03:00).";

    public bool RequiresTypedConfirmation { get; }

    public string TypedConfirmation
    {
        get => _typedConfirmation;
        set
        {
            if (SetProperty(ref _typedConfirmation, value))
            {
                OnPropertyChanged(nameof(CanConfirm));
            }
        }
    }

    public bool CanConfirm => (!RequiresTypedConfirmation ||
                               string.Equals(TypedConfirmation.Trim(), EnvironmentName, StringComparison.Ordinal)) &&
                               (!SendLater || SendAt is not null) && (!Moves || !RequiresNewIdsForMove || !PreserveMessageIds);

    public string ConfirmLabel => (SendLater, Moves) switch
    {
        (true, true) => "Schedule the move",
        (true, false) => "Schedule copies",
        (false, true) => "Send and remove originals",
        _ => "Send copies"
    };

    public string Summary
    {
        get
        {
            var sources = Messages
                .GroupBy(message => message.Source.DisplayName)
                .Select(group => $"• {group.Key}: {group.Count():N0}")
                .Take(8);
            var targets = Destination.Reference is { } reference
                ? [reference]
                : Messages.Select(message => DeadLetterResender.OriginalDestination(message.Source)).Distinct().ToArray();
            var fanOut = targets.Any(target => target.Kind == ServiceBusEntityKind.Topic)
                ? "\n\nA topic delivers each copy to every subscription whose filter matches, not only to the one it came from."
                : string.Empty;
            var mode = Moves
                ? "Every message is sent first. Then the originals that were sent are backed up and removed from their " +
                  "dead-letter queues. If a send fails, that original stays where it is."
                : "Copies are sent; the originals stay where they are.";
            var rewrite = Rewrite is null ? string.Empty : $"\n\nFind and replace: {RewritePreview}";
            var ids = PreserveMessageIds
                ? "\n\nMessage IDs are preserved. Duplicate detection may accept the send but suppress delivery. Azure and SQS/SNS FIFO moves require new IDs; use copy to preserve IDs."
                : "\n\nEvery copy receives a distinct new Message ID, assigned before sending and retained for retries and scheduled sends.";
            return $"Environment: {EnvironmentName}\n{string.Join("\n", sources)}\n\n{mode}{ids}{fanOut}{rewrite}";
        }
    }

    public ResendOptions ToOptions() =>
        new(Destination.Reference, Moves ? ResendMode.Move : ResendMode.Copy, MessagesPerSecond, Rewrite, SendAt, PreserveMessageIds);

    /// <summary>Parses "30m", "2h", "1d", "03:00" (the next time the clock shows it) or a date and time.</summary>
    public static DateTimeOffset? ParseWhen(string? text, DateTimeOffset now)
    {
        var value = text?.Trim() ?? string.Empty;
        var relative = Regex.Match(value, @"^(\d+)\s*(m|min|h|d)$", RegexOptions.IgnoreCase);
        if (relative.Success)
        {
            var amount = int.Parse(relative.Groups[1].Value, CultureInfo.InvariantCulture);
            var span = relative.Groups[2].Value.ToLowerInvariant() switch
            {
                "h" => TimeSpan.FromHours(amount),
                "d" => TimeSpan.FromDays(amount),
                _ => TimeSpan.FromMinutes(amount)
            };
            return amount > 0 && span <= TimeSpan.FromDays(30) ? now + span : null;
        }
        if (TimeOnly.TryParseExact(value, ["H:mm", "HH:mm"], CultureInfo.InvariantCulture, DateTimeStyles.None, out var clock))
        {
            var local = now.ToLocalTime();
            var today = new DateTimeOffset(local.Date + clock.ToTimeSpan(), local.Offset);
            return today > local ? today : today.AddDays(1);
        }
        if (DateTimeOffset.TryParse(value, CultureInfo.CurrentCulture, DateTimeStyles.AssumeLocal, out var at) ||
            DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out at))
        {
            return at > now && at - now <= TimeSpan.FromDays(30) ? at : null;
        }
        return null;
    }

    private static string Until(TimeSpan span) => span.TotalMinutes < 60
        ? $"in {Math.Max(1, (int)Math.Round(span.TotalMinutes))} min"
        : span.TotalHours < 48
            ? $"in {(int)span.TotalHours} h {span.Minutes} min"
            : $"in {(int)span.TotalDays} days";

    private void NotifySchedule()
    {
        OnPropertyChanged(nameof(SendAt));
        OnPropertyChanged(nameof(SendAtDescription));
        OnPropertyChanged(nameof(CanConfirm));
        OnPropertyChanged(nameof(ConfirmLabel));
    }

    private void SetRewrite<T>(ref T field, T value, [System.Runtime.CompilerServices.CallerMemberName] string? propertyName = null)
    {
        if (SetProperty(ref field, value, propertyName))
        {
            OnPropertyChanged(nameof(Rewrite));
            OnPropertyChanged(nameof(RewritePreview));
            OnPropertyChanged(nameof(Summary));
        }
    }
}
