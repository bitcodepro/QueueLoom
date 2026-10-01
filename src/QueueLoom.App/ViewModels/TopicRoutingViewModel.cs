using System.Collections.ObjectModel;
using System.Globalization;
using QueueLoom.App.Commands;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>What the routing window may do: read and change rules, and ask the operator.</summary>
public sealed record TopicRoutingServices(
    Func<CancellationToken, Task<IReadOnlyList<SubscriptionRules>>> LoadRules,
    Func<string, SubscriptionRule, bool, CancellationToken, Task> SaveRule,
    Func<string, string, CancellationToken, Task> DeleteRule,
    Func<RuleEditorViewModel, Task<SubscriptionRule?>> EditRule,
    Func<string, string, string?, Task<bool>> Confirm);

public sealed class RuleItemViewModel(SubscriptionRule rule)
{
    public SubscriptionRule Rule { get; } = rule;

    public string Name => Rule.DisplayName;

    /// <summary>A fanout binding has nothing to change; it can only be removed.</summary>
    public bool CanChange => Rule.Kind is not (RuleFilterKind.FanoutBinding or RuleFilterKind.OtherBinding);

    public string KindLabel => Rule.KindLabel;

    public string Filter => Rule.FilterText;

    public string? Action => Rule.Action;

    public bool HasAction => !string.IsNullOrWhiteSpace(Rule.Action);

    public string ActionText => $"Then: {Rule.Action}";
}

/// <summary>A subscription of the topic, its rules, and what the last check found for it.</summary>
public sealed class RoutingSubscriptionViewModel(SubscriptionRules rules) : ObservableObject
{
    private SubscriptionRouting? _result;

    public SubscriptionRules Source { get; } = rules;

    public string Name => Source.Subscription;

    public IReadOnlyList<RuleItemViewModel> Rules { get; } = rules.Rules.Select(rule => new RuleItemViewModel(rule)).ToArray();

    public string RulesText => Source.IsFallback
        ? "alternate exchange"
        : (Source.Service, Source.Rules.Count) switch
        {
            (RoutingService.Sns, 0) => "no filter policy · receives every message",
            (RoutingService.PubSub, 0) => "no filter · receives every message",
            (RoutingService.Sns or RoutingService.PubSub, _) => Source.Rules[0].KindLabel,
            (RoutingService.RabbitMq, 1) => $"1 binding · {Source.Rules[0].KindLabel}",
            (RoutingService.RabbitMq, var count) => $"{count} bindings",
            (_, 0) => "no rules",
            (_, 1) => $"1 rule · {Source.Rules[0].KindLabel}",
            (_, var count) => $"{count} rules"
        };

    public string? Note => Source.Note;

    public bool HasNote => !string.IsNullOrEmpty(Note);

    public string? Warning => Source.Warning;

    public bool HasWarning => Warning is not null;

    public SubscriptionRouting? Result
    {
        get => _result;
        set
        {
            if (SetProperty(ref _result, value))
            {
                OnPropertyChanged(nameof(HasResult));
                OnPropertyChanged(nameof(Receives));
                OnPropertyChanged(nameof(Skips));
                OnPropertyChanged(nameof(IsUnknown));
                OnPropertyChanged(nameof(ResultLabel));
                OnPropertyChanged(nameof(ResultText));
                OnPropertyChanged(nameof(HasResultText));
            }
        }
    }

    public bool HasResult => Result is not null;

    public bool Receives => Result?.Outcome == RoutingOutcome.Receives;

    public bool Skips => Result?.Outcome == RoutingOutcome.Skips;

    public bool IsUnknown => Result?.Outcome == RoutingOutcome.Unknown;

    public string ResultLabel => Result?.Outcome switch
    {
        RoutingOutcome.Receives => "RECEIVES",
        RoutingOutcome.Skips => "SKIPS",
        RoutingOutcome.Unknown => $"{Source.Service.Name().ToUpperInvariant()} DECIDES",
        _ => string.Empty
    };

    /// <summary>Why it receives the message or not; empty when the warning above already says it.</summary>
    public string ResultText => Result is null || Result.Summary == Warning ? string.Empty : Result.Summary;

    public bool HasResultText => ResultText.Length > 0;
}

/// <summary>
/// The subscriptions of one topic with their rules, and a test: which of them would receive a given message and
/// why the others would not. Rules can be added, changed and deleted when queue management and write access allow it.
/// </summary>
public sealed class TopicRoutingViewModel : ObservableObject
{
    private readonly TopicRoutingServices _services;
    private RoutingSubscriptionViewModel? _selected;
    private string _headline = string.Empty;
    private bool _isDropped;
    private bool _isBusy;
    private string _error = string.Empty;
    private string _testSubject;
    private string _testCorrelationId;
    private string _testMessageId;
    private string _testTo;
    private string _testReplyTo;
    private string _testSessionId;
    private string _testContentType;
    private string _testProperties;
    private string _testBody;
    private readonly EditableMessageProperties _baseProperties;
    private readonly Dictionary<string, (string Line, object? Value)> _originalProperties = new(StringComparer.Ordinal);
    private bool _loaded;

    public TopicRoutingViewModel(string topic, string environmentName, bool canEdit, string editHint, TopicRoutingServices services,
        MessageDraft? message = null, string? messageOrigin = null, RoutingService service = RoutingService.ServiceBus)
    {
        Topic = topic;
        Service = service;
        _testBody = message?.Body is { Format: not MessageBodyFormat.Base64 } body ? body.Content : string.Empty;
        EnvironmentName = environmentName;
        CanEdit = canEdit;
        EditHint = editHint;
        _services = services;
        MessageOrigin = messageOrigin;
        var properties = message?.Properties ?? EditableMessageProperties.Empty;
        // Fields the window does not show (ReplyToSessionId, PartitionKey…) are kept for the check as they are.
        _baseProperties = properties;
        _testSubject = properties.Subject ?? string.Empty;
        _testCorrelationId = properties.CorrelationId ?? string.Empty;
        _testMessageId = properties.MessageId ?? string.Empty;
        _testTo = properties.To ?? string.Empty;
        _testReplyTo = properties.ReplyTo ?? string.Empty;
        _testSessionId = properties.SessionId ?? string.Empty;
        _testContentType = properties.ContentType ?? string.Empty;
        // Each property keeps its exact typed value while its line is left as it is; only edited lines are read back.
        foreach (var property in message?.ApplicationProperties ?? [])
        {
            var value = RoutingMessage.Typed(property);
            _originalProperties[property.Name] = (Line(property.Name, value), value);
        }
        _testProperties = string.Join(Environment.NewLine, _originalProperties.Values.Select(original => original.Line));
        CheckCommand = new RelayCommand(Check, () => !IsBusy && _loaded);
        RefreshCommand = new AsyncRelayCommand(LoadAsync, () => !IsBusy);
        AddRuleCommand = new AsyncRelayCommand(AddRuleAsync, () => !IsBusy && CanEdit && CanAddTo(Selected));
        EditRuleCommand = new AsyncRelayCommand<RuleItemViewModel>(EditRuleAsync, rule => !IsBusy && CanEdit && rule?.CanChange == true);
        DeleteRuleCommand = new AsyncRelayCommand<RuleItemViewModel>(DeleteRuleAsync, rule => !IsBusy && CanEdit && rule is not null);
    }

    public string Topic { get; }

    public string EnvironmentName { get; }

    public RoutingService Service { get; }

    public string Title => Service == RoutingService.RabbitMq ? $"Bindings and routing · {Topic}" : $"Rules and routing · {Topic}";

    /// <summary>How this service's rules decide where a message goes.</summary>
    public string Explanation => Service.Explanation();

    public string ListTitle => Service switch
    {
        RoutingService.RabbitMq => "Queues and exchanges bound to it",
        RoutingService.Sns => "Subscriptions and filter policies",
        RoutingService.PubSub => "Subscriptions and filters",
        _ => "Subscriptions and rules"
    };

    public string AddRuleLabel => Service switch
    {
        RoutingService.RabbitMq => "Add binding…",
        RoutingService.Sns => "Add filter policy…",
        _ => "Add rule…"
    };

    /// <summary>In RabbitMQ the routing key is what bindings look at; QueueLoom sends Subject as the routing key.</summary>
    public string SubjectLabel => Service == RoutingService.RabbitMq ? "ROUTING KEY (SUBJECT)" : "SUBJECT (LABEL)";

    public string PropertiesLabel => Service switch
    {
        RoutingService.RabbitMq => "HEADERS · ONE PER LINE",
        RoutingService.Sns or RoutingService.PubSub => "MESSAGE ATTRIBUTES · ONE PER LINE",
        _ => "APPLICATION PROPERTIES · ONE PER LINE"
    };

    /// <summary>SNS policies can look at the JSON body, so the test message has one there.</summary>
    public bool ShowsBody => Service == RoutingService.Sns;

    /// <summary>The test message's body, for SNS policies with the MessageBody scope.</summary>
    public string TestBody { get => _testBody; set => SetProperty(ref _testBody, value ?? string.Empty); }

    public bool CanEdit { get; }

    public string EditHint { get; }

    /// <summary>Where the test message came from, for example "Dead letter order-1042 from orders / billing".</summary>
    public string? MessageOrigin { get; }

    public bool HasMessageOrigin => MessageOrigin is not null;

    public ObservableCollection<RoutingSubscriptionViewModel> Subscriptions { get; } = [];

    public RoutingSubscriptionViewModel? Selected
    {
        get => _selected;
        set
        {
            if (SetProperty(ref _selected, value))
            {
                OnPropertyChanged(nameof(HasSelection));
                OnPropertyChanged(nameof(AddRuleToolTip));
                AddRuleCommand.NotifyCanExecuteChanged();
            }
        }
    }

    public bool HasSelection => Selected is not null;

    public string AddRuleToolTip => Service switch
    {
        RoutingService.Sns when Selected?.Rules.Count > 0 => "This subscription has a filter policy; edit it instead",
        RoutingService.Sns => "Add a filter policy to the selected subscription",
        RoutingService.RabbitMq => "Bind the selected queue or exchange once more, with another key or headers",
        _ => "Add a rule to the selected subscription"
    };

    /// <summary>SNS allows one policy per subscription; an alternate exchange has no bindings to add to.</summary>
    private bool CanAddTo(RoutingSubscriptionViewModel? subscription) => subscription is not null && Service switch
    {
        RoutingService.Sns => subscription.Rules.Count == 0 && subscription.Source.Problem is null,
        RoutingService.RabbitMq => !subscription.Source.IsFallback,
        RoutingService.PubSub => false,
        _ => true
    };

    public RelayCommand CheckCommand { get; }

    public AsyncRelayCommand RefreshCommand { get; }

    public AsyncRelayCommand AddRuleCommand { get; }

    public AsyncRelayCommand<RuleItemViewModel> EditRuleCommand { get; }

    public AsyncRelayCommand<RuleItemViewModel> DeleteRuleCommand { get; }

    public string TestSubject { get => _testSubject; set => SetProperty(ref _testSubject, value ?? string.Empty); }

    public string TestCorrelationId { get => _testCorrelationId; set => SetProperty(ref _testCorrelationId, value ?? string.Empty); }

    public string TestMessageId { get => _testMessageId; set => SetProperty(ref _testMessageId, value ?? string.Empty); }

    public string TestTo { get => _testTo; set => SetProperty(ref _testTo, value ?? string.Empty); }

    public string TestReplyTo { get => _testReplyTo; set => SetProperty(ref _testReplyTo, value ?? string.Empty); }

    public string TestSessionId { get => _testSessionId; set => SetProperty(ref _testSessionId, value ?? string.Empty); }

    public string TestContentType { get => _testContentType; set => SetProperty(ref _testContentType, value ?? string.Empty); }

    /// <summary>One "name = value" per line: 'quoted' is text, 42 and 1.5 are numbers, true and false are booleans.</summary>
    public string TestProperties { get => _testProperties; set => SetProperty(ref _testProperties, value ?? string.Empty); }

    public string Headline
    {
        get => _headline;
        private set
        {
            if (SetProperty(ref _headline, value))
            {
                OnPropertyChanged(nameof(HasHeadline));
            }
        }
    }

    public bool HasHeadline => Headline.Length > 0;

    public bool IsDropped
    {
        get => _isDropped;
        private set => SetProperty(ref _isDropped, value);
    }

    public bool IsBusy
    {
        get => _isBusy;
        private set
        {
            if (SetProperty(ref _isBusy, value))
            {
                CheckCommand?.NotifyCanExecuteChanged();
                RefreshCommand.NotifyCanExecuteChanged();
                AddRuleCommand.NotifyCanExecuteChanged();
                EditRuleCommand.NotifyCanExecuteChanged();
                DeleteRuleCommand.NotifyCanExecuteChanged();
            }
        }
    }

    public string Error
    {
        get => _error;
        private set
        {
            if (SetProperty(ref _error, value))
            {
                OnPropertyChanged(nameof(HasError));
            }
        }
    }

    public bool HasError => Error.Length > 0;

    public int WarningCount => Subscriptions.Count(subscription => subscription.HasWarning);

    public string SubscriptionsCaption => Subscriptions.Count == 0
        ? Service == RoutingService.RabbitMq
            ? "No bindings: every message sent to this exchange is dropped."
            : "No subscriptions: every message sent to this topic is dropped."
        : $"{Subscriptions.Count} {Service.SubscriptionWord()}(s)" + (WarningCount > 0 ? $" · {WarningCount} receive nothing" : string.Empty);

    public async Task LoadAsync(CancellationToken cancellationToken = default)
    {
        var checkAfterwards = HasHeadline || HasMessageOrigin;
        IsBusy = true;
        Error = string.Empty;
        _loaded = false;
        try
        {
            var selected = Selected?.Name;
            var rules = await _services.LoadRules(cancellationToken).ConfigureAwait(true);
            Subscriptions.Clear();
            Headline = string.Empty;
            IsDropped = false;
            foreach (var subscription in rules)
            {
                Subscriptions.Add(new RoutingSubscriptionViewModel(subscription));
            }
            Selected = Subscriptions.FirstOrDefault(item => item.Name == selected) ?? Subscriptions.FirstOrDefault();
            OnPropertyChanged(nameof(WarningCount));
            OnPropertyChanged(nameof(SubscriptionsCaption));
            _loaded = true;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // Without the rules nothing can be said about routing: an old result or an empty list would read as
            // "no subscription takes it", so both are cleared and only the error is shown.
            Subscriptions.Clear();
            Selected = null;
            Headline = string.Empty;
            IsDropped = false;
            OnPropertyChanged(nameof(WarningCount));
            OnPropertyChanged(nameof(SubscriptionsCaption));
            Error = $"The rules of {Topic} could not be read: {exception.Message}";
        }
        finally
        {
            IsBusy = false;
        }
        if (_loaded && checkAfterwards)
        {
            Check();
        }
    }

    /// <summary>Works out, subscription by subscription, whether the test message would be copied there.</summary>
    public void Check()
    {
        if (!_loaded)
        {
            return;
        }
        Error = string.Empty;
        IReadOnlyList<KeyValuePair<string, object?>> properties;
        try
        {
            properties = ParseProperties(TestProperties, _originalProperties);
        }
        catch (FormatException exception)
        {
            Error = exception.Message;
            return;
        }
        var message = new RoutingMessage(_baseProperties with
        {
            MessageId = Blank(TestMessageId),
            CorrelationId = Blank(TestCorrelationId),
            ContentType = Blank(TestContentType),
            Subject = Blank(TestSubject),
            To = Blank(TestTo),
            ReplyTo = Blank(TestReplyTo),
            SessionId = Blank(TestSessionId)
        }, properties) { Body = TestBody };
        var result = TopicRouting.Route(Topic, Subscriptions.Select(item => item.Source).ToArray(), message, Service);
        foreach (var subscription in Subscriptions)
        {
            subscription.Result = result.Subscriptions.FirstOrDefault(item => item.Subscription == subscription.Name);
        }
        Headline = result.Headline;
        IsDropped = result.IsDropped;
    }

    private async Task AddRuleAsync(CancellationToken cancellationToken)
    {
        var subscription = Selected ?? throw new InvalidOperationException("Select a subscription first.");
        var rule = await _services.EditRule(new RuleEditorViewModel(Topic, subscription.Name, null, Service, BindingKind)).ConfigureAwait(true);
        if (rule is not null)
        {
            await ChangeAsync(token => _services.SaveRule(subscription.Name, rule, false, token), cancellationToken).ConfigureAwait(true);
        }
    }

    private async Task EditRuleAsync(RuleItemViewModel? item, CancellationToken cancellationToken)
    {
        var subscription = SubscriptionOf(item);
        var rule = await _services.EditRule(new RuleEditorViewModel(Topic, subscription.Name, item!.Rule, Service, BindingKind)).ConfigureAwait(true);
        if (rule is not null)
        {
            await ChangeAsync(token => _services.SaveRule(subscription.Name, rule, true, token), cancellationToken).ConfigureAwait(true);
        }
    }

    private async Task DeleteRuleAsync(RuleItemViewModel? item, CancellationToken cancellationToken)
    {
        var subscription = SubscriptionOf(item);
        var last = subscription.Rules.Count == 1;
        var (title, consequence, typeName) = Service switch
        {
            RoutingService.Sns => ("Delete the filter policy",
                "Without a filter policy the subscription receives every message sent to the topic.", (string?)subscription.Name),
            RoutingService.RabbitMq => ($"Delete binding {item!.Name}", last
                ? $"This is the only binding of {subscription.Name} to {Topic}: without it, it gets nothing from this exchange."
                : $"Messages that only this binding let in will no longer reach {subscription.Name}.", last ? subscription.Name : null),
            _ => ($"Delete rule {item!.Name}", last
                ? "This is the subscription's only rule. Without it the subscription receives no messages at all until a rule is added."
                : "Messages that only this rule let in will no longer reach the subscription.", last ? subscription.Name : null)
        };
        var confirmed = await _services.Confirm(
            title,
            $"Environment: {EnvironmentName}\n{(Service == RoutingService.RabbitMq ? "Destination" : "Subscription")}: {Topic} / {subscription.Name}\n" +
            $"Filter: {item!.Filter}\n\n{consequence}",
            typeName).ConfigureAwait(true);
        if (confirmed)
        {
            await ChangeAsync(token => _services.DeleteRule(subscription.Name, item.Rule.Name, token), cancellationToken).ConfigureAwait(true);
        }
    }

    /// <summary>RabbitMQ: every binding of an exchange is of the exchange's kind; a new one takes the kind of the others.</summary>
    private RuleFilterKind? BindingKind => Subscriptions.SelectMany(item => item.Source.Rules).FirstOrDefault(rule => rule.IsBinding)?.Kind;

    private RoutingSubscriptionViewModel SubscriptionOf(RuleItemViewModel? item) =>
        Subscriptions.FirstOrDefault(subscription => item is not null && subscription.Rules.Contains(item))
        ?? throw new InvalidOperationException("The rule is no longer listed. Refresh and try again.");

    private async Task ChangeAsync(Func<CancellationToken, Task> change, CancellationToken cancellationToken)
    {
        IsBusy = true;
        Error = string.Empty;
        try
        {
            await change(cancellationToken).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            Error = exception.Message;
            IsBusy = false;
            return;
        }
        IsBusy = false;
        await LoadAsync(cancellationToken).ConfigureAwait(true);
    }

    private static string Line(string name, object? value) => $"{name} = {RoutingValue.Format(value)}";

    /// <summary>
    /// Reads "name = value" lines: 'quoted' text (\n for a line break), whole or decimal numbers, true or false, and
    /// tagged values such as &lt;Guid&gt; …; anything else is text. A line identical to one in <paramref name="originals"/>
    /// keeps that property's original value and type.
    /// </summary>
    public static IReadOnlyList<KeyValuePair<string, object?>> ParseProperties(string text,
        IReadOnlyDictionary<string, (string Line, object? Value)>? originals = null)
    {
        var result = new List<KeyValuePair<string, object?>>();
        foreach (var line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var separator = line.IndexOf('=');
            if (separator <= 0)
            {
                throw new FormatException($"Write each property as name = value; '{line}' has no '='.");
            }
            var name = line[..separator].Trim();
            result.Add(new(name, originals is not null && originals.TryGetValue(name, out var original) && original.Line == line
                ? original.Value
                : RoutingValue.Parse(line[(separator + 1)..])));
        }
        return result;
    }

    /// <summary>Empty means the message has no such property; anything else is kept exactly, spaces included.</summary>
    private static string? Blank(string value) => string.IsNullOrEmpty(value) ? null : value;
}
