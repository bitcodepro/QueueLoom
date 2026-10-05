using System.Text.Json;
using System.Text.RegularExpressions;
using QueueLoom.Core.Routing;

namespace QueueLoom.App.ViewModels;

/// <summary>A filter kind offered when adding or changing a rule.</summary>
public sealed record RuleKindOption(RuleFilterKind Kind, string Label)
{
    public override string ToString() => Label;
}

/// <summary>An x-match mode of a RabbitMQ headers binding, as RabbitMQ spells it, with what it means.</summary>
public sealed record HeaderMatchOption(string Value, string Label)
{
    public override string ToString() => Label;
}

/// <summary>
/// Adds or changes one subscription rule: a Service Bus SQL or correlation rule, an SNS filter policy or a RabbitMQ
/// binding. Filters are read as they are typed, so mistakes show before the service is asked; the service still has
/// the last word when the rule is saved.
/// </summary>
public sealed partial class RuleEditorViewModel : ObservableObject
{
    private string _name;
    private RuleKindOption _kind;
    private string _sqlExpression;
    private string _action;
    private string _correlationId = string.Empty;
    private string _subject = string.Empty;
    private string _to = string.Empty;
    private string _replyTo = string.Empty;
    private string _sessionId = string.Empty;
    private string _contentType = string.Empty;
    private string _messageId = string.Empty;
    private string _replyToSessionId = string.Empty;
    private string _correlationProperties = string.Empty;
    private readonly Dictionary<string, (string Line, object Value)> _originalProperties = new(StringComparer.Ordinal);
    private string _error = string.Empty;
    private string _policy = string.Empty;
    private RuleKindOption _policyScope;
    private string _bindingKey = string.Empty;
    private HeaderMatchOption _headersMatch;
    private string _headers = string.Empty;
    private readonly SubscriptionRule? _existing;

    /// <param name="bindingKind">RabbitMQ: the kind of binding the exchange takes (all its bindings share it).</param>
    public RuleEditorViewModel(string topic, string subscription, SubscriptionRule? existing = null,
        RoutingService service = RoutingService.ServiceBus, RuleFilterKind? bindingKind = null)
    {
        Topic = topic;
        Subscription = subscription;
        Service = service;
        _existing = existing;
        BindingKind = existing?.IsBinding == true ? existing.Kind : bindingKind ?? RuleFilterKind.DirectBinding;
        IsNew = existing is null;
        _policyScope = PolicyScopes[existing?.OnMessageBody == true ? 1 : 0];
        _policy = existing?.Kind == RuleFilterKind.SnsFilterPolicy ? PrettyJson(existing.Expression ?? string.Empty) : string.Empty;
        _bindingKey = existing?.IsBinding == true ? existing.Expression ?? string.Empty : string.Empty;
        _headersMatch = HeaderModes.FirstOrDefault(mode => existing?.Arguments.TryGetValue("x-match", out var value) == true &&
                                                           value is string text && mode.Value == text)
                        ?? HeaderModes[0];
        if (existing?.Kind == RuleFilterKind.HeadersBinding)
        {
            _headers = string.Join(Environment.NewLine, existing.Arguments
                .Where(pair => pair.Key != "x-match")
                .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .Select(pair => Line(pair.Key, pair.Value!)));
        }
        _name = existing?.Name ?? string.Empty;
        _kind = Kinds.FirstOrDefault(kind => kind.Kind == existing?.Kind) ?? Kinds[0];
        _sqlExpression = existing?.Kind is RuleFilterKind.Sql or RuleFilterKind.True or RuleFilterKind.False
            ? existing.SqlExpression ?? string.Empty
            : string.Empty;
        _action = existing?.Action ?? string.Empty;
        if (existing?.Correlation is { } correlation)
        {
            _correlationId = correlation.CorrelationId ?? string.Empty;
            _subject = correlation.Subject ?? string.Empty;
            _to = correlation.To ?? string.Empty;
            _replyTo = correlation.ReplyTo ?? string.Empty;
            _sessionId = correlation.SessionId ?? string.Empty;
            _contentType = correlation.ContentType ?? string.Empty;
            _messageId = correlation.MessageId ?? string.Empty;
            _replyToSessionId = correlation.ReplyToSessionId ?? string.Empty;
            foreach (var (name, value) in correlation.Properties.OrderBy(pair => pair.Key, StringComparer.Ordinal))
            {
                _originalProperties[name] = (Line(name, value), value);
            }
            _correlationProperties = string.Join(Environment.NewLine, _originalProperties.Values.Select(original => original.Line));
        }
        if (existing?.Kind is RuleFilterKind.True or RuleFilterKind.False)
        {
            _kind = Kinds[0];
        }
    }

    public static IReadOnlyList<RuleKindOption> Kinds { get; } =
    [
        new(RuleFilterKind.Sql, "SQL filter"),
        new(RuleFilterKind.Correlation, "Correlation filter (exact matches)")
    ];

    /// <summary>Where an SNS filter policy looks: the message attributes or the JSON body.</summary>
    public static IReadOnlyList<RuleKindOption> PolicyScopes { get; } =
    [
        new(RuleFilterKind.SnsFilterPolicy, "Message attributes"),
        new(RuleFilterKind.SnsFilterPolicy, "Message body (JSON)")
    ];

    /// <summary>
    /// x-match of a RabbitMQ headers binding. "all" and "any" leave headers starting with "x-" out of the
    /// comparison; the "-with-x" forms compare them too.
    /// </summary>
    public static IReadOnlyList<HeaderMatchOption> HeaderModes { get; } =
    [
        new("all", "all (every header must match)"),
        new("any", "any (one header is enough)"),
        new("all-with-x", "all-with-x (every header, x- ones included)"),
        new("any-with-x", "any-with-x (one header, x- ones included)")
    ];

    public string Topic { get; }

    public string Subscription { get; }

    public RoutingService Service { get; }

    /// <summary>RabbitMQ: direct, topic, fanout or headers, as the exchange's type decides.</summary>
    public RuleFilterKind BindingKind { get; }

    public bool IsNew { get; }

    /// <summary>Something the operator should know before saving, such as a 1=1 rule that makes this one filter nothing.</summary>
    public string? Notice { get; init; }

    public bool HasNotice => !string.IsNullOrEmpty(Notice);

    public bool IsServiceBus => Service == RoutingService.ServiceBus;

    public bool IsSnsPolicy => Service == RoutingService.Sns;

    public bool IsBinding => Service == RoutingService.RabbitMq;

    public bool IsBindingKey => IsBinding && BindingKind is RuleFilterKind.DirectBinding or RuleFilterKind.TopicBinding or RuleFilterKind.OtherBinding;

    public bool IsHeadersBinding => IsBinding && BindingKind == RuleFilterKind.HeadersBinding;

    public bool IsFanoutBinding => IsBinding && BindingKind == RuleFilterKind.FanoutBinding;

    public string Title => Service switch
    {
        RoutingService.Sns => $"Filter policy · {Topic} / {Subscription}",
        RoutingService.RabbitMq => IsNew ? $"New binding · {Topic} → {Subscription}" : $"Binding · {Topic} → {Subscription}",
        _ => IsNew ? $"New rule · {Topic} / {Subscription}" : $"Rule {Name} · {Topic} / {Subscription}"
    };

    public string ConfirmLabel => Service switch
    {
        RoutingService.Sns => "Save policy",
        RoutingService.RabbitMq => IsNew ? "Add binding" : "Save binding",
        _ => IsNew ? "Add rule" : "Save rule"
    };

    public string BindingKeyLabel => BindingKind == RuleFilterKind.TopicBinding ? "BINDING PATTERN" : "BINDING KEY";

    public string BindingKeyHint => BindingKind switch
    {
        RuleFilterKind.TopicBinding =>
            "Words are separated by dots; * stands for exactly one word and # for any number, so order.* takes order.created but not order.eu.created, and order.# takes both. The routing key is the message's Subject.",
        RuleFilterKind.DirectBinding => "The routing key (the message's Subject) must equal this key exactly; it is case-sensitive.",
        _ => "The exchange comes from a plugin; RabbitMQ decides how it uses the key."
    };

    public string Policy
    {
        get => _policy;
        set
        {
            if (SetProperty(ref _policy, value ?? string.Empty))
            {
                OnPropertyChanged(nameof(PolicyCheck));
                OnPropertyChanged(nameof(HasPolicyProblem));
            }
        }
    }

    public RuleKindOption PolicyScope
    {
        get => _policyScope;
        set
        {
            if (SetProperty(ref _policyScope, value ?? PolicyScopes[0]))
            {
                OnPropertyChanged(nameof(PolicyCheck));
                OnPropertyChanged(nameof(HasPolicyProblem));
            }
        }
    }

    private bool PolicyOnBody => ReferenceEquals(PolicyScope, PolicyScopes[1]);

    /// <summary>What QueueLoom makes of the policy as it is typed.</summary>
    public string PolicyCheck
    {
        get
        {
            if (string.IsNullOrWhiteSpace(Policy))
            {
                return "For example: {\"region\": [\"EU\", \"US\"], \"amount\": [{\"numeric\": [\">\", 100]}]}. Every key must match, any one value of its list is enough.";
            }
            try
            {
                SnsFilterPolicy.Validate(Policy, PolicyOnBody);
                return "QueueLoom can read this policy. SNS checks it again when it is saved.";
            }
            catch (FilterPolicyException exception)
            {
                return exception.Message;
            }
        }
    }

    public bool HasPolicyProblem
    {
        get
        {
            if (string.IsNullOrWhiteSpace(Policy))
            {
                return false;
            }
            try
            {
                SnsFilterPolicy.Validate(Policy, PolicyOnBody);
                return false;
            }
            catch (FilterPolicyException)
            {
                return true;
            }
        }
    }

    public string BindingKey { get => _bindingKey; set => SetProperty(ref _bindingKey, value ?? string.Empty); }

    public HeaderMatchOption HeadersMatch { get => _headersMatch; set => SetProperty(ref _headersMatch, value ?? HeaderModes[0]); }

    /// <summary>One "name = value" per line: 'quoted' is text, 250 and 1.5 are numbers, true and false are booleans.</summary>
    public string Headers { get => _headers; set => SetProperty(ref _headers, value ?? string.Empty); }

    public string Name
    {
        get => _name;
        set => SetProperty(ref _name, value ?? string.Empty);
    }

    public RuleKindOption Kind
    {
        get => _kind;
        set
        {
            if (SetProperty(ref _kind, value ?? Kinds[0]))
            {
                OnPropertyChanged(nameof(IsSql));
                OnPropertyChanged(nameof(IsCorrelation));
            }
        }
    }

    public bool IsSql => IsServiceBus && Kind.Kind == RuleFilterKind.Sql;

    public bool IsCorrelation => IsServiceBus && Kind.Kind == RuleFilterKind.Correlation;

    public string SqlExpression
    {
        get => _sqlExpression;
        set
        {
            if (SetProperty(ref _sqlExpression, value ?? string.Empty))
            {
                OnPropertyChanged(nameof(SqlCheck));
                OnPropertyChanged(nameof(HasSqlProblem));
            }
        }
    }

    /// <summary>What QueueLoom makes of the filter as it is typed.</summary>
    public string SqlCheck
    {
        get
        {
            if (string.IsNullOrWhiteSpace(SqlExpression))
            {
                return "For example: region = 'EU' AND amount > 100, sys.Label LIKE 'order.%', tenant IN ('acme', 'globex'). Values are case-sensitive; property names are not.";
            }
            try
            {
                SqlFilter.Parse(SqlExpression);
                return "QueueLoom can read this filter. Service Bus checks it again when the rule is saved.";
            }
            catch (SqlFilterSyntaxException exception)
            {
                return $"At character {exception.Position + 1}: {exception.Message}";
            }
            catch (SqlFilterNotSupportedException exception)
            {
                return exception.Message;
            }
        }
    }

    public bool HasSqlProblem
    {
        get
        {
            try
            {
                SqlFilter.Parse(SqlExpression);
                return false;
            }
            catch (Exception exception) when (exception is SqlFilterSyntaxException)
            {
                return !string.IsNullOrWhiteSpace(SqlExpression);
            }
            catch (SqlFilterNotSupportedException)
            {
                return false;
            }
        }
    }

    public string Action
    {
        get => _action;
        set => SetProperty(ref _action, value ?? string.Empty);
    }

    public string CorrelationId { get => _correlationId; set => SetProperty(ref _correlationId, value ?? string.Empty); }

    public string Subject { get => _subject; set => SetProperty(ref _subject, value ?? string.Empty); }

    public string To { get => _to; set => SetProperty(ref _to, value ?? string.Empty); }

    public string ReplyTo { get => _replyTo; set => SetProperty(ref _replyTo, value ?? string.Empty); }

    public string SessionId { get => _sessionId; set => SetProperty(ref _sessionId, value ?? string.Empty); }

    public string ContentType { get => _contentType; set => SetProperty(ref _contentType, value ?? string.Empty); }

    public string MessageId { get => _messageId; set => SetProperty(ref _messageId, value ?? string.Empty); }

    public string ReplyToSessionId { get => _replyToSessionId; set => SetProperty(ref _replyToSessionId, value ?? string.Empty); }

    /// <summary>One "name = value" per line: 'quoted' is text, 250 and 1.5 are numbers, true and false are booleans.</summary>
    public string CorrelationProperties
    {
        get => _correlationProperties;
        set => SetProperty(ref _correlationProperties, value ?? string.Empty);
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

    public SubscriptionRule? TryBuild()
    {
        Error = string.Empty;
        switch (Service)
        {
            case RoutingService.Sns:
                return BuildPolicy();
            case RoutingService.RabbitMq:
                return BuildBinding();
        }
        var name = Name.Trim();
        if (name.Length is 0 or > 50 || !RuleName().IsMatch(name))
        {
            Error = "Give the rule a name of up to 50 letters, digits, '.', '-', '_' or '$'.";
            return null;
        }
        var action = string.IsNullOrWhiteSpace(Action) ? null : Action.Trim();
        if (IsSql)
        {
            if (string.IsNullOrWhiteSpace(SqlExpression))
            {
                Error = "Enter the SQL filter.";
                return null;
            }
            if (HasSqlProblem)
            {
                Error = SqlCheck;
                return null;
            }
            return new SubscriptionRule(name, RuleFilterKind.Sql, SqlExpression.Trim(), Action: action);
        }

        var properties = new Dictionary<string, object>(StringComparer.Ordinal);
        foreach (var line in CorrelationProperties.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var separator = line.IndexOf('=');
            if (separator <= 0)
            {
                Error = $"Write each property as name = value; '{line}' has no '='.";
                return null;
            }
            var property = line[..separator].Trim();
            // A line left as it was keeps the exact value Service Bus returned, whatever its type.
            try
            {
                properties[property] = _originalProperties.TryGetValue(property, out var original) && original.Line == line
                    ? original.Value
                    : RoutingValue.Parse(line[(separator + 1)..]);
            }
            catch (FormatException exception)
            {
                // A typo in a tagged value ("<Int32> nope", "<DateTime> soon") keeps the dialog open with the reason.
                Error = $"{property}: {exception.Message}";
                return null;
            }
        }
        if (properties.FirstOrDefault(pair => !RoutingValue.IsAllowedInCorrelationRule(pair.Value)) is { Key: not null } unsupported)
        {
            Error = $"{unsupported.Key}: Service Bus rules accept text, <Int32> or Int64 numbers, decimals (Double), true/false and " +
                    $"<DateTime> values, not a {unsupported.Value.GetType().Name}.";
            return null;
        }
        var fields = new CorrelationFilterFields(Blank(CorrelationId), Blank(MessageId), Blank(To), Blank(ReplyTo), Blank(Subject),
            Blank(SessionId), Blank(ReplyToSessionId), Blank(ContentType)) { Properties = properties };
        if (fields.IsEmpty)
        {
            Error = "Fill in at least one field or property: an empty correlation filter would take every message.";
            return null;
        }
        return new SubscriptionRule(name, RuleFilterKind.Correlation, Correlation: fields, Action: action);
    }

    private SubscriptionRule? BuildPolicy()
    {
        if (string.IsNullOrWhiteSpace(Policy))
        {
            Error = "Enter the filter policy. To let every message through, delete the policy instead.";
            return null;
        }
        try
        {
            SnsFilterPolicy.Validate(Policy, PolicyOnBody);
        }
        catch (FilterPolicyException exception)
        {
            Error = exception.Message;
            return null;
        }
        using var document = JsonDocument.Parse(Policy);
        return new SubscriptionRule(_existing?.Name ?? "FilterPolicy", RuleFilterKind.SnsFilterPolicy)
        {
            Expression = JsonSerializer.Serialize(document.RootElement, new JsonSerializerOptions { Encoder = ReadableJson }),
            OnMessageBody = PolicyOnBody,
            Title = "Filter policy"
        };
    }

    private SubscriptionRule? BuildBinding()
    {
        var arguments = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (IsHeadersBinding)
        {
            arguments["x-match"] = HeadersMatch.Value;
            foreach (var line in Headers.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                var separator = line.IndexOf('=');
                if (separator <= 0)
                {
                    Error = $"Write each header as name = value; '{line}' has no '='.";
                    return null;
                }
                var header = line[..separator].Trim();
                if (header == "x-match")
                {
                    Error = "Use the MATCH selector for x-match; do not list it as a header.";
                    return null;
                }
                if (arguments.ContainsKey(header))
                {
                    Error = $"Header '{header}' is duplicated. List each header once.";
                    return null;
                }
                object? value;
                try
                {
                    var text = line[(separator + 1)..].Trim();
                    // RabbitMQ's null binding argument checks presence, rather than the text "null".
                    value = text.Equals("null", StringComparison.OrdinalIgnoreCase) ? null : RoutingValue.Parse(text);
                }
                catch (FormatException exception)
                {
                    Error = $"{header}: {exception.Message}";
                    return null;
                }
                value = value switch
                {
                    int number => (long)number,
                    _ => value
                };
                if (value is not (null or string or long or double or bool))
                {
                    Error = $"{header}: a binding compares text, whole numbers, decimals and true/false, not a {value.GetType().Name}.";
                    return null;
                }
                arguments[header] = value;
            }
            if (arguments.Count == 1)
            {
                Error = "List at least one header: a headers binding without any takes every message.";
                return null;
            }
        }
        // A binding is named by RabbitMQ (its properties key); a change keeps the old name to remove the old binding.
        return new SubscriptionRule(_existing?.Name ?? string.Empty, BindingKind)
        {
            Expression = IsBindingKey ? BindingKey : string.Empty,
            Arguments = arguments
        };
    }

    // Policies hold comparisons such as ">"; they stay readable instead of becoming \u003E.
    private static readonly System.Text.Encodings.Web.JavaScriptEncoder ReadableJson = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping;

    private static string PrettyJson(string json)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            return JsonSerializer.Serialize(document.RootElement, new JsonSerializerOptions { WriteIndented = true, Encoder = ReadableJson });
        }
        catch (JsonException)
        {
            return json;
        }
    }

    /// <summary>Empty means "not checked"; anything else is kept exactly, since correlation fields match exactly.</summary>
    private static string? Blank(string value) => string.IsNullOrEmpty(value) ? null : value;

    private static string Line(string name, object value) => $"{name} = {RoutingValue.Format(value)}";

    [GeneratedRegex(@"^[A-Za-z0-9$._-]+$")]
    private static partial Regex RuleName();
}
