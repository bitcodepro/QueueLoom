using System.Text.RegularExpressions;
using QueueLoom.Core.Routing;

namespace QueueLoom.App.ViewModels;

/// <summary>A filter kind offered when adding or changing a rule.</summary>
public sealed record RuleKindOption(RuleFilterKind Kind, string Label)
{
    public override string ToString() => Label;
}

/// <summary>
/// Adds or changes one subscription rule. The SQL filter is read as it is typed, so mistakes show before Service Bus
/// is asked; Service Bus still has the last word when the rule is saved.
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

    public RuleEditorViewModel(string topic, string subscription, SubscriptionRule? existing = null)
    {
        Topic = topic;
        Subscription = subscription;
        IsNew = existing is null;
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

    public string Topic { get; }

    public string Subscription { get; }

    public bool IsNew { get; }

    public string Title => IsNew ? $"New rule · {Topic} / {Subscription}" : $"Rule {Name} · {Topic} / {Subscription}";

    public string ConfirmLabel => IsNew ? "Add rule" : "Save rule";

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

    public bool IsSql => Kind.Kind == RuleFilterKind.Sql;

    public bool IsCorrelation => Kind.Kind == RuleFilterKind.Correlation;

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
                return "For example: region = 'EU' AND amount > 100, sys.Label LIKE 'order.%', tenant IN ('acme', 'globex'). Text is case-sensitive.";
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
            properties[property] = _originalProperties.TryGetValue(property, out var original) && original.Line == line
                ? original.Value
                : RoutingValue.Parse(line[(separator + 1)..]);
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

    /// <summary>Empty means "not checked"; anything else is kept exactly, since correlation fields match exactly.</summary>
    private static string? Blank(string value) => string.IsNullOrEmpty(value) ? null : value;

    private static string Line(string name, object value) => $"{name} = {RoutingValue.Format(value)}";

    [GeneratedRegex(@"^[A-Za-z0-9$._-]+$")]
    private static partial Regex RuleName();
}
