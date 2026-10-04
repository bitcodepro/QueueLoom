using System.Globalization;
using System.Net;
using System.Text.Json;

namespace QueueLoom.Core.Routing;

/// <summary>A filter policy SNS would not accept, with the reason.</summary>
public sealed class FilterPolicyException(string message) : FormatException(message);

/// <summary>
/// Amazon SNS subscription filter policies: a JSON object whose keys are message attributes (or, with the
/// MessageBody scope, fields of the JSON body) and whose values list what each may be. Every key must match;
/// any one value of a key's list is enough. Names and values are case-sensitive, a missing attribute matches
/// nothing but <c>{"exists": false}</c>, and <c>$or</c> joins alternative policies.
/// </summary>
public static class SnsFilterPolicy
{
    private enum Match
    {
        No,
        Yes,
        Unknown
    }

    private enum ScalarKind
    {
        String,
        Number,
        Boolean,
        Null,
        Binary,
        Other
    }

    private readonly record struct Scalar(ScalarKind Kind, string Text, double Number = 0, bool Flag = false)
    {
        public string Describe() => Kind switch
        {
            ScalarKind.String => $"'{Text}'",
            ScalarKind.Number => Text,
            ScalarKind.Boolean => Flag ? "true" : "false",
            ScalarKind.Null => "null",
            ScalarKind.Binary => "a Binary attribute",
            _ => Text
        };
    }

    private static readonly HashSet<string> Operators =
        ["prefix", "suffix", "equals-ignore-case", "anything-but", "numeric", "exists", "cidr"];

    /// <summary>Throws <see cref="FilterPolicyException"/> when SNS would reject the policy.</summary>
    public static void Validate(string policy, bool onBody)
    {
        using var document = ParseDocument(policy);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new FilterPolicyException("A filter policy is a JSON object, for example {\"region\": [\"EU\"]}.");
        }
        ValidateObject(document.RootElement, onBody, string.Empty);
    }

    /// <summary>Whether SNS would deliver the message to a subscription with this policy, and why.</summary>
    public static (RoutingOutcome Outcome, string Explanation) Evaluate(string policy, bool onBody, RoutingMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        JsonDocument document;
        try
        {
            document = ParseDocument(policy);
        }
        catch (FilterPolicyException exception)
        {
            return (RoutingOutcome.Unknown, $"QueueLoom cannot read this policy ({exception.Message}).");
        }

        using (document)
        {
            Scope scope;
            if (onBody)
            {
                if (message.Body is null)
                {
                    return (RoutingOutcome.Unknown, "The policy looks at the body, which QueueLoom does not have as text here; SNS decides.");
                }
                try
                {
                    using var body = JsonDocument.Parse(message.Body);
                    scope = new BodyScope(body.RootElement.Clone());
                }
                catch (JsonException)
                {
                    return (RoutingOutcome.Skips, "The policy looks at the body, and the body is not JSON, so SNS matches nothing in it.");
                }
            }
            else
            {
                scope = new AttributeScope(message);
            }

            var misses = new List<string>();
            var unknowns = new List<string>();
            var match = EvaluateObject(document.RootElement, scope, string.Empty, misses, unknowns);
            return match switch
            {
                Match.Yes => (RoutingOutcome.Receives, "The filter policy matches."),
                Match.Unknown => (RoutingOutcome.Unknown, string.Join("; ", unknowns.Take(3))),
                _ => (RoutingOutcome.Skips, misses.Count > 0 ? string.Join("; ", misses.Take(3)) : "The filter policy does not match.")
            };
        }
    }

    private static JsonDocument ParseDocument(string policy)
    {
        if (string.IsNullOrWhiteSpace(policy))
        {
            throw new FilterPolicyException("The policy is empty.");
        }
        try
        {
            return JsonDocument.Parse(policy);
        }
        catch (JsonException exception)
        {
            throw new FilterPolicyException($"It is not valid JSON: {exception.Message}");
        }
    }

    private static void ValidateObject(JsonElement policy, bool onBody, string path)
    {
        if (!policy.EnumerateObject().Any())
        {
            throw new FilterPolicyException(path.Length == 0 ? "The policy has no keys." : $"{path} has no keys.");
        }
        foreach (var property in policy.EnumerateObject())
        {
            var name = Join(path, property.Name);
            if (property.Name == "$or")
            {
                if (property.Value.ValueKind != JsonValueKind.Array || property.Value.GetArrayLength() < 2 ||
                    property.Value.EnumerateArray().Any(item => item.ValueKind != JsonValueKind.Object))
                {
                    throw new FilterPolicyException("$or takes a list of at least two policies (JSON objects).");
                }
                foreach (var alternative in property.Value.EnumerateArray())
                {
                    ValidateObject(alternative, onBody, path);
                }
                continue;
            }
            switch (property.Value.ValueKind)
            {
                case JsonValueKind.Object when onBody:
                    ValidateObject(property.Value, onBody, name);
                    break;
                case JsonValueKind.Array:
                    if (property.Value.GetArrayLength() == 0)
                    {
                        throw new FilterPolicyException($"{name} lists no values.");
                    }
                    foreach (var condition in property.Value.EnumerateArray())
                    {
                        ValidateCondition(condition, name, onBody);
                    }
                    break;
                default:
                    throw new FilterPolicyException(onBody
                        ? $"{name} must be a list of values, such as [\"EU\"], or a nested object."
                        : $"{name} must be a list of values, such as [\"EU\"]. Nested objects need the MessageBody scope.");
            }
        }
    }

    private static void ValidateCondition(JsonElement condition, string name, bool onBody)
    {
        switch (condition.ValueKind)
        {
            case JsonValueKind.String or JsonValueKind.Number:
                return;
            case JsonValueKind.True or JsonValueKind.False or JsonValueKind.Null when onBody:
                return;
            case JsonValueKind.Object:
                var operators = condition.EnumerateObject().ToArray();
                if (operators.Length != 1)
                {
                    throw new FilterPolicyException($"{name}: each condition object holds one operator, such as {{\"prefix\": \"EU\"}}.");
                }
                var (op, argument) = (operators[0].Name, operators[0].Value);
                if (!Operators.Contains(op))
                {
                    throw new FilterPolicyException($"{name}: SNS has no operator \"{op}\".");
                }
                var valid = op switch
                {
                    "prefix" or "suffix" => argument.ValueKind == JsonValueKind.String ||
                                            argument.ValueKind == JsonValueKind.Object && IsIgnoreCase(argument),
                    "equals-ignore-case" or "cidr" => argument.ValueKind == JsonValueKind.String,
                    "exists" => argument.ValueKind is JsonValueKind.True or JsonValueKind.False,
                    "numeric" => IsNumericRange(argument),
                    _ => argument.ValueKind is JsonValueKind.String or JsonValueKind.Number ||
                         argument.ValueKind == JsonValueKind.Array && argument.GetArrayLength() > 0 &&
                         argument.EnumerateArray().All(item => item.ValueKind is JsonValueKind.String or JsonValueKind.Number) ||
                         argument.ValueKind == JsonValueKind.Object && argument.EnumerateObject().Count() == 1 &&
                         argument.EnumerateObject().First() is { } inner &&
                         (inner.Name is "prefix" or "suffix" && inner.Value.ValueKind == JsonValueKind.String ||
                          inner.Name == "equals-ignore-case" && (inner.Value.ValueKind == JsonValueKind.String ||
                              inner.Value.ValueKind == JsonValueKind.Array && inner.Value.GetArrayLength() > 0 &&
                              inner.Value.EnumerateArray().All(item => item.ValueKind == JsonValueKind.String)))
                };
                if (!valid)
                {
                    throw new FilterPolicyException($"{name}: \"{op}\" does not take {argument.GetRawText()}.");
                }
                return;
            default:
                throw new FilterPolicyException($"{name}: {condition.GetRawText()} is not a value SNS can match on a message attribute.");
        }
    }

    private static bool IsIgnoreCase(JsonElement argument) =>
        argument.EnumerateObject().ToArray() is [{ Name: "equals-ignore-case", Value.ValueKind: JsonValueKind.String }];

    private static bool IsNumericRange(JsonElement argument)
    {
        if (argument.ValueKind != JsonValueKind.Array)
        {
            return false;
        }
        var items = argument.EnumerateArray().ToArray();
        if (items.Length is not (2 or 4))
        {
            return false;
        }
        for (var index = 0; index < items.Length; index += 2)
        {
            if (items[index].ValueKind != JsonValueKind.String || items[index].GetString() is not ("=" or "<" or "<=" or ">" or ">=") ||
                items[index + 1].ValueKind != JsonValueKind.Number)
            {
                return false;
            }
        }
        return items.Length == 2 || items[0].GetString() is ">" or ">=" && items[2].GetString() is "<" or "<=";
    }

    private static Match EvaluateObject(JsonElement policy, Scope scope, string path, List<string> misses, List<string> unknowns)
    {
        var result = Match.Yes;
        foreach (var property in policy.EnumerateObject())
        {
            var match = Match.No;
            var name = Join(path, property.Name);
            if (property.Name == "$or" && property.Value.ValueKind == JsonValueKind.Array)
            {
                var branchMisses = new List<string>();
                foreach (var alternative in property.Value.EnumerateArray())
                {
                    var branch = alternative.ValueKind == JsonValueKind.Object
                        ? EvaluateObject(alternative, scope, path, branchMisses, unknowns)
                        : Match.No;
                    match = branch == Match.Yes || match == Match.Yes ? Match.Yes : branch == Match.Unknown ? Match.Unknown : match;
                }
                if (match == Match.No)
                {
                    misses.Add("no alternative of $or matches (" + string.Join("; ", branchMisses.Take(2)) + ")");
                }
            }
            else if (property.Value.ValueKind == JsonValueKind.Object)
            {
                match = scope.Child(property.Name) is { } child
                    ? EvaluateObject(property.Value, child, name, misses, unknowns)
                    : Miss(misses, $"{name} is missing");
            }
            else if (property.Value.ValueKind == JsonValueKind.Array)
            {
                var values = scope.Get(property.Name);
                match = Match.No;
                foreach (var condition in property.Value.EnumerateArray())
                {
                    var one = EvaluateCondition(condition, values, name, unknowns, scope.HasProperties);
                    match = one == Match.Yes || match == Match.Yes ? Match.Yes : one == Match.Unknown ? Match.Unknown : match;
                }
                if (match == Match.No)
                {
                    var wanted = Compact(property.Value);
                    misses.Add(values is null
                        ? $"{name} is missing; the policy wants {wanted}"
                        : $"{name} is {Describe(values)}; the policy wants {wanted}");
                }
            }
            else
            {
                unknowns.Add($"QueueLoom cannot read the condition on {name}; SNS decides");
                match = Match.Unknown;
            }

            if (match == Match.No)
            {
                result = Match.No;
            }
            else if (match == Match.Unknown && result == Match.Yes)
            {
                result = Match.Unknown;
            }
        }
        return result;
    }

    private static Match Miss(List<string> misses, string text)
    {
        misses.Add(text);
        return Match.No;
    }

    private static Match EvaluateCondition(JsonElement condition, IReadOnlyList<Scalar>? values, string name, List<string> unknowns, bool hasProperties)
    {
        if (condition.ValueKind == JsonValueKind.Object &&
            condition.EnumerateObject().ToArray() is [{ Name: "exists" } exists] &&
            exists.Value.ValueKind is JsonValueKind.True or JsonValueKind.False)
        {
            return exists.Value.GetBoolean()
                ? values is not null ? Match.Yes : Match.No
                : values is null && hasProperties ? Match.Yes : Match.No;
        }
        if (values is null)
        {
            return Match.No;
        }

        var result = Match.No;
        foreach (var value in values)
        {
            var match = EvaluateScalar(condition, value, name, unknowns);
            if (match == Match.Yes)
            {
                return Match.Yes;
            }
            if (match == Match.Unknown)
            {
                result = Match.Unknown;
            }
        }
        return result;
    }

    private static Match EvaluateScalar(JsonElement condition, Scalar value, string name, List<string> unknowns)
    {
        switch (condition.ValueKind)
        {
            case JsonValueKind.String:
                return value.Kind switch
                {
                    ScalarKind.String => Is(string.Equals(value.Text, condition.GetString(), StringComparison.Ordinal)),
                    ScalarKind.Number => Unknown(unknowns,
                        $"{name} is the Number {value.Text} and the policy compares it with the text '{condition.GetString()}'; SNS decides"),
                    _ => Match.No
                };
            case JsonValueKind.Number:
                return value.Kind switch
                {
                    ScalarKind.Number => Is(value.Number == condition.GetDouble()),
                    ScalarKind.String => Unknown(unknowns,
                        $"{name} is the text '{value.Text}' and the policy compares it with the number {condition.GetRawText()}; SNS decides"),
                    _ => Match.No
                };
            case JsonValueKind.True or JsonValueKind.False:
                return Is(value.Kind == ScalarKind.Boolean && value.Flag == condition.GetBoolean());
            case JsonValueKind.Null:
                return Is(value.Kind == ScalarKind.Null);
            case JsonValueKind.Object:
                break;
            default:
                return Match.No;
        }

        var operators = condition.EnumerateObject().ToArray();
        if (operators.Length != 1)
        {
            return Unknown(unknowns, $"QueueLoom cannot read the condition {Compact(condition)} on {name}; SNS decides");
        }
        var (op, argument) = (operators[0].Name, operators[0].Value);
        switch (op)
        {
            case "prefix" or "suffix":
                if (value.Kind != ScalarKind.String)
                {
                    return Match.No;
                }
                var (text, comparison) = argument.ValueKind == JsonValueKind.Object && IsIgnoreCase(argument)
                    ? (argument.GetProperty("equals-ignore-case").GetString()!, StringComparison.OrdinalIgnoreCase)
                    : (argument.GetString() ?? string.Empty, StringComparison.Ordinal);
                return Is(op == "prefix" ? value.Text.StartsWith(text, comparison) : value.Text.EndsWith(text, comparison));
            case "equals-ignore-case":
                return Is(value.Kind == ScalarKind.String &&
                          string.Equals(value.Text, argument.GetString(), StringComparison.OrdinalIgnoreCase));
            case "numeric":
                if (value.Kind == ScalarKind.String)
                {
                    return Unknown(unknowns, $"{name} is the text '{value.Text}' and the policy compares it as a number; SNS decides");
                }
                return Is(value.Kind == ScalarKind.Number && InRange(argument, value.Number));
            case "cidr":
                return Is(value.Kind == ScalarKind.String && IPAddress.TryParse(value.Text, out var address) &&
                          IPNetwork.TryParse(argument.GetString(), out var network) && network.Contains(address));
            case "anything-but":
                return AnythingBut(argument, value);
            default:
                return Unknown(unknowns, $"QueueLoom does not know the operator \"{op}\" on {name}; SNS decides");
        }
    }

    private static Match AnythingBut(JsonElement argument, Scalar value)
    {
        if (value.Kind is ScalarKind.Binary)
        {
            return Match.No;
        }
        if (argument.ValueKind == JsonValueKind.Object && argument.EnumerateObject().FirstOrDefault() is { Name: { } inner } property)
        {
            if (value.Kind != ScalarKind.String)
            {
                return Match.Yes;
            }
            return inner switch
            {
                "prefix" => Is(!value.Text.StartsWith(property.Value.GetString() ?? string.Empty, StringComparison.Ordinal)),
                "suffix" => Is(!value.Text.EndsWith(property.Value.GetString() ?? string.Empty, StringComparison.Ordinal)),
                _ => Is(!(property.Value.ValueKind == JsonValueKind.Array ? property.Value.EnumerateArray().ToArray() : [property.Value])
                    .Any(item => item.ValueKind == JsonValueKind.String &&
                                 string.Equals(item.GetString(), value.Text, StringComparison.OrdinalIgnoreCase)))
            };
        }
        var excluded = argument.ValueKind == JsonValueKind.Array ? argument.EnumerateArray().ToArray() : [argument];
        return Is(!excluded.Any(item => item.ValueKind switch
        {
            JsonValueKind.String => value.Kind == ScalarKind.String && value.Text == item.GetString(),
            JsonValueKind.Number => value.Kind == ScalarKind.Number && value.Number == item.GetDouble(),
            _ => false
        }));
    }

    private static bool InRange(JsonElement range, double number)
    {
        var items = range.EnumerateArray().ToArray();
        for (var index = 0; index + 1 < items.Length; index += 2)
        {
            var bound = items[index + 1].GetDouble();
            var ok = items[index].GetString() switch
            {
                "=" => number == bound,
                "<" => number < bound,
                "<=" => number <= bound,
                ">" => number > bound,
                ">=" => number >= bound,
                _ => false
            };
            if (!ok)
            {
                return false;
            }
        }
        return true;
    }

    private static Match Is(bool value) => value ? Match.Yes : Match.No;

    private static Match Unknown(List<string> unknowns, string text)
    {
        unknowns.Add(text);
        return Match.Unknown;
    }

    private static string Join(string path, string name) => path.Length == 0 ? name : $"{path}.{name}";

    private static string Compact(JsonElement element) => JsonSerializer.Serialize(element);

    private static string Describe(IReadOnlyList<Scalar> values) => values.Count == 1
        ? values[0].Describe()
        : "[" + string.Join(", ", values.Select(value => value.Describe())) + "]";

    /// <summary>What a policy key refers to: a message attribute, or a field of the JSON body.</summary>
    private abstract class Scope
    {
        public abstract bool HasProperties { get; }
        /// <summary>The value (an array gives each of its items); null when it is missing.</summary>
        public abstract IReadOnlyList<Scalar>? Get(string key);

        public abstract Scope? Child(string key);
    }

    private sealed class AttributeScope(RoutingMessage message) : Scope
    {
        public override bool HasProperties => message.Attributes.Count > 0;
        public override IReadOnlyList<Scalar>? Get(string key)
        {
            if (!message.Attributes.TryGetValue(key, out var value))
            {
                return null;
            }
            var text = message.AttributeTextOf(key);
            return value switch
            {
                byte[] => [new Scalar(ScalarKind.Binary, string.Empty)],
                string => [new Scalar(ScalarKind.String, text)],
                byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal =>
                    // SNS reads the Number from the text QueueLoom sends, so a Single 0.1 is 0.1, not 0.10000000149011612.
                    [new Scalar(ScalarKind.Number, text,
                        double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var number)
                            ? number
                            : Convert.ToDouble(value, CultureInfo.InvariantCulture))],
                // QueueLoom sends other types (Boolean, Guid, dates…) as String attributes with their text.
                _ => [new Scalar(ScalarKind.String, text)]
            };
        }

        public override Scope? Child(string key) => null;
    }

    /// <summary>
    /// Fields of the JSON body. As in SNS payload filtering, arrays are flattened: a key under an array of objects
    /// (the Records of an S3 event) looks into every object, and a key whose value is an array matches on any item.
    /// </summary>
    private sealed class BodyScope(IReadOnlyList<JsonElement> elements) : Scope
    {
        public override bool HasProperties => elements.SelectMany(Flatten)
            .Any(element => element.ValueKind == JsonValueKind.Object && element.EnumerateObject().Any());
        public BodyScope(JsonElement element)
            : this([element])
        {
        }

        public override IReadOnlyList<Scalar>? Get(string key)
        {
            var values = Values(key).ToArray();
            return values.Length == 0 ? null : values.SelectMany(Flatten).Where(value => value.ValueKind != JsonValueKind.Object).Select(ToScalar).ToArray();
        }

        public override Scope? Child(string key)
        {
            var objects = Values(key).SelectMany(Flatten).Where(value => value.ValueKind == JsonValueKind.Object).ToArray();
            return objects.Length == 0 ? null : new BodyScope(objects);
        }

        private IEnumerable<JsonElement> Values(string key) => elements
            .SelectMany(Flatten)
            .Where(element => element.ValueKind == JsonValueKind.Object && element.TryGetProperty(key, out _))
            .Select(element => element.GetProperty(key));

        private static IEnumerable<JsonElement> Flatten(JsonElement element) =>
            element.ValueKind == JsonValueKind.Array ? element.EnumerateArray().SelectMany(Flatten) : [element];

        private static Scalar ToScalar(JsonElement value) => value.ValueKind switch
        {
            JsonValueKind.String => new Scalar(ScalarKind.String, value.GetString() ?? string.Empty),
            JsonValueKind.Number => new Scalar(ScalarKind.Number, value.GetRawText(), value.GetDouble()),
            JsonValueKind.True or JsonValueKind.False => new Scalar(ScalarKind.Boolean, value.GetRawText(), Flag: value.GetBoolean()),
            JsonValueKind.Null => new Scalar(ScalarKind.Null, "null"),
            _ => new Scalar(ScalarKind.Other, value.GetRawText())
        };
    }
}
