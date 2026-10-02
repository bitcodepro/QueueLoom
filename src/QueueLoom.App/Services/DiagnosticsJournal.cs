using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.Profiles;

namespace QueueLoom.App.Services;

public enum DiagnosticStage { Unknown, Started, Executing, Completed, Cancelled, Failed }
public enum DiagnosticOutcome { Unknown, Confirmed, Rejected }
public enum DiagnosticCheck { Unknown, Verified, Mismatch }
public enum DiagnosticRecovery { Unknown, Requested, StartupAcknowledged, Restored, Failed }

/// <summary>Only typed facts enter this bounded, in-memory journal. Never reads logs or saved user data.</summary>
public sealed class DiagnosticsJournal
{
    public const int MaximumEvents = 64;
    public static DiagnosticsJournal Session { get; } = new();
    private readonly object _sync = new();
    private readonly Queue<Event> _events = new();
    private long _sequence;
    private long _discarded;
    private sealed record Event(long Operation, DateTimeOffset Timestamp, string Kind, string Broker,
        string? AddressKey, string? EntityKey, DiagnosticStage Stage, DiagnosticOutcome Outcome,
        SafeError[] Errors, UpdatePhase? UpdateStage, DiagnosticCheck Checksum, DiagnosticRecovery Restart,
        DiagnosticRecovery Rollback);

    // Exact application-owned labels, not caller prose or exception strings.
    private static readonly HashSet<string> Kinds = new(StringComparer.Ordinal)
    {
        "Loading environments", "Saving environment", "Updating environment", "Deleting environment",
        "Connecting", "Disconnecting", "Refreshing topology", "Scanning dead letters", "Scanning all environments",
        "Searching dead letters", "Peeking messages", "Peeking DLQ", "Peeking transfer DLQ", "Opening DLQ",
        "Purging environment dead letters", "Purging topic dead letters", "Purging selected dead letters",
        "Loading backups", "Loading backup message", "Deleting local backup", "Deleting local backups",
        "Sending message", "Unlocking writes", "Loading next page", "Monitor", "Update",
        "Resending messages", "Deleting messages", "Replaying messages", "Continuing unattempted items",
        "Retrying proven rejections", "Running scheduled resend", "Exporting messages", "Creating queue",
        "Updating queue", "Deleting queue", "Loading routing", "Purging messages",
        "Checking routing", "Deleting old backups", "Deleting selected messages", "Exporting environments",
        "Importing environments", "Opening rules", "Reading topic", "Replaying loaded messages",
        "Resending selected messages", "Restoring filtered backups", "Resuming latest batch", "Running a scheduled resend",
        "Sending a test alert", "Formatting JSON"
    };

    public long Begin(string? kind, MessagingProvider? broker = null, string? address = null, string? entity = null)
    {
        lock (_sync)
        {
            var id = ++_sequence;
            Add(new(id, DateTimeOffset.UtcNow, kind is not null && Kinds.Contains(kind) ? kind : "Other operation",
                broker is { } provider && Enum.IsDefined(provider) ? provider.ToString() : "Unknown",
                Key(address), Key(entity) is { } entityKey ? Key((Key(address) ?? "Unknown") + entityKey) : null, DiagnosticStage.Started, DiagnosticOutcome.Unknown, [], null,
                DiagnosticCheck.Unknown, DiagnosticRecovery.Unknown, DiagnosticRecovery.Unknown));
            return id;
        }
    }

    public void Record(long operation, DiagnosticStage stage, DiagnosticOutcome outcome = DiagnosticOutcome.Unknown,
        Exception? error = null, UpdatePhase? updateStage = null, DiagnosticCheck checksum = DiagnosticCheck.Unknown,
        DiagnosticRecovery restart = DiagnosticRecovery.Unknown, DiagnosticRecovery rollback = DiagnosticRecovery.Unknown)
    {
        // Summarize immediately; retain neither exception objects nor raw strings.
        var errors = Summarize(error);
        lock (_sync)
        {
            var prior = _events.LastOrDefault(e => e.Operation == operation);
            if (prior is null) return;
            Add(prior with { Timestamp = DateTimeOffset.UtcNow, Stage = Defined(stage), Outcome = Defined(outcome),
                Errors = errors, UpdateStage = updateStage is { } phase && Enum.IsDefined(phase) ? phase : null,
                Checksum = Defined(checksum), Restart = Defined(restart), Rollback = Defined(rollback) });
        }
    }

    private static T Defined<T>(T value) where T : struct, Enum => Enum.IsDefined(value) ? value : default;
    private void Add(Event item)
    {
        _events.Enqueue(item);
        if (_events.Count > MaximumEvents) { _events.Dequeue(); _discarded++; }
    }
    private static string? Key(string? value)
    {
        if (string.IsNullOrEmpty(value) || value.Length > 4096) return null;
        try { return Convert.ToHexString(SHA256.HashData(new UTF8Encoding(false, true).GetBytes(value))); }
        catch (EncoderFallbackException) { return null; }
    }

    public DiagnosticsPreview Capture()
    {
        lock (_sync)
        {
            var operations = new Dictionary<long, string>();
            var addresses = new Dictionary<string, string>(StringComparer.Ordinal);
            var entities = new Dictionary<string, string>(StringComparer.Ordinal);
            string Placeholder<TKey>(Dictionary<TKey, string> map, TKey key, string label) where TKey : notnull
            {
                if (!map.TryGetValue(key, out var result)) map[key] = result = $"{label}-{map.Count + 1}";
                return result;
            }
            var events = _events.Select(e => new
            {
                Operation = Placeholder(operations, e.Operation, "operation"), e.Timestamp, e.Kind, e.Broker,
                Address = e.AddressKey is null ? null : Placeholder(addresses, e.AddressKey, "broker"),
                Entity = e.EntityKey is null ? null : Placeholder(entities, e.EntityKey, "entity"),
                Stage = e.Stage.ToString(), Outcome = e.Outcome.ToString(), e.Errors,
                UpdateStage = e.UpdateStage?.ToString(), Checksum = e.Checksum.ToString(),
                Restart = e.Restart.ToString(), Rollback = e.Rollback.ToString()
            }).ToArray();
            var generated = DateTimeOffset.UtcNow;
            var omittedForSize = 0;
            string Serialize() => JsonSerializer.Serialize(new
            {
                SchemaVersion = 1, GeneratedAt = generated,
                ApplicationVersion = typeof(DiagnosticsJournal).Assembly.GetName().Version?.ToString() ?? "Unknown",
                OS = OperatingSystem.IsWindows() ? "Windows" : OperatingSystem.IsLinux() ? "Linux" : OperatingSystem.IsMacOS() ? "macOS" : "Unknown",
                OSVersion = Environment.OSVersion.Version.ToString(),
                Architecture = RuntimeInformation.OSArchitecture.ToString(), ProcessArchitecture = RuntimeInformation.ProcessArchitecture.ToString(),
                Scope, DiscardedEvents = _discarded, OmittedForSize = omittedForSize, Events = events
            }, new JsonSerializerOptions { WriteIndented = true });
            var json = Serialize();
            const string heading = "QueueLoom local diagnostics\n\n";
            while (events.Length > 0 && Encoding.UTF8.GetByteCount(heading + Scope + "\n\n" + json) + Encoding.UTF8.GetByteCount(json) > DiagnosticsPreview.MaximumOutputBytes)
            {
                events = events[1..]; omittedForSize++; json = Serialize();
            }
            // The readable report contains the very same allowlisted fields, with no additional data sources.
            return new DiagnosticsPreview(heading + Scope + "\n\n" + json, json);
        }
    }

    public const string Scope = "Current session only; up to 64 typed technical events, four exceptions and three application stack methods per exception; output is limited to 256 KiB. " +
        "Older events or excess exception details may be omitted. Unknown means no confirmation was recorded. " +
        "Exception messages, Activity prose, logs, credentials, connection strings, message bodies/headers, settings, environment variables, " +
        "files, backups and command lines are excluded. Broker/entity references use report-local placeholders; local paths are omitted. " +
        "This is a troubleshooting summary, not a complete audit. Review both files before sharing publicly. Nothing is uploaded or sent.";

    public sealed record SafeError(string Category, string Code, string[] Frames);
    private static SafeError[] Summarize(Exception? exception)
    {
        var result = new List<SafeError>();
        var pending = new Stack<Exception>();
        var visited = new HashSet<Exception>(ReferenceEqualityComparer.Instance);
        if (exception is not null) pending.Push(exception);
        while (pending.TryPop(out var current) && result.Count < 4)
        {
            if (!visited.Add(current)) continue;
            var type = current.GetType();
            var category = type == typeof(TimeoutException) ? "Timeout" : type == typeof(OperationCanceledException) || type == typeof(TaskCanceledException) ? "Cancellation" :
                type == typeof(UnauthorizedAccessException) ? "AccessDenied" : type == typeof(IOException) ? "IO" : type == typeof(InvalidDataException) ? "InvalidData" :
                type == typeof(InvalidOperationException) ? "InvalidOperation" : type == typeof(AggregateException) ? "Aggregate" : "Other";
            var code = "Unknown";
            if (type == typeof(Azure.Messaging.ServiceBus.ServiceBusException) && current is Azure.Messaging.ServiceBus.ServiceBusException sb && Enum.IsDefined(sb.Reason))
            { category = "ServiceBus"; code = sb.Reason.ToString(); }
            if (type == typeof(System.Net.Http.HttpRequestException) && current is System.Net.Http.HttpRequestException http)
            { category = "HTTP"; code = http.StatusCode is { } status && Enum.IsDefined(status) ? ((int)status).ToString(System.Globalization.CultureInfo.InvariantCulture) : "Unknown"; }
            var frames = new List<string>();
            try
            {
                foreach (var frame in (new StackTrace(current, false).GetFrames() ?? []).Take(64))
                {
                    var method = frame.GetMethod();
                    var declaring = method?.DeclaringType;
                    // Only metadata from this static application assembly; omit SDK/test/dynamic frames and all source paths.
                    if (declaring?.Assembly != typeof(DiagnosticsJournal).Assembly || method is null ||
                        declaring.Assembly.IsDynamic || declaring.FullName is not { } name) continue;
                    var label = name + "." + method.Name;
                    frames.Add(label.Length > 96 ? label[..96] : label);
                    if (frames.Count == 3) break;
                }
            }
            catch { /* Malformed exception metadata is omitted. */ }
            result.Add(new(category, code, frames.ToArray()));
            if (current is AggregateException aggregate)
            {
                foreach (var child in aggregate.InnerExceptions.Take(8).Reverse()) if (pending.Count < 16) pending.Push(child);
            }
            else if (current.InnerException is { } inner && pending.Count < 16) pending.Push(inner);
        }
        return result.ToArray();
    }
}
