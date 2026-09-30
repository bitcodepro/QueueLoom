using System.Globalization;
using QueueLoom.App.Models;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class BackupMessageItemViewModel(DeadLetterBackupSummary summary)
{
    public DeadLetterBackupSummary Summary { get; } = summary;

    public string ProfileName => Summary.ProfileName;

    public string EnvironmentLabel => Summary.Environment.Trim().ToUpperInvariant() switch
    {
        "DEVELOPMENT" => "DEV",
        "PRODUCTION" => "PROD",
        var label => label
    };

    public Tone EnvironmentTone => Tones.ForEnvironmentName(Summary.Environment);

    public string SourceDisplay => Summary.Source.DisplayName;

    public string SourceKind => Summary.Source.Kind == ServiceBusEntityKind.Queue
        ? "QUEUE"
        : "SUBSCRIPTION";

    public string SubQueueLabel => Summary.SubQueue switch
    {
        ServiceBusSubQueue.TransferDeadLetter => "TRANSFER DLQ",
        ServiceBusSubQueue.Active => "SCHEDULED / DEFERRED",
        _ => "DLQ"
    };

    public string MessageId => Summary.MessageId ?? "(no MessageId)";

    public string CorrelationId => Summary.CorrelationId ?? "—";

    public string Subject => Summary.Subject ?? "—";

    public string EnqueuedAt => Summary.EnqueuedAt?.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) ?? "—";

    public string BackedUpAt => Summary.BackedUpAt.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    public string BodySize => $"{Summary.BodySize:N0} bytes";

    public string FileName => Path.GetFileName(Summary.FilePath);

    public string FilePath => Summary.FilePath;

    public bool IsReadable => Summary.IsReadable;

    public bool HasError => !Summary.IsReadable;

    public string Error => Summary.Error ?? string.Empty;
}
