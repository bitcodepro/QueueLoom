using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class BackupMessageItemViewModel(DeadLetterBackupSummary summary)
{
    public DeadLetterBackupSummary Summary { get; } = summary;

    public string ProfileName => Summary.ProfileName;

    public string EnvironmentLabel => Summary.Environment.ToUpperInvariant();

    public string EnvironmentColor => Summary.Environment.ToUpperInvariant() switch
    {
        "DEVELOPMENT" or "DEV" => "#2DD4BF",
        "TEST" => "#8B7CFF",
        "PRODUCTION" or "PROD" => "#FF6B82",
        _ => "#FFB45E"
    };

    public string SourceDisplay => Summary.Source.DisplayName;

    public string SourceKind => Summary.Source.Kind == ServiceBusEntityKind.Queue
        ? "QUEUE"
        : "SUBSCRIPTION";

    public string SubQueueLabel => Summary.SubQueue == ServiceBusSubQueue.TransferDeadLetter
        ? "TRANSFER DLQ"
        : "DLQ";

    public string MessageId => Summary.MessageId ?? "(no MessageId)";

    public string CorrelationId => Summary.CorrelationId ?? "—";

    public string Subject => Summary.Subject ?? "—";

    public string EnqueuedAt => Summary.EnqueuedAt?.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss") ?? "—";

    public string BackedUpAt => Summary.BackedUpAt.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");

    public string BodySize => $"{Summary.BodySize:N0} bytes";

    public string FileName => Path.GetFileName(Summary.FilePath);

    public string FilePath => Summary.FilePath;

    public bool IsReadable => Summary.IsReadable;

    public bool HasError => !Summary.IsReadable;

    public string Error => Summary.Error ?? string.Empty;
}
