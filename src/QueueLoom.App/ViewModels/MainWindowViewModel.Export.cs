using QueueLoom.App.Commands;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Saving the listed (or ticked) messages to a JSON or CSV file.</summary>
public sealed partial class MainWindowViewModel
{
    public AsyncRelayCommand ExportMessagesCommand { get; private set; } = null!;

    public string ExportMessagesLabel => HasMarkedMessages
        ? $"Export {MarkedMessageCount:N0}…"
        : "Export all…";

    private void InitializeExport()
    {
        ExportMessagesCommand = _commands.Create(
            token => RunOperationAsync("Exporting messages", ExportMessagesAsync, token),
            () => !IsBusy && Messages.Count > 0);
    }

    private async Task ExportMessagesAsync(CancellationToken cancellationToken)
    {
        var selection = HasMarkedMessages
            ? Messages.Where(message => message.IsMarked).ToArray()
            : Messages.ToArray();
        if (selection.Length == 0)
        {
            throw new InvalidOperationException("There are no messages to export.");
        }

        var path = await _dialogs.ChooseSaveFileAsync(
                "Export messages",
                $"queueloom-messages-{DateTimeOffset.Now:yyyyMMdd-HHmmss}.json",
                [("JSON (all details)", "*.json"), ("CSV (spreadsheet)", "*.csv")],
                cancellationToken)
            .ConfigureAwait(true);
        if (string.IsNullOrWhiteSpace(path))
        {
            StatusText = "Export cancelled";
            return;
        }

        var narrowed = await MessageExport.WriteAsync(
                path,
                selection.Select(message => new ExportedMessage(
                    $"{message.ProfileName} ({message.EnvironmentLabel})", message.Message)).ToArray(),
                cancellationToken)
            .ConfigureAwait(true);
        StatusText = $"Exported {selection.Length:N0} message(s) to {path}" +
                     (narrowed ? ". The file's group no longer has the extra access it had: replacing the file safely cannot keep its group" : string.Empty);
        AddActivity("Info", "Messages exported", $"{selection.Length:N0} message(s) · {path}");
    }
}
