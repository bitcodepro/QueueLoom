using QueueLoom.App.Commands;
using QueueLoom.App.Services;

namespace QueueLoom.App.ViewModels;

public sealed partial class MainWindowViewModel
{
    public DiagnosticsJournal Diagnostics { get; private set; } = new();
    public AsyncRelayCommand ExportDiagnosticsCommand { get; private set; } = null!;

    private void InitializeDiagnostics(DiagnosticsJournal? diagnostics)
    {
        Diagnostics = diagnostics ?? new();
        ExportDiagnosticsCommand = new AsyncRelayCommand(ExportDiagnosticsAsync);
    }

    private async Task ExportDiagnosticsAsync(CancellationToken token)
    {
        try
        {
            var preview = Diagnostics.Capture();
            if (!await _dialogs.PreviewDiagnosticsAsync(preview, token).ConfigureAwait(true)) return;
            token.ThrowIfCancellationRequested();
            var destination = await _dialogs.ChooseSaveFileAsync("Save local diagnostics ZIP (choose a new file)",
                "QueueLoom-diagnostics.zip", [("ZIP archive", "*.zip")], token).ConfigureAwait(true);
            if (string.IsNullOrWhiteSpace(destination)) return;
            await preview.SaveAsync(destination, token).ConfigureAwait(true);
            await _dialogs.ShowMessageAsync("Diagnostics saved", "The previewed report was saved locally. Review it before sharing publicly. Nothing was uploaded.", cancellationToken: token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch
        {
            // Never echo destination paths or exception messages into this flow.
            await _dialogs.ShowMessageAsync("Diagnostics could not be saved", "Choose a new local ZIP filename in a writable folder. Existing files are never replaced.", isError: true);
        }
    }
}
