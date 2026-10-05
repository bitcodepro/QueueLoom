using Microsoft.Extensions.Logging;
using QueueLoom.App.Services;
using QueueLoom.Core.IO;
using QueueLoom.App.Commands;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Abstractions;

namespace QueueLoom.App.ViewModels;

/// <summary>Exporting environments to a file and importing them, without any password, key or connection string.</summary>
public sealed partial class MainWindowViewModel
{
    public AsyncRelayCommand ExportEnvironmentsCommand { get; private set; } = null!;

    public AsyncRelayCommand ImportEnvironmentsCommand { get; private set; } = null!;

    private void InitializeEnvironmentTransfer()
    {
        ExportEnvironmentsCommand = _commands.Create(
            token => RunOperationAsync("Exporting environments", ExportEnvironmentsAsync, token),
            () => !IsBusy && Profiles.Count > 0);
        ImportEnvironmentsCommand = _commands.Create(
            token => RunOperationAsync("Importing environments", ImportEnvironmentsAsync, token),
            () => !IsBusy);
        Profiles.CollectionChanged += (_, _) => ExportEnvironmentsCommand.NotifyCanExecuteChanged();
    }

    private async Task ExportEnvironmentsAsync(CancellationToken cancellationToken)
    {
        var path = await _dialogs.ChooseSaveFileAsync(
                "Export environments",
                $"queueloom-environments-{DateTimeOffset.Now:yyyyMMdd}.json",
                [("QueueLoom environments", "*.json")],
                cancellationToken)
            .ConfigureAwait(true);
        if (string.IsNullOrWhiteSpace(path))
        {
            StatusText = "Export cancelled";
            return;
        }

        var profiles = Profiles.Select(profile => profile.Profile).ToArray();
        // The user may pick an existing export: it is replaced only once the new one is complete.
        var narrowed = await SafeFileWriter.WriteTextAsync(path, EnvironmentTransfer.Export(profiles), cancellationToken).ConfigureAwait(true);
        StatusText = $"Exported {profiles.Length:N0} environment(s) to {Path.GetFileName(path)}, without passwords, keys or connection strings" +
                     (narrowed ? ". The file's group no longer has the extra access it had: replacing the file safely cannot keep its group" : string.Empty);
        AddActivity("Info", "Environments exported", $"{profiles.Length:N0} environments · {path}");
    }

    private async Task ImportEnvironmentsAsync(CancellationToken cancellationToken)
    {
        var path = await _dialogs.ChooseOpenFileAsync("Import environments", [("QueueLoom environments", "*.json")], cancellationToken)
            .ConfigureAwait(true);
        if (string.IsNullOrWhiteSpace(path))
        {
            StatusText = "Import cancelled";
            return;
        }

        var info = new FileInfo(path);
        if (info.Length > 5 * 1024 * 1024)
        {
            throw new InvalidOperationException("The file is too large to be an environments file.");
        }
        var json = await File.ReadAllTextAsync(path, cancellationToken).ConfigureAwait(true);
        EnvironmentImport import;
        var saved = 0;
        try
        {
            var coordinator = _profileRepository as IProfileMutationCoordinator;
            await using var mutation = coordinator is null ? null : await coordinator.AcquireProfileMutationAsync(cancellationToken).ConfigureAwait(true);
            import = EnvironmentTransfer.Import(json, await _profileRepository.ListAsync(cancellationToken).ConfigureAwait(true));
            foreach (var profile in import.Profiles)
            {
                await _profileRepository.UpsertAsync(profile, cancellationToken).ConfigureAwait(true);
                saved++;
            }
        }
        catch (Exception exception) when (saved > 0 && exception is not OutOfMemoryException)
        {
            // Environments saved before the failure stay saved: list them, and say how far the import got.
            _logger.LogWarning(exception, "Importing environments stopped after {Saved} of them", saved);
            try
            {
                await ReloadProfilesAsync(CancellationToken.None, SelectedProfile?.Id).ConfigureAwait(true);
            }
            catch (Exception refresh) when (refresh is not OutOfMemoryException)
            {
                // Best effort: the same storage problem may stop the listing too; the import summary still goes out.
                _logger.LogWarning(refresh, "The environment list could not be refreshed after a partial import");
            }
            // The summary is what the operator reads, so the cause goes into it (an inner exception would replace it).
            throw new InvalidOperationException(
                $"Imported {saved:N0} environment(s), read-only, before the import stopped: {SanitizeException(exception)}");
        }
        await ReloadProfilesAsync(cancellationToken, import.Profiles.FirstOrDefault()?.Id ?? SelectedProfile?.Id).ConfigureAwait(true);

        var summary = $"Imported {import.Profiles.Count:N0} environment(s), read-only" +
                      (import.NeedSecrets > 0 ? $"; {import.NeedSecrets:N0} need their password or key: choose Edit to enter it" : string.Empty) +
                      (import.Skipped.Count > 0 ? $"; {import.Skipped.Count:N0} skipped" : string.Empty);
        StatusText = summary;
        AddActivity(import.Skipped.Count > 0 ? "Warning" : "Success", "Environments imported",
            summary + (import.Skipped.Count > 0 ? ". " + string.Join(" ", import.Skipped) : string.Empty));
        if (import.Skipped.Count > 0)
        {
            await _dialogs.ShowMessageAsync("Some environments were not imported", string.Join(Environment.NewLine, import.Skipped),
                cancellationToken: cancellationToken).ConfigureAwait(true);
        }
    }
}
