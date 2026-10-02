using QueueLoom.App.Commands;
using QueueLoom.Core.Profiles;

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
        await File.WriteAllTextAsync(path, EnvironmentTransfer.Export(profiles), cancellationToken).ConfigureAwait(true);
        StatusText = $"Exported {profiles.Length:N0} environment(s) to {Path.GetFileName(path)}, without passwords, keys or connection strings";
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
        var import = EnvironmentTransfer.Import(
            await File.ReadAllTextAsync(path, cancellationToken).ConfigureAwait(true),
            Profiles.Select(profile => profile.Profile).ToArray());
        foreach (var profile in import.Profiles)
        {
            await _profileRepository.UpsertAsync(profile, cancellationToken).ConfigureAwait(true);
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
