using System.Globalization;
using QueueLoom.App.Services;

namespace QueueLoom.App.ViewModels;

public enum UpdateDialogStage
{
    Available,
    Downloading,
    Ready,
    Failed
}

/// <summary>What the update dialog ends with: nothing, or starting the version that was just installed.</summary>
public enum UpdateDialogResult
{
    Later,
    Restart
}

/// <summary>
/// The update window: offers the new version, downloads and installs it with progress, then offers a restart.
/// Where QueueLoom cannot replace itself (a build folder, a read-only program folder), it offers the release page.
/// </summary>
public sealed class UpdateDialogViewModel : ObservableObject
{
    private readonly Func<IProgress<UpdateProgress>, CancellationToken, Task>? _install;
    private CancellationTokenSource? _cancellation;
    private UpdateDialogStage _stage = UpdateDialogStage.Available;
    private double _progress;
    private bool _isProgressKnown;
    private string _progressText = string.Empty;
    private string _error = string.Empty;

    public UpdateDialogViewModel(
        string currentVersion,
        UpdateCheckResult update,
        Func<IProgress<UpdateProgress>, CancellationToken, Task>? install,
        string? cannotInstallReason = null)
    {
        CurrentVersion = currentVersion;
        Update = update;
        _install = install;
        CannotInstallReason = install is null
            ? cannotInstallReason ?? "This copy of QueueLoom cannot replace itself."
            : null;
    }

    public UpdateCheckResult Update { get; }

    public string CurrentVersion { get; }

    public string NewVersion => Update.Version.ToString(3);

    public string? CannotInstallReason { get; }

    public bool CanInstall => _install is not null;

    public UpdateDialogStage Stage
    {
        get => _stage;
        private set
        {
            if (SetProperty(ref _stage, value))
            {
                OnPropertyChanged(nameof(Title));
                OnPropertyChanged(nameof(Message));
                OnPropertyChanged(nameof(IsAvailable));
                OnPropertyChanged(nameof(IsDownloading));
                OnPropertyChanged(nameof(IsReady));
                OnPropertyChanged(nameof(IsFailed));
                OnPropertyChanged(nameof(ShowInstallButton));
            }
        }
    }

    public bool IsAvailable => Stage == UpdateDialogStage.Available;

    public bool IsDownloading => Stage == UpdateDialogStage.Downloading;

    public bool IsReady => Stage == UpdateDialogStage.Ready;

    public bool IsFailed => Stage == UpdateDialogStage.Failed;

    public bool ShowInstallButton => IsAvailable && CanInstall;

    public string Title => Stage switch
    {
        UpdateDialogStage.Downloading => $"Updating to QueueLoom {NewVersion}",
        UpdateDialogStage.Ready => $"QueueLoom {NewVersion} is installed",
        UpdateDialogStage.Failed => "The update was not installed",
        _ => $"QueueLoom {NewVersion} is available"
    };

    public string Message => Stage switch
    {
        UpdateDialogStage.Downloading =>
            "Downloading the package from GitHub. It is checked against its published SHA-256 checksum before anything is replaced.",
        UpdateDialogStage.Ready =>
            "Restart QueueLoom to use the new version. If you restart later, it starts the next time you open QueueLoom.",
        UpdateDialogStage.Failed =>
            "Your current version was left unchanged. You can download the new version from the releases page instead.",
        _ => CanInstall
            ? $"You have {CurrentVersion}. QueueLoom downloads the new version from GitHub, checks it and installs it. " +
              "Your environments, settings and backups are kept."
            : $"You have {CurrentVersion}. {CannotInstallReason} Download the new version from the releases page."
    };

    public double Progress
    {
        get => _progress;
        private set => SetProperty(ref _progress, value);
    }

    public bool IsProgressKnown
    {
        get => _isProgressKnown;
        private set => SetProperty(ref _isProgressKnown, value);
    }

    public string ProgressText
    {
        get => _progressText;
        private set => SetProperty(ref _progressText, value);
    }

    public string Error
    {
        get => _error;
        private set => SetProperty(ref _error, value);
    }

    /// <summary>Downloads and installs; ends in <see cref="UpdateDialogStage.Ready"/> or <see cref="UpdateDialogStage.Failed"/>.</summary>
    public async Task InstallAsync()
    {
        if (_install is null || Stage is UpdateDialogStage.Downloading or UpdateDialogStage.Ready)
        {
            return;
        }

        _cancellation = new CancellationTokenSource();
        Error = string.Empty;
        Progress = 0;
        IsProgressKnown = false;
        ProgressText = "Starting the download…";
        Stage = UpdateDialogStage.Downloading;
        var progress = new Progress<UpdateProgress>(update =>
        {
            IsProgressKnown = update.Percent.HasValue;
            Progress = update.Percent ?? 0;
            ProgressText = update.Total is { } total
                ? $"{Megabytes(update.Downloaded)} of {Megabytes(total)} MB"
                : $"{Megabytes(update.Downloaded)} MB";
        });
        try
        {
            await _install(progress, _cancellation.Token).ConfigureAwait(true);
            Stage = UpdateDialogStage.Ready;
        }
        catch (OperationCanceledException) when (_cancellation.IsCancellationRequested)
        {
            Stage = UpdateDialogStage.Available;
        }
        catch (Exception exception) when (exception is InvalidOperationException or IOException or UnauthorizedAccessException
                                              or HttpRequestException or InvalidDataException or TaskCanceledException)
        {
            Error = exception.Message;
            Stage = UpdateDialogStage.Failed;
        }
        finally
        {
            _cancellation.Dispose();
            _cancellation = null;
        }
    }

    public void CancelDownload() => _cancellation?.Cancel();

    private static string Megabytes(long bytes) => (bytes / 1024d / 1024d).ToString("0.0", CultureInfo.CurrentCulture);
}
