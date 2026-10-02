using System.Globalization;
using QueueLoom.App.Services;
using QueueLoom.Core.Diagnostics;

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
    private int _running;
    private int _attempt;
    private UpdatePhase _phase = UpdatePhase.ChecksumFetch;
    private bool _canRetry;

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
    public bool CanRetry => IsFailed && _canRetry && CanInstall;
    public UpdatePhase Phase => _phase;
    public string StageLabel => Phase switch
    {
        UpdatePhase.ChecksumFetch => "Fetching checksum",
        UpdatePhase.Downloading => "Downloading package",
        UpdatePhase.Verification => "Verifying SHA-256 checksum",
        UpdatePhase.Extraction => "Extracting package",
        UpdatePhase.Installation => "Installing verified package",
        UpdatePhase.Restart => "Restarting and awaiting startup acknowledgement",
        _ => "Recovery requires inspection"
    };
    public bool CanCancel => IsDownloading && Phase is not (UpdatePhase.Installation or UpdatePhase.Restart or UpdatePhase.Recovery);

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
                OnPropertyChanged(nameof(CanRetry));
                OnPropertyChanged(nameof(CanCancel));
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
        UpdateDialogStage.Failed => $"Update stopped: {StageLabel}",
        _ => $"QueueLoom {NewVersion} is available"
    };

    public string Message => Stage switch
    {
        UpdateDialogStage.Downloading =>
            StageLabel + ". The package is checked against its published SHA-256 checksum before installation.",
        UpdateDialogStage.Ready =>
            "Installation finished. Update now requests an automatic restart. If this dialog remains open, use Restart now. Startup acknowledgement is still required before the previous files are cleaned up.",
        UpdateDialogStage.Failed =>
            CanRetry ? "This stage can be retried safely. Retry starts a fresh verified download." :
                "The installation state has not been verified. Retry is blocked while recovery may be required. Keep the update receipt and previous files, and inspect the failure before using the releases page.",
        _ => CanInstall
            ? $"You have {CurrentVersion}. QueueLoom downloads the new version from GitHub, checks it and installs it. " +
              "It restarts automatically after installation. Your environments, settings and backups are kept."
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
        if (_install is null || Stage == UpdateDialogStage.Ready || (IsFailed && !CanRetry) || Interlocked.CompareExchange(ref _running, 1, 0) != 0)
        {
            return;
        }

        _cancellation = new CancellationTokenSource();
        var cancellation = _cancellation;
        var attempt = Interlocked.Increment(ref _attempt);
        _canRetry = false;
        _phase = UpdatePhase.ChecksumFetch;
        Error = string.Empty;
        Progress = 0;
        IsProgressKnown = false;
        ProgressText = "Fetching checksum…";
        Stage = UpdateDialogStage.Downloading;
        var progress = new Progress<UpdateProgress>(update =>
        {
            if (Volatile.Read(ref _running) == 0 || attempt != Volatile.Read(ref _attempt) || !IsDownloading) return;
            if (update.Phase < _phase) return;
            _phase = update.Phase;
            OnPropertyChanged(nameof(Phase)); OnPropertyChanged(nameof(StageLabel)); OnPropertyChanged(nameof(Message)); OnPropertyChanged(nameof(CanCancel));
            IsProgressKnown = update.Percent.HasValue;
            Progress = update.Percent ?? 0;
            ProgressText = update.Phase != UpdatePhase.Downloading ? StageLabel : update.Total is { } total
                ? $"{Megabytes(update.Downloaded)} of {Megabytes(total)} MB"
                : $"{Megabytes(update.Downloaded)} MB";
        });
        try
        {
            await _install(progress, cancellation.Token).ConfigureAwait(true);
            ProgressText = "Installation finished; restart and startup acknowledgement pending.";
            Stage = UpdateDialogStage.Ready;
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            if (_phase is UpdatePhase.Installation or UpdatePhase.Restart or UpdatePhase.Recovery)
            { Error = "Interrupted during installation. Inspect the update receipt before retrying."; Stage = UpdateDialogStage.Failed; }
            else { ProgressText = "Cancelled before installation."; Stage = UpdateDialogStage.Available; }
        }
        catch (Exception exception)
        {
            if (exception is UpdateStageException failure) { _phase = failure.Phase; _canRetry = failure.SafeToRetry; }
            Error = SensitiveDataRedactor.SummarizeException(exception);
            OnPropertyChanged(nameof(StageLabel));
            Stage = UpdateDialogStage.Failed;
        }
        finally
        {
            Interlocked.Exchange(ref _running, 0);
            cancellation.Dispose();
            _cancellation = null;
        }
    }

    public void CancelDownload() { if (CanCancel) _cancellation?.Cancel(); }

    private static string Megabytes(long bytes) => (bytes / 1024d / 1024d).ToString("0.0", CultureInfo.CurrentCulture);
}
