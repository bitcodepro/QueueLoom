using QueueLoom.App.Commands;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>A scheduled resend in the list on Activity.</summary>
public sealed class ScheduledResendItemViewModel : ObservableObject
{
    private string _status = string.Empty;

    public ScheduledResendItemViewModel(ScheduledResend resend, RelayCommand<ScheduledResendItemViewModel> runNow,
        RelayCommand<ScheduledResendItemViewModel> cancel)
    {
        Resend = resend;
        RunNow = runNow;
        Cancel = cancel;
    }

    public ScheduledResend Resend { get; }

    public RelayCommand<ScheduledResendItemViewModel> RunNow { get; }

    public RelayCommand<ScheduledResendItemViewModel> Cancel { get; }

    public string Title => $"{(Resend.Mode == ResendMode.Move ? "Move" : "Resend")} {Resend.Items.Count:N0} " +
                           $"message{(Resend.Items.Count == 1 ? string.Empty : "s")} to {Resend.DestinationDisplay}";

    public string When => $"{Resend.DueAt.ToLocalTime():ddd d MMM HH:mm} · {Resend.EnvironmentName}";

    /// <summary>Why it has not run yet, for example "Waiting for Staging to be connected with write access".</summary>
    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }
}
