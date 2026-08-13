namespace QueueLoom.App.ViewModels;

public sealed class ConfirmDialogViewModel : ObservableObject
{
    private string _confirmationText = string.Empty;

    public ConfirmDialogViewModel(
        string title,
        string message,
        bool isDangerous,
        string? requiredText,
        bool showCancel = true,
        string confirmLabel = "Continue",
        string cancelLabel = "Cancel")
    {
        Title = title;
        Message = message;
        IsDangerous = isDangerous;
        RequiredText = requiredText;
        ShowCancel = showCancel;
        ConfirmLabel = confirmLabel;
        CancelLabel = cancelLabel;
    }

    public string Title { get; }

    public string Message { get; }

    public bool IsDangerous { get; }

    public string? RequiredText { get; }

    public bool RequiresText => !string.IsNullOrEmpty(RequiredText);

    public string RequiredTextPrompt => RequiresText
        ? $"Type “{RequiredText}” to continue"
        : string.Empty;

    public bool ShowCancel { get; }

    public string ConfirmLabel { get; }

    public string CancelLabel { get; }

    public string ConfirmationText
    {
        get => _confirmationText;
        set
        {
            if (SetProperty(ref _confirmationText, value))
            {
                OnPropertyChanged(nameof(CanConfirm));
            }
        }
    }

    public bool CanConfirm => !RequiresText || string.Equals(
        ConfirmationText.Trim(),
        RequiredText,
        StringComparison.Ordinal);
}
