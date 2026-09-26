namespace QueueLoom.App.Services;

public interface IClipboardService
{
    /// <summary>Copies text to the system clipboard; returns false when the clipboard is unavailable.</summary>
    Task<bool> SetTextAsync(string text);
}

public interface IAppLauncher
{
    /// <summary>Opens a web page in the default browser; returns false when the platform refused.</summary>
    Task<bool> OpenUriAsync(Uri uri);

    /// <summary>Opens a directory in the platform file manager; returns false when the platform refused.</summary>
    Task<bool> OpenFolderAsync(string path);
}

public enum NotificationTone
{
    Information,
    Success,
    Warning,
    Error
}

public interface INotificationService
{
    void Show(string title, string message, NotificationTone tone = NotificationTone.Information);
}

public interface IThemeService
{
    QueueLoom.Core.Settings.AppThemePreference Preference { get; }

    void Apply(QueueLoom.Core.Settings.AppThemePreference preference);
}
