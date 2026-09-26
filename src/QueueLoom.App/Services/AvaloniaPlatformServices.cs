using Avalonia.Controls;
using Avalonia.Controls.Notifications;
using Avalonia.Input.Platform;
using Avalonia.Platform.Storage;
using Microsoft.Extensions.Logging;

namespace QueueLoom.App.Services;

/// <summary>Resolves the window that hosts dialogs, clipboard access and toasts once it exists.</summary>
public sealed class TopLevelAccessor
{
    public TopLevel? Current { get; set; }

    public Window Window => Current as Window
        ?? throw new InvalidOperationException("The main window has not been created yet.");
}

public sealed class AvaloniaClipboardService(TopLevelAccessor topLevel, ILogger<AvaloniaClipboardService> logger)
    : IClipboardService
{
    public async Task<bool> SetTextAsync(string text)
    {
        var clipboard = topLevel.Current?.Clipboard;
        if (clipboard is null)
        {
            return false;
        }

        try
        {
            await clipboard.SetTextAsync(text).ConfigureAwait(true);
            return true;
        }
        catch (Exception exception)
        {
            logger.LogWarning(exception, "Clipboard write failed");
            return false;
        }
    }
}

public sealed class AvaloniaAppLauncher(TopLevelAccessor topLevel, ILogger<AvaloniaAppLauncher> logger) : IAppLauncher
{
    public async Task<bool> OpenUriAsync(Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        var launcher = topLevel.Current?.Launcher;
        if (launcher is null)
        {
            return false;
        }

        try
        {
            return await launcher.LaunchUriAsync(uri).ConfigureAwait(true);
        }
        catch (Exception exception)
        {
            logger.LogWarning(exception, "Could not open {Uri}", uri);
            return false;
        }
    }

    public async Task<bool> OpenFolderAsync(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var launcher = topLevel.Current?.Launcher;
        if (launcher is null)
        {
            return false;
        }

        try
        {
            var directory = Directory.CreateDirectory(path);
            return await launcher.LaunchDirectoryInfoAsync(directory).ConfigureAwait(true);
        }
        catch (Exception exception)
        {
            logger.LogWarning(exception, "Could not open folder {Path}", path);
            return false;
        }
    }
}

public sealed class WindowNotificationService(TopLevelAccessor topLevel) : INotificationService
{
    private static readonly TimeSpan DisplayTime = TimeSpan.FromSeconds(4);
    private WindowNotificationManager? _manager;

    public void Show(string title, string message, NotificationTone tone = NotificationTone.Information)
    {
        var host = topLevel.Current;
        if (host is null)
        {
            return;
        }

        if (_manager is null)
        {
            _manager = new WindowNotificationManager(host)
            {
                Position = NotificationPosition.BottomRight,
                MaxItems = 3
            };
            // The manager drops notifications until its template exists; apply it now so
            // the very first toast is not lost.
            _manager.ApplyTemplate();
        }
        _manager.Show(new Notification(title, message, ToNotificationType(tone), DisplayTime));
    }

    private static NotificationType ToNotificationType(NotificationTone tone) => tone switch
    {
        NotificationTone.Success => NotificationType.Success,
        NotificationTone.Warning => NotificationType.Warning,
        NotificationTone.Error => NotificationType.Error,
        _ => NotificationType.Information
    };
}
