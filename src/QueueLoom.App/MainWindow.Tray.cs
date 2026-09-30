using System.ComponentModel;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Platform;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App;

/// <summary>
/// The system tray: with "Keep running in the tray" on, closing the window only hides it, so monitors and
/// scheduled resends go on. The tray menu opens the window again or quits.
/// </summary>
public sealed partial class MainWindow
{
    private TrayIcon? _tray;
    private bool _quitRequested;
    private bool _trayHintShown;

    /// <summary>True when the close was turned into hiding the window.</summary>
    private bool HideToTrayInsteadOfClosing()
    {
        if (_quitRequested || _viewModel is not { KeepInTray: true } || _tray is null)
        {
            return false;
        }
        Hide();
        if (!_trayHintShown)
        {
            _trayHintShown = true;
            _tray.ToolTipText = "QueueLoom keeps running here. " + _viewModel.TrayToolTip;
        }
        return true;
    }

    private void UpdateTray()
    {
        if (_viewModel is null || Application.Current is not { } application)
        {
            return;
        }
        if (!_viewModel.KeepInTray)
        {
            if (_tray is not null)
            {
                TrayIcon.SetIcons(application, null);
                _tray.Dispose();
                _tray = null;
            }
            return;
        }

        if (_tray is null)
        {
            var open = new NativeMenuItem("Open QueueLoom");
            open.Click += (_, _) => ShowFromTray();
            var quit = new NativeMenuItem("Quit QueueLoom");
            quit.Click += (_, _) => QuitFromTray();
            using var icon = AssetLoader.Open(new Uri("avares://QueueLoom/Assets/queueloom-128.png"));
            _tray = new TrayIcon
            {
                Icon = new WindowIcon(icon),
                Menu = new NativeMenu { Items = { open, new NativeMenuItemSeparator(), quit } }
            };
            _tray.Clicked += (_, _) => ShowFromTray();
            TrayIcon.SetIcons(application, [_tray]);
        }
        _tray.ToolTipText = _viewModel.TrayToolTip;
    }

    private void OnTrayRelevantPropertyChanged(object? sender, PropertyChangedEventArgs args)
    {
        if (args.PropertyName is nameof(MainWindowViewModel.KeepInTray) or nameof(MainWindowViewModel.IsMonitoring)
            or nameof(MainWindowViewModel.MonitorStatus))
        {
            UpdateTray();
        }
    }

    private void ShowFromTray()
    {
        Show();
        if (WindowState == WindowState.Minimized)
        {
            WindowState = WindowState.Normal;
        }
        Activate();
    }

    private void QuitFromTray()
    {
        _quitRequested = true;
        ShowFromTray();
        Close();
    }

    private void RemoveTray()
    {
        if (_tray is not null && Application.Current is { } application)
        {
            TrayIcon.SetIcons(application, null);
            _tray.Dispose();
            _tray = null;
        }
    }
}
