using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace QueueLoom.App.Models;

public sealed class NavigationItem(string key, string label, string shortcut) : INotifyPropertyChanged
{
    private int _alertCount;

    public string Key { get; } = key;

    /// <summary>Name of the icon in <c>Controls.Icons</c>; views resolve it to a geometry.</summary>
    public string Icon => Key;
    public string Label { get; } = label;

    /// <summary>Keyboard gesture that opens this page, shown in the tooltip.</summary>
    public string Shortcut { get; } = shortcut;

    public string ToolTip => $"{Label} ({Shortcut})";

    public int AlertCount
    {
        get => _alertCount;
        set
        {
            if (_alertCount == value)
            {
                return;
            }
            _alertCount = value;
            OnPropertyChanged();
            OnPropertyChanged(nameof(HasAlerts));
        }
    }

    public bool HasAlerts => AlertCount > 0;

    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged([CallerMemberName] string? name = null) =>
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}
