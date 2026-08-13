using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace QueueLoom.App.Models;

public sealed class NavigationItem(string key, string icon, string label) : INotifyPropertyChanged
{
    private int _alertCount;

    public string Key { get; } = key;
    public string Icon { get; } = icon;
    public string Label { get; } = label;

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
