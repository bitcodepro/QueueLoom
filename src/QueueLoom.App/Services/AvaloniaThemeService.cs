using Avalonia;
using Avalonia.Styling;
using QueueLoom.Core.Settings;

namespace QueueLoom.App.Services;

public sealed class AvaloniaThemeService : IThemeService
{
    public AppThemePreference Preference { get; private set; } = AppThemePreference.Dark;

    public void Apply(AppThemePreference preference)
    {
        Preference = preference;
        if (Application.Current is { } application)
        {
            application.RequestedThemeVariant = preference switch
            {
                AppThemePreference.Light => ThemeVariant.Light,
                AppThemePreference.Dark => ThemeVariant.Dark,
                _ => ThemeVariant.Default
            };
        }
    }
}
