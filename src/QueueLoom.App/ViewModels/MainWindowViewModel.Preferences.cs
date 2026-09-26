using QueueLoom.App.Commands;
using QueueLoom.App.Services;
using QueueLoom.Core.Settings;

namespace QueueLoom.App.ViewModels;

public sealed partial class MainWindowViewModel
{
    private readonly IThemeService? _theme;
    private AppThemePreference _themePreference = AppThemePreference.Dark;

    public RelayCommand CycleThemeCommand { get; private set; } = null!;

    /// <summary>The operator's colour scheme choice; persisted by the shell.</summary>
    public AppThemePreference ThemePreference
    {
        get => _themePreference;
        set
        {
            if (SetProperty(ref _themePreference, value))
            {
                _theme?.Apply(value);
                OnPropertyChanged(nameof(ThemeLabel));
            }
        }
    }

    public string ThemeLabel => ThemePreference switch
    {
        AppThemePreference.Light => "Light theme",
        AppThemePreference.System => "System theme",
        _ => "Dark theme"
    };

    /// <summary>Applies stored preferences without treating them as user edits.</summary>
    public void ApplyPreferences(AppSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        MonitorIntervalSeconds = settings.MonitorIntervalSeconds;
        ThemePreference = settings.Theme;
    }

    private void InitializePreferences()
    {
        CycleThemeCommand = new RelayCommand(() => ThemePreference = ThemePreference switch
        {
            AppThemePreference.Dark => AppThemePreference.Light,
            AppThemePreference.Light => AppThemePreference.System,
            _ => AppThemePreference.Dark
        });
    }
}
