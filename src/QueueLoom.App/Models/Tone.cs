using QueueLoom.Core.Profiles;

namespace QueueLoom.App.Models;

/// <summary>
/// Semantic colour roles. Views map a tone to the active theme's brush, so view models
/// never carry concrete colours and light/dark themes stay consistent.
/// </summary>
public enum Tone
{
    Neutral,
    Accent,
    Violet,
    Success,
    Warning,
    Danger
}

public static class Tones
{
    public static Tone ForEnvironment(EnvironmentKind environment) => environment switch
    {
        EnvironmentKind.Development => Tone.Accent,
        EnvironmentKind.Test => Tone.Violet,
        EnvironmentKind.Production => Tone.Danger,
        _ => Tone.Warning
    };

    /// <summary>Maps a persisted environment label (backups store the display text) to a tone.</summary>
    public static Tone ForEnvironmentName(string? environment) => environment?.Trim().ToUpperInvariant() switch
    {
        "DEVELOPMENT" or "DEV" => Tone.Accent,
        "TEST" => Tone.Violet,
        "PRODUCTION" or "PROD" => Tone.Danger,
        _ => Tone.Warning
    };

    public static Tone ForActivityLevel(string? level) => level switch
    {
        "Error" => Tone.Danger,
        "Warning" => Tone.Warning,
        "Success" => Tone.Success,
        _ => Tone.Neutral
    };

    public static string ResourceKey(Tone tone) => tone switch
    {
        Tone.Accent => "AccentBrush",
        Tone.Violet => "VioletBrush",
        Tone.Success => "SuccessBrush",
        Tone.Warning => "WarningBrush",
        Tone.Danger => "DangerBrush",
        _ => "TextSecondaryBrush"
    };
}
