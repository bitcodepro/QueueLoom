using Avalonia.Data.Converters;
using Avalonia.Media;

namespace QueueLoom.App.Controls;

/// <summary>
/// QueueLoom's icon set: simple 24×24 filled geometries drawn for this app
/// (no third-party glyphs). Use with <c>PathIcon</c> or <c>{x:Static}</c>.
/// </summary>
public static class Icons
{
    /// <summary>Converts an icon name (for example a navigation page key) to its geometry.</summary>
    public static IValueConverter ByName { get; } = new FuncValueConverter<string?, Geometry?>(Find);

    public static Geometry? Find(string? name) => name switch
    {
        "Overview" => Overview,
        "Explorer" => Explorer,
        "DeadLetters" or "Messages" => Messages,
        "Backups" => Backups,
        "Composer" => Composer,
        "Monitors" => Monitors,
        "Environments" => Environments,
        "Activity" => Activity,
        "Theme" => Theme,
        "Search" => Search,
        "Plug" => Plug,
        "AzureServiceBus" => Azure,
        "AmazonSqsSns" => Aws,
        "GooglePubSub" => GoogleCloud,
        _ => null
    };

    public static Geometry Overview { get; } = Parse(
        "M3,3 H10.5 V10.5 H3 Z M13.5,3 H21 V10.5 H13.5 Z M3,13.5 H10.5 V21 H3 Z M13.5,13.5 H21 V21 H13.5 Z");

    public static Geometry Explorer { get; } = Parse(
        "M3,3.5 H8 V8.5 H3 Z M10,5 H21 V7 H10 Z M6,10 H10 V14 H6 Z M12,11 H21 V13 H12 Z M6,16 H10 V20 H6 Z M12,17 H21 V19 H12 Z M4.5,8.5 H6 V18.5 H4.5 Z");

    public static Geometry Messages { get; } = Parse(
        "F0 M2,5 H22 V19 H2 Z M4,7.4 V17 H20 V7.4 L12,13 Z");

    public static Geometry Backups { get; } = Parse(
        "F0 M2.5,3.5 H21.5 V9 H2.5 Z M3.5,10 H20.5 V20.5 H3.5 Z M9,12.5 H15 V14.5 H9 Z");

    public static Geometry Composer { get; } = Parse(
        "M2,21 L23,12 L2,3 V10 L17,12 L2,14 Z");

    public static Geometry Monitors { get; } = Parse(
        "M12,22 C13.1,22 14,21.1 14,20 H10 C10,21.1 10.9,22 12,22 Z M18,16 V11 C18,7.9 16.4,5.4 13.5,4.7 V4 C13.5,3.2 12.8,2.5 12,2.5 C11.2,2.5 10.5,3.2 10.5,4 V4.7 C7.6,5.4 6,7.9 6,11 V16 L4,18 V19 H20 V18 Z");

    public static Geometry Environments { get; } = Parse(
        "F0 M3,3.5 H21 V10 H3 Z M3,14 H21 V20.5 H3 Z M5.5,5.75 H8 V7.75 H5.5 Z M5.5,16.25 H8 V18.25 H5.5 Z M10.5,5.75 H18.5 V7.75 H10.5 Z M10.5,16.25 H18.5 V18.25 H10.5 Z");

    public static Geometry Activity { get; } = Parse(
        "F0 M12,2 A10,10 0 1 0 12,22 A10,10 0 1 0 12,2 Z M12,4 A8,8 0 1 1 12,20 A8,8 0 1 1 12,4 Z M11,6.5 H13 V11.6 L16.4,15 L15,16.4 L11,12.4 Z");

    public static Geometry Theme { get; } = Parse(
        "F0 M12,2 A10,10 0 1 0 12,22 A10,10 0 1 0 12,2 Z M12,4 A8,8 0 1 1 12,20 A8,8 0 1 1 12,4 Z M12,5.5 A6.5,6.5 0 0 1 12,18.5 Z");

    public static Geometry Search { get; } = Parse(
        "F0 M10,2.5 A7.5,7.5 0 1 0 10,17.5 A7.5,7.5 0 1 0 10,2.5 Z M10,4.5 A5.5,5.5 0 1 1 10,15.5 A5.5,5.5 0 1 1 10,4.5 Z M14.6,16 L16,14.6 L21.5,20.1 L20.1,21.5 Z");

    public static Geometry Plug { get; } = Parse(
        "M8,2 H10 V7 H14 V2 H16 V7 H18 V12 C18,14.8 16.1,17.1 13,17.8 V22 H11 V17.8 C7.9,17.1 6,14.8 6,12 V7 H8 Z");

    /// <summary>Provider marks: plain shapes that tell the clouds apart, not the vendors' logos.</summary>
    public static Geometry Azure { get; } = Parse(
        "M9.6,3 H14.2 L8.1,21 H2 Z M15.2,8.4 L22,21 H10.6 L14.4,17.3 H17.2 L13.4,11.6 Z");

    public static Geometry Aws { get; } = Parse(
        "M2.5,13.2 C7.6,17.4 16.4,17.4 21.5,13.2 L22.6,14.9 C16.8,19.9 7.2,19.9 1.4,14.9 Z M16.6,10.4 L22.8,11.2 L20.7,17 Z M5,4 H7.2 L9,9.4 L10.8,4 H13 L10,12 H8 Z");

    public static Geometry GoogleCloud { get; } = Parse(
        "F0 M12,1.8 L20.9,6.9 V17.1 L12,22.2 L3.1,17.1 V6.9 Z M12,7.2 A4.8,4.8 0 1 0 12,16.8 A4.8,4.8 0 1 0 12,7.2 Z");

    private static Geometry Parse(string data) => Geometry.Parse(data);
}
