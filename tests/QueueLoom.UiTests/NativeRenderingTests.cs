using Avalonia.Controls;
using Avalonia.Headless;
using Avalonia.Media;
using SkiaSharp;
using System.Reflection;

namespace QueueLoom.UiTests;

public sealed class NativeRenderingTests
{
    [Fact]
    public Task UpgradedNativeLibraries_RenderMultilingualTextToPng() => UiSession.RunAsync(() =>
    {
        // Shape through Avalonia/HarfBuzz, rasterize through Skia, then decode the actual PNG.
        Assert.Equal(4, typeof(SKBitmap).Assembly.GetName().Version!.Major);
        // HarfBuzz preserves AssemblyVersion 1.x for binary compatibility; use its file version.
        Assert.StartsWith("14.2.1.301", typeof(HarfBuzzSharp.Buffer).Assembly
            .GetCustomAttribute<AssemblyFileVersionAttribute>()!.Version, StringComparison.Ordinal);
        var window = new Window
        {
            Width = 600,
            Height = 120,
            Background = Brushes.White,
            Content = new TextBlock
            {
                Text = "QueueLoom — Українська — العربية — ffi",
                FontSize = 28,
                Foreground = Brushes.Black,
                Margin = new Avalonia.Thickness(16)
            }
        };
        try
        {
            window.Show();
            window.UpdateLayout();
            using var frame = window.CaptureRenderedFrame();
            Assert.NotNull(frame);
            using var png = new MemoryStream();
            frame.Save(png, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
            using var decoded = SKBitmap.Decode(png.ToArray());
            Assert.NotNull(decoded);
            Assert.True(decoded.Width >= 600);
            Assert.True(decoded.Pixels.Count(pixel => pixel.Red < 128 && pixel.Alpha > 0) > 100,
                "The rendered text must produce visible dark pixels on the white background.");
            var directory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
                File.WriteAllBytes(Path.Combine(directory, "native-multilingual.png"), png.ToArray());
            }
        }
        finally
        {
            window.Close();
        }
        return Task.CompletedTask;
    });
}
