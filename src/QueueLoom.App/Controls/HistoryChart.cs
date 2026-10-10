using System.Globalization;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Media;
using QueueLoom.Core.Monitoring;

namespace QueueLoom.App.Controls;

/// <summary>
/// A line chart of dead-letter counts over time: a filled line, three gridlines with counts, the period's start and
/// end below, and the exact count and time of the nearest sample under the pointer.
/// </summary>
public sealed class HistoryChart : Control
{
    public static readonly StyledProperty<IReadOnlyList<DeadLetterHistoryPoint>?> PointsProperty =
        AvaloniaProperty.Register<HistoryChart, IReadOnlyList<DeadLetterHistoryPoint>?>(nameof(Points));

    public static readonly StyledProperty<DateTimeOffset> FromProperty =
        AvaloniaProperty.Register<HistoryChart, DateTimeOffset>(nameof(From));

    public static readonly StyledProperty<DateTimeOffset> ToProperty =
        AvaloniaProperty.Register<HistoryChart, DateTimeOffset>(nameof(To));

    public static readonly StyledProperty<IBrush?> LineBrushProperty =
        AvaloniaProperty.Register<HistoryChart, IBrush?>(nameof(LineBrush));

    public static readonly StyledProperty<IBrush?> GridBrushProperty =
        AvaloniaProperty.Register<HistoryChart, IBrush?>(nameof(GridBrush));

    public static readonly StyledProperty<IBrush?> LabelBrushProperty =
        AvaloniaProperty.Register<HistoryChart, IBrush?>(nameof(LabelBrush));

    public static readonly StyledProperty<IBrush?> TooltipBrushProperty =
        AvaloniaProperty.Register<HistoryChart, IBrush?>(nameof(TooltipBrush));

    private const double LeftAxis = 44;
    private const double BottomAxis = 22;
    private const double TopPadding = 10;
    private const double RightPadding = 8;
    private Point? _pointer;

    static HistoryChart()
    {
        AffectsRender<HistoryChart>(PointsProperty, FromProperty, ToProperty, LineBrushProperty, GridBrushProperty,
            LabelBrushProperty, TooltipBrushProperty);
        FocusableProperty.OverrideDefaultValue<HistoryChart>(false);
    }

    public IReadOnlyList<DeadLetterHistoryPoint>? Points
    {
        get => GetValue(PointsProperty);
        set => SetValue(PointsProperty, value);
    }

    public DateTimeOffset From
    {
        get => GetValue(FromProperty);
        set => SetValue(FromProperty, value);
    }

    public DateTimeOffset To
    {
        get => GetValue(ToProperty);
        set => SetValue(ToProperty, value);
    }

    public IBrush? LineBrush
    {
        get => GetValue(LineBrushProperty);
        set => SetValue(LineBrushProperty, value);
    }

    public IBrush? GridBrush
    {
        get => GetValue(GridBrushProperty);
        set => SetValue(GridBrushProperty, value);
    }

    public IBrush? LabelBrush
    {
        get => GetValue(LabelBrushProperty);
        set => SetValue(LabelBrushProperty, value);
    }

    public IBrush? TooltipBrush
    {
        get => GetValue(TooltipBrushProperty);
        set => SetValue(TooltipBrushProperty, value);
    }

    /// <summary>A round axis maximum (1, 2, 5 × 10ⁿ steps) at or above the largest count, split into three steps.</summary>
    public static long AxisMaximum(long peak)
    {
        if (peak <= 3)
        {
            return 3;
        }

        var step = (double)peak / 3;
        var magnitude = Math.Pow(10, Math.Floor(Math.Log10(step)));
        var nice = new[] { 1d, 2d, 2.5d, 5d, 10d }.First(factor => factor * magnitude >= step) * magnitude;
        return (long)Math.Ceiling(nice) * 3;
    }

    protected override void OnPointerMoved(PointerEventArgs e)
    {
        base.OnPointerMoved(e);
        _pointer = e.GetPosition(this);
        InvalidateVisual();
    }

    protected override void OnPointerExited(PointerEventArgs e)
    {
        base.OnPointerExited(e);
        _pointer = null;
        InvalidateVisual();
    }

    public override void Render(DrawingContext context)
    {
        var points = Points;
        var line = LineBrush ?? Brushes.Teal;
        var grid = new Pen(GridBrush ?? Brushes.Gray, 1, DashStyle.Dash);
        var labels = LabelBrush ?? Brushes.Gray;
        var plot = new Rect(LeftAxis, TopPadding,
            Math.Max(1, Bounds.Width - LeftAxis - RightPadding), Math.Max(1, Bounds.Height - TopPadding - BottomAxis));
        var span = Math.Max(1, (To - From).Ticks);
        var maximum = AxisMaximum(points is { Count: > 0 } ? points.Max(point => point.Count) : 0);

        // Background hit area so the pointer is tracked across the whole plot.
        context.FillRectangle(Brushes.Transparent, new Rect(Bounds.Size));
        for (var step = 0; step <= 3; step++)
        {
            var gridY = plot.Bottom - plot.Height * step / 3;
            context.DrawLine(grid, new Point(plot.Left, gridY), new Point(plot.Right, gridY));
            var value = maximum * step / 3;
            var text = Text(value.ToString("N0", CultureInfo.CurrentCulture), labels, 11);
            context.DrawText(text, new Point(plot.Left - 8 - text.Width, gridY - text.Height / 2));
        }

        var format = TimeFormat(To - From);
        for (var tick = 0; tick <= 4; tick++)
        {
            var at = From + TimeSpan.FromTicks((To - From).Ticks * tick / 4);
            var label = Text(at.ToLocalTime().ToString(format, CultureInfo.CurrentCulture), labels, 11);
            var labelX = plot.Left + plot.Width * tick / 4 - (tick == 0 ? 0 : tick == 4 ? label.Width : label.Width / 2);
            context.DrawText(label, new Point(labelX, plot.Bottom + 5));
        }

        if (points is not { Count: > 0 })
        {
            return;
        }

        Point Position(DeadLetterHistoryPoint point) => new(
            plot.Left + plot.Width * Math.Clamp((double)(point.At - From).Ticks / span, 0, 1),
            plot.Bottom - plot.Height * point.Count / maximum);

        var positions = points.Select(Position).ToArray();
        var area = new StreamGeometry();
        using (var geometry = area.Open())
        {
            geometry.BeginFigure(new Point(positions[0].X, plot.Bottom), true);
            foreach (var position in positions)
            {
                geometry.LineTo(position);
            }
            geometry.LineTo(new Point(positions[^1].X, plot.Bottom));
            geometry.EndFigure(true);
        }
        var fill = line is ISolidColorBrush solid
            ? new LinearGradientBrush
            {
                StartPoint = new RelativePoint(0, 0, RelativeUnit.Relative),
                EndPoint = new RelativePoint(0, 1, RelativeUnit.Relative),
                GradientStops =
                {
                    new GradientStop(Color.FromArgb(90, solid.Color.R, solid.Color.G, solid.Color.B), 0),
                    new GradientStop(Color.FromArgb(0, solid.Color.R, solid.Color.G, solid.Color.B), 1)
                }
            }
            : (IBrush)Brushes.Transparent;
        context.DrawGeometry(fill, null, area);

        var stroke = new StreamGeometry();
        using (var geometry = stroke.Open())
        {
            geometry.BeginFigure(positions[0], false);
            foreach (var position in positions.Skip(1))
            {
                geometry.LineTo(position);
            }
            geometry.EndFigure(false);
        }
        var pen = new Pen(line, 2, lineJoin: PenLineJoin.Round, lineCap: PenLineCap.Round);
        context.DrawGeometry(null, pen, stroke);
        if (positions.Length == 1)
        {
            context.DrawEllipse(line, null, positions[0], 3.5, 3.5);
        }

        if (_pointer is not { } pointer || !plot.Inflate(6).Contains(pointer))
        {
            return;
        }

        var nearest = 0;
        for (var index = 1; index < positions.Length; index++)
        {
            if (Math.Abs(positions[index].X - pointer.X) < Math.Abs(positions[nearest].X - pointer.X))
            {
                nearest = index;
            }
        }

        var focus = positions[nearest];
        context.DrawLine(new Pen(GridBrush ?? Brushes.Gray, 1), new Point(focus.X, plot.Top), new Point(focus.X, plot.Bottom));
        context.DrawEllipse(TooltipBrush ?? Brushes.Black, new Pen(line, 2), focus, 4.5, 4.5);

        var sample = points[nearest];
        var count = Text($"{QueueLoom.Core.Monitoring.DeadLetterCountText.Format(sample.Count, sample.Quality)} dead letters",
            LineBrush ?? Brushes.White, 12, FontWeight.SemiBold);
        var time = Text(sample.At.ToLocalTime().ToString("ddd d MMM, HH:mm", CultureInfo.CurrentCulture), labels, 11);
        var width = Math.Max(count.Width, time.Width) + 16;
        var height = count.Height + time.Height + 10;
        var x = Math.Clamp(focus.X + 10, plot.Left, Math.Max(plot.Left, plot.Right - width));
        if (focus.X + 10 + width > plot.Right)
        {
            x = Math.Max(plot.Left, focus.X - 10 - width);
        }
        var y = Math.Clamp(focus.Y - height - 8, 0, Math.Max(0, plot.Bottom - height));
        var box = new Rect(x, y, width, height);
        context.DrawRectangle(TooltipBrush ?? Brushes.Black, new Pen(GridBrush ?? Brushes.Gray, 1), box, 6, 6);
        context.DrawText(count, new Point(box.X + 8, box.Y + 5));
        context.DrawText(time, new Point(box.X + 8, box.Y + 5 + count.Height));
    }

    /// <summary>Times of day for short periods, with the weekday once a period spans midnight, dates for weeks.</summary>
    public static string TimeFormat(TimeSpan span) =>
        span <= TimeSpan.FromHours(12) ? "HH:mm" : span <= TimeSpan.FromDays(2) ? "ddd HH:mm" : "d MMM";

    private static FormattedText Text(string text, IBrush brush, double size, FontWeight weight = FontWeight.Normal) =>
        new(text, CultureInfo.CurrentCulture, FlowDirection.LeftToRight,
            new Typeface(FontFamily.Default, FontStyle.Normal, weight), size, brush);
}
