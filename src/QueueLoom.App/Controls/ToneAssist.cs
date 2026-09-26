using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.Documents;
using Avalonia.Controls.Primitives;
using Avalonia.Data;
using QueueLoom.App.Models;

namespace QueueLoom.App.Controls;

/// <summary>
/// Binds a control's brush to the theme resource of a semantic <see cref="Tone"/>.
/// The resource binding is dynamic, so a light/dark switch recolours rows immediately.
/// It binds at local-value priority so the tone wins over class styles such as <c>eyebrow</c>.
/// </summary>
public static class ToneAssist
{
    public static readonly AttachedProperty<Tone?> BackgroundProperty =
        AvaloniaProperty.RegisterAttached<Control, Tone?>("Background", typeof(ToneAssist));

    public static readonly AttachedProperty<Tone?> ForegroundProperty =
        AvaloniaProperty.RegisterAttached<Control, Tone?>("Foreground", typeof(ToneAssist));

    private static readonly AttachedProperty<IDisposable?> BackgroundSubscriptionProperty =
        AvaloniaProperty.RegisterAttached<Control, IDisposable?>("BackgroundSubscription", typeof(ToneAssist));

    private static readonly AttachedProperty<IDisposable?> ForegroundSubscriptionProperty =
        AvaloniaProperty.RegisterAttached<Control, IDisposable?>("ForegroundSubscription", typeof(ToneAssist));

    static ToneAssist()
    {
        BackgroundProperty.Changed.AddClassHandler<Control>((control, args) =>
            Rebind(control, args.GetNewValue<Tone?>(), BackgroundSubscriptionProperty, BackgroundTarget(control)));
        ForegroundProperty.Changed.AddClassHandler<Control>((control, args) =>
            Rebind(control, args.GetNewValue<Tone?>(), ForegroundSubscriptionProperty, ForegroundTarget(control)));
    }

    public static Tone? GetBackground(Control control) => control.GetValue(BackgroundProperty);

    public static void SetBackground(Control control, Tone? value) => control.SetValue(BackgroundProperty, value);

    public static Tone? GetForeground(Control control) => control.GetValue(ForegroundProperty);

    public static void SetForeground(Control control, Tone? value) => control.SetValue(ForegroundProperty, value);

    private static AvaloniaProperty? BackgroundTarget(Control control) => control switch
    {
        Border => Border.BackgroundProperty,
        Panel => Panel.BackgroundProperty,
        TemplatedControl => TemplatedControl.BackgroundProperty,
        _ => null
    };

    private static AvaloniaProperty? ForegroundTarget(Control control) => control switch
    {
        TextBlock => TextBlock.ForegroundProperty,
        TemplatedControl => TemplatedControl.ForegroundProperty,
        _ => TextElement.ForegroundProperty
    };

    private static void Rebind(
        Control control,
        Tone? tone,
        AttachedProperty<IDisposable?> subscriptionProperty,
        AvaloniaProperty? target)
    {
        control.GetValue(subscriptionProperty)?.Dispose();
        control.SetValue(subscriptionProperty, null);
        if (target is null || tone is null)
        {
            return;
        }

        var resource = control.GetResourceObservable(Tones.ResourceKey(tone.Value));
        control.SetValue(subscriptionProperty, control.Bind(target, resource, BindingPriority.LocalValue));
    }
}
