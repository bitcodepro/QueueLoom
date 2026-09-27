using Avalonia;
using Avalonia.Controls;
using Avalonia.Data;
using Avalonia.Layout;
using Avalonia.Media;
using QueueLoom.Core.Profiles;

namespace QueueLoom.App.Controls;

/// <summary>
/// A small chip with the cloud's mark and short name (AZURE, AWS, GCP). It tells at a glance which service an
/// environment lives in, next to the coloured dot that tells its stage (dev, test, prod).
/// </summary>
public sealed class ProviderBadge : Border
{
    public static readonly StyledProperty<MessagingProvider?> ProviderProperty =
        AvaloniaProperty.Register<ProviderBadge, MessagingProvider?>(nameof(Provider));

    /// <summary>Shows the full service name ("Amazon SQS / SNS") instead of the short one.</summary>
    public static readonly StyledProperty<bool> ShowFullNameProperty =
        AvaloniaProperty.Register<ProviderBadge, bool>(nameof(ShowFullName));

    private readonly PathIcon _icon = new() { Width = 11, Height = 11, VerticalAlignment = VerticalAlignment.Center };
    private readonly TextBlock _label = new()
    {
        FontSize = 10.5,
        FontWeight = FontWeight.Bold,
        LetterSpacing = 0.6,
        VerticalAlignment = VerticalAlignment.Center
    };
    private readonly List<IDisposable> _bindings = [];

    public ProviderBadge()
    {
        CornerRadius = new CornerRadius(5);
        Padding = new Thickness(6, 2, 7, 2);
        BorderThickness = new Thickness(1);
        VerticalAlignment = VerticalAlignment.Center;
        HorizontalAlignment = HorizontalAlignment.Left;
        Child = new StackPanel
        {
            Orientation = Orientation.Horizontal,
            Spacing = 5,
            Children = { _icon, _label }
        };
        Update();
    }

    public MessagingProvider? Provider
    {
        get => GetValue(ProviderProperty);
        set => SetValue(ProviderProperty, value);
    }

    public bool ShowFullName
    {
        get => GetValue(ShowFullNameProperty);
        set => SetValue(ShowFullNameProperty, value);
    }

    public static string ResourcePrefix(MessagingProvider provider) => provider switch
    {
        MessagingProvider.AmazonSqsSns => "ProviderAws",
        MessagingProvider.GooglePubSub => "ProviderGcp",
        _ => "ProviderAzure"
    };

    protected override void OnPropertyChanged(AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);
        if (change.Property == ProviderProperty || change.Property == ShowFullNameProperty)
        {
            Update();
        }
    }

    private void Update()
    {
        foreach (var binding in _bindings)
        {
            binding.Dispose();
        }
        _bindings.Clear();

        IsVisible = Provider is not null;
        if (Provider is not { } provider)
        {
            return;
        }

        _icon.Data = Icons.Find(provider.ToString());
        _label.Text = ShowFullName ? provider.DisplayName() : provider.ShortName();
        ToolTip.SetTip(this, provider.DisplayName());
        Avalonia.Automation.AutomationProperties.SetName(this, provider.DisplayName());

        var prefix = ResourcePrefix(provider);
        _bindings.Add(Bind(BackgroundProperty, this.GetResourceObservable($"{prefix}SoftBrush"), BindingPriority.LocalValue));
        _bindings.Add(Bind(BorderBrushProperty, this.GetResourceObservable($"{prefix}Brush"), BindingPriority.LocalValue));
        _bindings.Add(_icon.Bind(PathIcon.ForegroundProperty, _icon.GetResourceObservable($"{prefix}Brush"), BindingPriority.LocalValue));
        _bindings.Add(_label.Bind(TextBlock.ForegroundProperty, _label.GetResourceObservable($"{prefix}Brush"), BindingPriority.LocalValue));
    }
}
