using Avalonia;
using Avalonia.Controls;
using NavigationPage = QueueLoom.App.Models.NavigationPage;
using QueueLoom.App.Views.Pages;

namespace QueueLoom.App.Controls;

/// <summary>
/// Shows one page at a time. Pages are created on first visit and then cached, so an
/// unopened page costs nothing and a revisited page keeps its scroll and splitter state.
/// Pages inherit the host's data context.
/// </summary>
public sealed class PageHost : ContentControl
{
    public static readonly StyledProperty<NavigationPage> PageProperty =
        AvaloniaProperty.Register<PageHost, NavigationPage>(nameof(Page));

    private readonly Dictionary<NavigationPage, Control> _pages = [];

    public PageHost()
    {
        Content = GetOrCreate(Page);
    }

    public NavigationPage Page
    {
        get => GetValue(PageProperty);
        set => SetValue(PageProperty, value);
    }

    /// <summary>The pages created so far; exposed for UI tests.</summary>
    public IReadOnlyDictionary<NavigationPage, Control> CreatedPages => _pages;

    protected override void OnPropertyChanged(AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);
        if (change.Property == PageProperty)
        {
            Content = GetOrCreate(change.GetNewValue<NavigationPage>());
        }
    }

    private Control GetOrCreate(NavigationPage page)
    {
        if (!_pages.TryGetValue(page, out var control))
        {
            control = Create(page);
            _pages[page] = control;
        }
        return control;
    }

    private static Control Create(NavigationPage page) => page switch
    {
        NavigationPage.Overview => new OverviewPage(),
        NavigationPage.Explorer => new ExplorerPage(),
        NavigationPage.DeadLetters => new MessagesPage(),
        NavigationPage.Backups => new BackupsPage(),
        NavigationPage.Composer => new ComposerPage(),
        NavigationPage.Monitors => new MonitorsPage(),
        NavigationPage.Environments => new EnvironmentsPage(),
        NavigationPage.Activity => new ActivityPage(),
        _ => throw new ArgumentOutOfRangeException(nameof(page), page, "Unknown page.")
    };
}
