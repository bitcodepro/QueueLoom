using Avalonia.Controls;
using Avalonia.Threading;
using QueueLoom.App.ViewModels;

namespace QueueLoom.App.Views;

public sealed partial class MessageDetailsView : UserControl
{
    public MessageDetailsView()
    {
        InitializeComponent();
        // After the new message has reached the tabs; switching during the change would show the old one.
        DataContextChanged += (_, _) => Dispatcher.UIThread.Post(ChooseBodyTab, DispatcherPriority.Loaded);
    }

    /// <summary>
    /// Shows the readable form of a packed body first: Body switches to Decoded when the new message has one, and back
    /// when it has none. A tab the operator chose (Properties, Dead-letter reason) is kept.
    /// </summary>
    private void ChooseBodyTab()
    {
        var hasDecoded = DataContext is MessageItemViewModel { HasDecodedBody: true };
        if (hasDecoded && Tabs.SelectedItem == BodyTab)
        {
            Tabs.SelectedItem = DecodedTab;
        }
        else if (!hasDecoded && Tabs.SelectedItem == DecodedTab)
        {
            Tabs.SelectedItem = BodyTab;
        }
    }
}
