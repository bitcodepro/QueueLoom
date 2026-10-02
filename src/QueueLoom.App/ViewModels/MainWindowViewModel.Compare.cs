using QueueLoom.App.Commands;

namespace QueueLoom.App.ViewModels;

/// <summary>Comparing the two ticked messages.</summary>
public sealed partial class MainWindowViewModel
{
    public AsyncRelayCommand CompareMarkedMessagesCommand { get; private set; } = null!;

    public bool CanCompareMarkedMessages => MarkedMessageCount == 2;

    private void InitializeCompare()
    {
        CompareMarkedMessagesCommand = _commands.Create(CompareMarkedMessagesAsync, () => CanCompareMarkedMessages);
    }

    private async Task CompareMarkedMessagesAsync(CancellationToken cancellationToken)
    {
        var marked = Messages.Where(message => message.IsMarked).Take(3).ToArray();
        if (marked.Length != 2)
        {
            ErrorText = "Tick exactly two messages to compare them.";
            return;
        }
        await _dialogs.ShowComparisonAsync(new CompareDialogViewModel(marked[0], marked[1]), cancellationToken).ConfigureAwait(true);
    }
}
