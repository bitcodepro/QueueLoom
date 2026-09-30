using QueueLoom.App.Commands;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>
/// Azure Service Bus subscription rules: which messages of a topic each subscription receives, where a message
/// would go, and adding, changing or deleting rules when queue management and write access allow it.
/// </summary>
public sealed partial class MainWindowViewModel
{
    public AsyncRelayCommand OpenTopicRoutingCommand { get; private set; } = null!;

    public AsyncRelayCommand CheckMessageRoutingCommand { get; private set; } = null!;

    public AsyncRelayCommand CheckDraftRoutingCommand { get; private set; } = null!;

    public bool SupportsRouting => IsConnected && _workspace.SupportsSubscriptionRules;

    private string? SelectedEntityTopic => SelectedEntity switch
    {
        { IsTopic: true } topic => topic.Name,
        { IsSubscription: true } subscription => subscription.ParentPath,
        _ => null
    };

    private string? SelectedMessageTopic => SelectedMessage?.Message.Source is { Kind: ServiceBusEntityKind.Subscription } source &&
                                            SelectedMessage.ProfileId == ConnectedProfileId
        ? source.TopicName
        : null;

    private string? DraftTopic => SelectedDestination?.Reference is { Kind: ServiceBusEntityKind.Topic } topic ? topic.Name : null;

    private void InitializeRouting()
    {
        OpenTopicRoutingCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Opening rules", ct => ShowRoutingAsync(SelectedEntityTopic!, null, null, ct), token),
            () => !IsBusy && SupportsRouting && SelectedEntityTopic is not null);
        CheckMessageRoutingCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Checking routing", ct => ShowRoutingAsync(SelectedMessageTopic!,
                SelectedMessage!.Message.CreateDraft(),
                $"{(SelectedMessage.IsDeadLetter ? "Dead letter" : "Message")} {SelectedMessage.MessageId} from {SelectedMessage.SourceDisplay}", ct), token),
            () => !IsBusy && SupportsRouting && SelectedMessageTopic is not null && SelectedMessage?.Message.IsBodyTruncated == false);
        CheckDraftRoutingCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Checking routing", ct => ShowRoutingAsync(DraftTopic!, BuildDraft(), "The draft in Composer", ct), token),
            () => !IsBusy && SupportsRouting && DraftTopic is not null);
        PropertyChanged += (_, args) =>
        {
            if (args.PropertyName is nameof(IsBusy) or nameof(IsConnected) or nameof(SelectedEntity) or nameof(SelectedMessage)
                or nameof(SelectedDestination) or nameof(CanWrite))
            {
                OnPropertyChanged(nameof(SupportsRouting));
                OpenTopicRoutingCommand.NotifyCanExecuteChanged();
                CheckMessageRoutingCommand.NotifyCanExecuteChanged();
                CheckDraftRoutingCommand.NotifyCanExecuteChanged();
            }
        };
    }

    private async Task ShowRoutingAsync(string topic, MessageDraft? message, string? origin, CancellationToken cancellationToken)
    {
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect to an environment first.");
        var canEdit = profile.AllowQueueManagement && CanWrite;
        var hint = canEdit
            ? "Rule changes apply at once to new messages; messages already in a subscription stay."
            : !profile.AllowQueueManagement
                ? "To change rules, turn on \"Allow creating, changing and deleting queues\" for this environment."
                : "Unlock write access to change rules.";
        var services = new TopicRoutingServices(
            token => _workspace.GetTopicRulesAsync(topic, token),
            async (subscription, rule, replace, token) =>
            {
                var reference = ServiceBusEntityReference.Subscription(topic, subscription);
                RecordOperationIntent(replace ? "Change subscription rule started" : "Add subscription rule started", $"{rule.Name}: {rule.FilterText}", reference);
                await _workspace.SaveSubscriptionRuleAsync(topic, subscription, rule, replace, token).ConfigureAwait(true);
                AddActivity("Success", replace ? "Subscription rule changed" : "Subscription rule added",
                    $"{profile.Name} · {topic} / {subscription} · {rule.Name}: {rule.FilterText}", reference);
            },
            async (subscription, rule, token) =>
            {
                var reference = ServiceBusEntityReference.Subscription(topic, subscription);
                RecordOperationIntent("Delete subscription rule started", rule, reference);
                await _workspace.DeleteSubscriptionRuleAsync(topic, subscription, rule, token).ConfigureAwait(true);
                AddActivity("Warning", "Subscription rule deleted", $"{profile.Name} · {topic} / {subscription} · {rule}", reference);
            },
            editor => _dialogs.EditRuleAsync(editor, CancellationToken.None),
            (title, text, requiredText) => _dialogs.ConfirmAsync(title, text, isDangerous: true, requiredText: requiredText,
                cancellationToken: CancellationToken.None));
        var routing = new TopicRoutingViewModel(topic, profile.Name, canEdit, hint, services, message, origin);
        await _dialogs.ShowTopicRoutingAsync(routing, cancellationToken).ConfigureAwait(true);
        StatusText = routing.HasHeadline ? routing.Headline : $"Rules of {topic} reviewed";
    }
}
