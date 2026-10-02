using QueueLoom.App.Commands;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>
/// Subscription rules (Service Bus rules, SNS filter policies, Pub/Sub filters, RabbitMQ bindings): which messages
/// of a topic each subscription receives, where a message would go, and changing rules where the service allows it
/// and queue management and write access are on.
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
        OpenTopicRoutingCommand = _commands.Create(
            token => RunWorkspaceOperationAsync("Opening rules", ct => ShowRoutingAsync(SelectedEntityTopic!, null, null, ct), token),
            () => !IsBusy && SupportsRouting && SelectedEntityTopic is not null);
        CheckMessageRoutingCommand = _commands.Create(
            token => RunWorkspaceOperationAsync("Checking routing", ct => ShowRoutingAsync(SelectedMessageTopic!,
                SelectedMessage!.Message.CreateDraft(),
                $"{(SelectedMessage.IsDeadLetter ? "Dead letter" : "Message")} {SelectedMessage.MessageId} from {SelectedMessage.SourceDisplay}", ct), token),
            () => !IsBusy && SupportsRouting && SelectedMessageTopic is not null && SelectedMessage?.Message.IsBodyTruncated == false);
        CheckDraftRoutingCommand = _commands.Create(
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
        var service = _workspace.RoutingService;
        var editingNote = _workspace.RuleEditingNote;
        var canEdit = profile.AllowQueueManagement && CanWrite && editingNote is null;
        var rules = service == RoutingService.RabbitMq ? "bindings" : "rules";
        var hint = editingNote ?? (canEdit
            ? service switch
            {
                RoutingService.Sns => "SNS can take a few minutes to apply a changed filter policy; messages already delivered stay.",
                RoutingService.RabbitMq => "Binding changes apply at once to new messages; messages already in a queue stay.",
                _ => "Rule changes apply at once to new messages; messages already in a subscription stay."
            }
            : !profile.AllowQueueManagement
                ? $"To change {rules}, turn on \"Allow creating, changing and deleting queues\" for this environment."
                : $"Unlock write access to change {rules}.");
        var services = new TopicRoutingServices(
            token => _workspace.GetTopicRulesAsync(topic, token),
            async (subscription, rule, replace, token) =>
            {
                RequireManagementWriteAccess(profile);
                var reference = ServiceBusEntityReference.Subscription(topic, subscription);
                var text = rule.Name.Length == 0 || rule.Kind == RuleFilterKind.SnsFilterPolicy ? rule.FilterText : $"{rule.DisplayName}: {rule.FilterText}";
                RecordOperationIntent(replace ? "Change subscription rule started" : "Add subscription rule started", text, reference);
                await _workspace.SaveSubscriptionRuleAsync(topic, subscription, rule, replace, token).ConfigureAwait(true);
                AddActivity("Success", replace ? "Subscription rule changed" : "Subscription rule added",
                    $"{profile.Name} · {topic} / {subscription} · {text}", reference);
            },
            async (subscription, rule, token) =>
            {
                RequireManagementWriteAccess(profile);
                var reference = ServiceBusEntityReference.Subscription(topic, subscription);
                RecordOperationIntent("Delete subscription rule started", rule.DisplayName, reference);
                await _workspace.DeleteSubscriptionRuleAsync(topic, subscription, rule, token).ConfigureAwait(true);
                AddActivity("Warning", "Subscription rule deleted", $"{profile.Name} · {topic} / {subscription} · {rule.DisplayName}: {rule.FilterText}", reference);
            },
            editor => _dialogs.EditRuleAsync(editor, cancellationToken),
            (title, text, requiredText) => _dialogs.ConfirmAsync(title, text, isDangerous: true, requiredText: requiredText,
                cancellationToken: cancellationToken));
        var routing = new TopicRoutingViewModel(topic, profile.Name, canEdit, hint, services, message, origin, service);
        try { await _dialogs.ShowTopicRoutingAsync(routing, cancellationToken).ConfigureAwait(true); }
        finally { await routing.DisposeAsync().ConfigureAwait(true); }
        StatusText = routing.HasHeadline ? routing.Headline : $"Rules of {topic} reviewed";
    }
}
