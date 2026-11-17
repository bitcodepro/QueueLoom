using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;

namespace QueueLoom.App.Services;

public interface IUserDialogService
{
    /// <summary>Shows the exact frozen export; false means cancel. Implementations fail closed by default.</summary>
    Task<bool> PreviewDiagnosticsAsync(DiagnosticsPreview preview, CancellationToken cancellationToken = default) =>
        Task.FromResult(false);

    Task<ProfileEditorResult?> EditProfileAsync(
        ServiceBusProfile? profile,
        CancellationToken cancellationToken = default);

    Task<bool> ConfirmAsync(
        string title,
        string message,
        bool isDangerous = false,
        string? requiredText = null,
        CancellationToken cancellationToken = default);

    Task ShowMessageAsync(
        string title,
        string message,
        bool isError = false,
        CancellationToken cancellationToken = default);

    /// <summary>Asks where and how to resend messages. Null means cancelled.</summary>
    Task<ResendOptions?> ChooseResendOptionsAsync(
        ResendDialogViewModel viewModel,
        CancellationToken cancellationToken = default) =>
        Task.FromResult<ResendOptions?>(viewModel.CanConfirm ? viewModel.ToOptions() : null);

    /// <summary>
    /// Shows the new-queue or queue-settings dialog. Returns a <see cref="QueueLoom.Core.ServiceBus.QueueDefinition"/>
    /// or <see cref="QueueLoom.Core.ServiceBus.QueueSettings"/>; null means cancelled.
    /// </summary>
    Task<object?> EditQueueAsync(QueueDialogViewModel viewModel, CancellationToken cancellationToken = default) =>
        Task.FromResult<object?>(viewModel.IsNew ? viewModel.TryBuildDefinition() : viewModel.TryBuildSettings());

    /// <summary>Shows a topic's subscriptions, their rules and where a message would go.</summary>
    Task ShowTopicRoutingAsync(TopicRoutingViewModel viewModel, CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <summary>Adds or changes a subscription rule. Null means cancelled.</summary>
    Task<QueueLoom.Core.Routing.SubscriptionRule?> EditRuleAsync(RuleEditorViewModel viewModel, CancellationToken cancellationToken = default) =>
        Task.FromResult(viewModel.TryBuild());

    /// <summary>Shows two messages side by side.</summary>
    Task ShowComparisonAsync(CompareDialogViewModel viewModel, CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <summary>Asks which file to open. Null means cancelled.</summary>
    Task<string?> ChooseOpenFileAsync(
        string title,
        IReadOnlyList<(string Name, string Pattern)> fileTypes,
        CancellationToken cancellationToken = default) =>
        Task.FromResult<string?>(null);

    /// <summary>Asks where to save a file. Null means cancelled.</summary>
    Task<string?> ChooseSaveFileAsync(
        string title,
        string suggestedFileName,
        IReadOnlyList<(string Name, string Pattern)> fileTypes,
        CancellationToken cancellationToken = default) =>
        Task.FromResult<string?>(null);
}

public sealed record ProfileEditorResult(
    ServiceBusProfile Profile,
    string? ConnectionString,
    bool ReplacesConnectionString)
{
    /// <summary>A new Kafka Schema Registry password to store, if one was entered.</summary>
    public string? SchemaRegistryPassword { get; init; }

    /// <summary>The registry no longer needs a password, so the stored one is removed.</summary>
    public bool RemovesSchemaRegistryPassword { get; init; }

    /// <summary>A new password of the client certificate (mutual TLS) to store, if one was entered.</summary>
    public string? ClientCertificatePassword { get; init; }

    /// <summary>The client certificate was removed, so its stored password is removed too.</summary>
    public bool RemovesClientCertificatePassword { get; init; }

    /// <summary>A new password of the environment's proxy to store, if one was entered.</summary>
    public string? ProxyPassword { get; init; }

    /// <summary>The proxy no longer signs in, so its stored password is removed.</summary>
    public bool RemovesProxyPassword { get; init; }
}
