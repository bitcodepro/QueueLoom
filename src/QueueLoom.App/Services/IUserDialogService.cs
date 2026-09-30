using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;

namespace QueueLoom.App.Services;

public interface IUserDialogService
{
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
    bool ReplacesConnectionString);
