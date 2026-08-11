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
}

public sealed record ProfileEditorResult(
    ServiceBusProfile Profile,
    string? ConnectionString,
    bool ReplacesConnectionString);
