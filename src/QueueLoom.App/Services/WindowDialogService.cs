using Avalonia.Controls;
using Avalonia.Threading;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.Profiles;

namespace QueueLoom.App.Services;

public sealed class WindowDialogService(Window owner) : IUserDialogService
{
    public Task<ProfileEditorResult?> EditProfileAsync(
        ServiceBusProfile? profile,
        CancellationToken cancellationToken = default) =>
        ShowDialogAsync<ProfileEditorResult?>(
            new ProfileEditorWindow(new ProfileEditorViewModel(profile)),
            cancellationToken);

    public Task<bool> ConfirmAsync(
        string title,
        string message,
        bool isDangerous = false,
        string? requiredText = null,
        CancellationToken cancellationToken = default) =>
        ShowDialogAsync<bool>(
            new ConfirmDialogWindow(
                new ConfirmDialogViewModel(title, message, isDangerous, requiredText))
            , cancellationToken);

    public async Task ShowMessageAsync(
        string title,
        string message,
        bool isError = false,
        CancellationToken cancellationToken = default)
    {
        await ShowDialogAsync<bool>(
                new ConfirmDialogWindow(
                    new ConfirmDialogViewModel(title, message, isError, requiredText: null, showCancel: false)),
                cancellationToken)
            .ConfigureAwait(true);
    }

    public Task<bool> PromptForUpdateAsync(
        string version,
        CancellationToken cancellationToken = default) =>
        ShowDialogAsync<bool>(
            new ConfirmDialogWindow(
                new ConfirmDialogViewModel(
                    "QueueLoom update available",
                    $"QueueLoom {version} is available. Open its GitHub release page?",
                    isDangerous: false,
                    requiredText: null,
                    showCancel: true,
                    confirmLabel: "Open GitHub release",
                    cancelLabel: "Not now")),
            cancellationToken);

    private async Task<T> ShowDialogAsync<T>(Window dialog, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var registration = cancellationToken.Register(
            static state => Dispatcher.UIThread.Post(((Window)state!).Close),
            dialog);
        var result = await dialog.ShowDialog<T>(owner).ConfigureAwait(true);
        cancellationToken.ThrowIfCancellationRequested();
        return result;
    }
}
