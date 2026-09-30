using Avalonia.Controls;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Core.Profiles;

namespace QueueLoom.App.Services;

public sealed class WindowDialogService(TopLevelAccessor owner) : IUserDialogService
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
                new ConfirmDialogViewModel(
                    title,
                    message,
                    isDangerous,
                    requiredText,
                    confirmLabel: isDangerous ? "Confirm action" : "Continue")),
            cancellationToken);

    public async Task ShowMessageAsync(
        string title,
        string message,
        bool isError = false,
        CancellationToken cancellationToken = default)
    {
        await ShowDialogAsync<bool>(
                new ConfirmDialogWindow(
                    new ConfirmDialogViewModel(
                        title,
                        message,
                        isDangerous: false,
                        requiredText: null,
                        showCancel: false,
                        confirmLabel: "Close")),
                cancellationToken)
            .ConfigureAwait(true);
    }

    public Task<ResendOptions?> ChooseResendOptionsAsync(
        ResendDialogViewModel viewModel,
        CancellationToken cancellationToken = default) =>
        ShowDialogAsync<ResendOptions?>(new ResendDialogWindow(viewModel), cancellationToken);

    public async Task<string?> ChooseSaveFileAsync(
        string title,
        string suggestedFileName,
        IReadOnlyList<(string Name, string Pattern)> fileTypes,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var file = await owner.Window.StorageProvider.SaveFilePickerAsync(new FilePickerSaveOptions
        {
            Title = title,
            SuggestedFileName = suggestedFileName,
            ShowOverwritePrompt = true,
            FileTypeChoices = fileTypes
                .Select(type => new FilePickerFileType(type.Name) { Patterns = [type.Pattern] })
                .ToArray()
        }).ConfigureAwait(true);
        return file?.TryGetLocalPath();
    }

    public Task<bool> PromptForUpdateAsync(
        string version,
        CancellationToken cancellationToken = default) =>
        ShowDialogAsync<bool>(
            new ConfirmDialogWindow(
                new ConfirmDialogViewModel(
                    "QueueLoom update available",
                    $"QueueLoom {version} is available. Open the QueueLoom page on GitHub?",
                    isDangerous: false,
                    requiredText: null,
                    showCancel: true,
                    confirmLabel: "Open GitHub",
                    cancelLabel: "Not now")),
            cancellationToken);

    private async Task<T> ShowDialogAsync<T>(Window dialog, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var registration = cancellationToken.Register(
            static state => Dispatcher.UIThread.Post(((Window)state!).Close),
            dialog);
        var result = await dialog.ShowDialog<T>(owner.Window).ConfigureAwait(true);
        cancellationToken.ThrowIfCancellationRequested();
        return result;
    }
}
