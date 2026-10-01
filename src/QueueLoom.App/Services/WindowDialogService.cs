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

    public async Task<string?> ChooseOpenFileAsync(
        string title,
        IReadOnlyList<(string Name, string Pattern)> fileTypes,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var files = await owner.Window.StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
        {
            Title = title,
            AllowMultiple = false,
            FileTypeFilter = fileTypes
                .Select(type => new FilePickerFileType(type.Name) { Patterns = [type.Pattern] })
                .ToArray()
        }).ConfigureAwait(true);
        return files.Count == 0 ? null : files[0].TryGetLocalPath();
    }

    public Task<object?> EditQueueAsync(QueueDialogViewModel viewModel, CancellationToken cancellationToken = default) =>
        ShowDialogAsync<object?>(new QueueDialogWindow(viewModel), cancellationToken);

    public Task ShowTopicRoutingAsync(TopicRoutingViewModel viewModel, CancellationToken cancellationToken = default) =>
        ShowDialogAsync<object?>(new TopicRoutingWindow(viewModel), cancellationToken);

    public Task<QueueLoom.Core.Routing.SubscriptionRule?> EditRuleAsync(RuleEditorViewModel viewModel, CancellationToken cancellationToken = default) =>
        ShowDialogAsync<QueueLoom.Core.Routing.SubscriptionRule?>(new RuleEditorWindow(viewModel), cancellationToken);

    public Task ShowComparisonAsync(CompareDialogViewModel viewModel, CancellationToken cancellationToken = default) =>
        ShowDialogAsync<object?>(new CompareDialogWindow(viewModel), cancellationToken);

    public Task<UpdateDialogResult> ShowUpdateAsync(
        UpdateDialogViewModel viewModel,
        IAppLauncher? launcher,
        CancellationToken cancellationToken = default) =>
        ShowDialogAsync<UpdateDialogResult>(new UpdateDialogWindow(viewModel, launcher), cancellationToken);

    private async Task<T> ShowDialogAsync<T>(Window dialog, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var registration = cancellationToken.Register(
            static state => Dispatcher.UIThread.Post(((Window)state!).Close),
            dialog);
        // A dialog opened from another dialog (a rule editor from the routing window) belongs to that dialog.
        var parent = _openDialogs.Count > 0 ? _openDialogs[^1] : owner.Window;
        _openDialogs.Add(dialog);
        try
        {
            var result = await dialog.ShowDialog<T>(parent).ConfigureAwait(true);
            cancellationToken.ThrowIfCancellationRequested();
            return result;
        }
        finally
        {
            _openDialogs.Remove(dialog);
        }
    }

    private readonly List<Window> _openDialogs = [];
}
