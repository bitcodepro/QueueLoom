using Avalonia.Controls;
using Avalonia.Threading;
using ModelContextProtocol.Server;
using QueueLoom.App.ViewModels;
using QueueLoom.App.Views;
using QueueLoom.Mcp;

namespace QueueLoom.App.Mcp;

/// <summary>
/// Shows each requested change in a QueueLoom window on the user's desktop. The MCP client and the model
/// have no way to press its buttons, so an approval here always comes from a person.
/// </summary>
public sealed class DesktopApprover(TimeSpan? timeout = null) : IOperationApprover
{
    private readonly TimeSpan _timeout = timeout ?? TimeSpan.FromMinutes(5);
    private readonly SemaphoreSlim _oneAtATime = new(1, 1);

    /// <summary>Raised on the UI thread when an approval window opens; used by UI automation tests.</summary>
    public event Action<ConfirmDialogWindow>? WindowOpened;

    public async Task<ApprovalDecision> RequestAsync(ApprovalRequest request, McpServer server, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        await _oneAtATime.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var expiry = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            expiry.CancelAfter(_timeout);
            var approved = await Dispatcher.UIThread.InvokeAsync(() => ShowAsync(request, expiry.Token)).ConfigureAwait(false);
            if (approved)
            {
                return ApprovalDecision.Approve("Approved by the user in QueueLoom.");
            }

            return expiry.IsCancellationRequested && !cancellationToken.IsCancellationRequested
                ? ApprovalDecision.Deny($"Nobody approved the change within {_timeout.TotalMinutes:N0} minutes.")
                : ApprovalDecision.Deny("The user declined the change in QueueLoom.");
        }
        finally
        {
            _oneAtATime.Release();
        }
    }

    private async Task<bool> ShowAsync(ApprovalRequest request, CancellationToken cancellationToken)
    {
        var window = new ConfirmDialogWindow(new ConfirmDialogViewModel(
            $"Approve: {request.Action}",
            $"An AI assistant connected through MCP asks to change Service Bus.\n\n{request.Details}",
            isDangerous: true,
            requiredText: request.IsProduction ? request.EnvironmentName : null,
            confirmLabel: "Approve",
            cancelLabel: "Deny"))
        {
            Topmost = true,
            WindowStartupLocation = WindowStartupLocation.CenterScreen,
            ShowActivated = true
        };

        var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        window.Closed += (_, _) => closed.TrySetResult();
        await using var registration = cancellationToken.Register(() => Dispatcher.UIThread.Post(window.Close));
        window.Show();
        window.Activate();
        WindowOpened?.Invoke(window);
        await closed.Task.ConfigureAwait(true);
        return window.Confirmed && !cancellationToken.IsCancellationRequested;
    }
}
