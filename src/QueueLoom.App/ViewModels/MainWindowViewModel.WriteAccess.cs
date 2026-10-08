using QueueLoom.App.Models;
using QueueLoom.Core.Profiles;

namespace QueueLoom.App.ViewModels;

/// <summary>Temporary write unlock for read-only and production environments.</summary>
public sealed partial class MainWindowViewModel
{
    public bool CanWrite => IsConnected &&
                            _connectedProfile?.CanWrite == true &&
                            // The operator's environment: an unlock that expires while a monitor check holds another
                            // environment shows read-only at once, before the relock gets the connection back.
                            (_writeUnlockProfileId != _connectedProfile?.Id || IsTemporaryWriteUnlockActive);

    private bool IsTemporaryWriteUnlockActive =>
        _writeUnlockExpiresAt is { } expiresAt && Clock.GetUtcNow() < expiresAt;

    public bool CanUnlockWrites => IsConnected && !CanWrite;

    public string WriteAccessLabel => CanWrite ? "WRITE ENABLED" : "READ ONLY";

    public Tone WriteAccessTone => CanWrite ? Tone.Warning : Tone.Neutral;

    private async Task UnlockWritesAsync(CancellationToken cancellationToken)
    {
        var selected = GetConnectedProfileItem();
        var confirmed = await _dialogs.ConfirmAsync(
            "Temporarily unlock writes",
            "Write access will be enabled for this local session for 10 minutes. Azure RBAC/SAS permissions still apply.",
            isDangerous: selected.IsProduction,
            requiredText: selected.IsProduction ? selected.Name : null,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            return;
        }

        var unlocked = selected.Profile with { AccessMode = ProfileAccessMode.ReadWrite };
        CancelWriteUnlockTimerWithoutWaiting();
        await _workspace.SetAccessModeAsync(ProfileAccessMode.ReadWrite, cancellationToken).ConfigureAwait(true);
        _connectedProfile = unlocked;
        var expiresAt = Clock.GetUtcNow().AddMinutes(10);
        _writeUnlockCancellation = new CancellationTokenSource();
        _writeUnlockProfileId = selected.Id;
        _writeUnlockExpiresAt = expiresAt;
        _writeUnlockTask = RelockAfterDelayAsync(selected.Id, expiresAt, _writeUnlockCancellation.Token);
        NotifyConnectionState();
        StatusText = "Write access unlocked for 10 minutes";
        AddActivity("Warning", "Writes unlocked", $"{selected.Name} · expires in 10 minutes");
    }

    private async Task RelockAfterDelayAsync(
        Guid profileId,
        DateTimeOffset expiresAt,
        CancellationToken cancellationToken)
    {
        try
        {
            var delay = expiresAt - Clock.GetUtcNow();
            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, Clock, cancellationToken).ConfigureAwait(true);
            }
            NotifyConnectionState();
            await _workspaceGate.WaitAsync(cancellationToken).ConfigureAwait(true);
            try
            {
                var profile = Profiles.FirstOrDefault(item => item.Id == profileId);
                if (profile is not null && _workspace.ConnectedProfileId == profileId)
                {
                    await _workspace.SetAccessModeAsync(profile.Profile.AccessMode, cancellationToken)
                        .ConfigureAwait(true);
                    _connectedProfile = profile.Profile;
                    NotifyConnectionState();
                    StatusText = "Temporary write access expired; environment is read-only";
                    AddActivity("Info", "Writes relocked", profile.Name);
                }
            }
            finally
            {
                _workspaceGate.Release();
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception exception)
        {
            ErrorText = SanitizeException(exception);
        }
        finally
        {
            if (_writeUnlockProfileId == profileId &&
                _writeUnlockExpiresAt == expiresAt &&
                (_workspace.ConnectedProfileId != profileId || _connectedProfile?.CanWrite != true))
            {
                ClearTemporaryWriteState();
            }
        }
    }

    private async Task StopWriteUnlockTimerAsync()
    {
        var cancellation = _writeUnlockCancellation;
        var task = _writeUnlockTask;
        cancellation?.Cancel();
        if (task is not null)
        {
            try
            {
                await task.ConfigureAwait(true);
            }
            catch (OperationCanceledException)
            {
            }
        }
        cancellation?.Dispose();
        if (ReferenceEquals(_writeUnlockCancellation, cancellation))
        {
            _writeUnlockCancellation = null;
            _writeUnlockTask = null;
        }
    }

    private WriteOperationCancellation CreateExpiryBoundedWriteCancellation(Guid profileId, CancellationToken token)
    {
        CancellationTokenSource? expiry = null;
        if (_writeUnlockProfileId == profileId && _writeUnlockExpiresAt is { } expiresAt)
        {
            var remaining = expiresAt - Clock.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
                throw new InvalidOperationException("Temporary write access expired before the operation started.");
            expiry = new CancellationTokenSource(remaining, Clock);
        }
        return new WriteOperationCancellation(token, expiry);
    }

    private sealed class WriteOperationCancellation(CancellationToken token, CancellationTokenSource? expiry) : IDisposable
    {
        private readonly CancellationTokenSource _linked = CancellationTokenSource.CreateLinkedTokenSource(token, expiry?.Token ?? default);
        public CancellationToken Token => _linked.Token;
        public void Dispose() { _linked.Dispose(); expiry?.Dispose(); }
    }

    private void CancelWriteUnlockTimerWithoutWaiting()
    {
        var cancellation = _writeUnlockCancellation;
        var task = _writeUnlockTask;
        _writeUnlockCancellation = null;
        _writeUnlockTask = null;
        _writeUnlockProfileId = null;
        _writeUnlockExpiresAt = null;
        cancellation?.Cancel();
        if (cancellation is not null)
        {
            _ = DisposeCancelledWriteTimerAsync(task, cancellation);
        }
    }

    private static async Task DisposeCancelledWriteTimerAsync(
        Task? task,
        CancellationTokenSource cancellation)
    {
        try
        {
            if (task is not null)
            {
                await task.ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            cancellation.Dispose();
        }
    }

    private void ClearTemporaryWriteState()
    {
        _writeUnlockProfileId = null;
        _writeUnlockExpiresAt = null;
        NotifyConnectionState();
    }
}
