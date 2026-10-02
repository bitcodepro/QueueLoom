using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;

namespace QueueLoom.Tests;

public sealed class ImprovementUpdaterTests
{
    private static UpdateDialogViewModel Model(Func<IProgress<UpdateProgress>, CancellationToken, Task> install) =>
        new("1.5.5", new UpdateCheckResult(new Version(1, 6, 0), "v1.6.0", new Uri("https://example.test/release")), install);

    [Fact]
    public async Task GenericFailureDoesNotAssertCurrentFilesWereUnchanged()
    {
        var model = Model((_, _) => throw new IOException("installation failed"));
        await model.InstallAsync();
        Assert.DoesNotContain("left unchanged", model.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task QueuedProgressCannotReplaceTerminalProgress()
    {
        IProgress<UpdateProgress>? saved = null;
        var model = Model((progress, _) => { saved = progress; return Task.CompletedTask; });
        await model.InstallAsync();
        var terminal = model.ProgressText;
        saved!.Report(new UpdateProgress(123, 456));
        await Task.Delay(100);
        Assert.Equal(terminal, model.ProgressText);
    }

    [Fact]
    public async Task SafeRetryUsesFreshAttemptAndIgnoresOldProgressAndRepeatedClicks()
    {
        var calls = 0;
        IProgress<UpdateProgress>? first = null;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var model = Model(async (progress, _) =>
        {
            if (++calls == 1)
            {
                first = progress;
                throw new UpdateStageException(UpdatePhase.Verification, "Checksum mismatch", true, new IOException());
            }
            await gate.Task;
        });
        await model.InstallAsync();
        Assert.True(model.CanRetry);
        Assert.Equal(UpdatePhase.Verification, model.Phase);
        var retry = model.InstallAsync();
        await model.InstallAsync();
        Assert.Equal(2, calls);
        first!.Report(new UpdateProgress(400, 400) { Phase = UpdatePhase.Recovery });
        await Task.Delay(100);
        Assert.Equal(UpdatePhase.ChecksumFetch, model.Phase);
        gate.SetResult(); await retry;
        Assert.True(model.IsReady);
        Assert.False(model.CanRetry);
    }

    [Fact]
    public async Task RecoveryFailureBlocksRetryAndCannotClaimRollback()
    {
        var calls = 0;
        var model = Model((_, _) => { calls++; throw new UpdateStageException(UpdatePhase.Recovery, "Rollback incomplete", false, new IOException()); });
        await model.InstallAsync();
        Assert.False(model.CanRetry);
        await model.InstallAsync();
        Assert.Equal(1, calls);
        Assert.DoesNotContain("restored", model.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("not been verified", model.Message, StringComparison.Ordinal);
    }
}
