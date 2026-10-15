using System.Diagnostics;
using System.Text.Json;
using QueueLoom.Core.Diagnostics;

namespace QueueLoom.Tests;

public sealed partial class UpdateRestartProcessTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SimulatedUnknownOwnershipFailsClosedAndPreservesPrimary(bool primaryFails)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows event/process ownership controls."); return; }
        var target = Installation();
        await LiveActors.Run(async actors =>
        {
            var actor = actors.Start(target.Executable);
            // Simulation at the inspection boundary, not a natural Win32 access-denied reproduction.
            _openCandidate = (candidate, root) => candidate.Id == actor.Process.Id
                ? throw new InvalidOperationException("SIMULATED unknown ownership before executable inspection.")
                : OwnedProcess.Open(candidate, root);
            try
            {
                var failure = await Record.ExceptionAsync(() => IsolatedAsync(() => primaryFails
                    ? throw new InvalidOperationException("Original test failure must remain primary.")
                    : Task.CompletedTask));
                Assert.NotNull(failure);
                Assert.Contains(primaryFails ? "Original test failure" : "SIMULATED unknown ownership", failure.Message, StringComparison.Ordinal);
                Assert.Contains("SIMULATED unknown ownership", _cleanupFailure?.Message, StringComparison.Ordinal);
                Assert.True(File.Exists(target.Executable));
                Assert.True(File.Exists(Path.Combine(_root, "QueueLoom.Core.dll")));
                Assert.False(actor.Process.HasExited);
                ActorLog($"SIMULATED unresolved ownership retained files and live actor {actor.Process.Id}; primary failure requested {primaryFails}. No native access-denied claim.");
            }
            finally { _openCandidate = OwnedProcess.Open; }
        });
    }

    [Fact]
    public async Task ActualActorExitIsObservedBeforeInstallationDeletion()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows event/process ownership controls."); return; }
        string installationRoot;
        var observedExit = false;
        await using (var installation = new UpdateRestartProcessTests())
        {
            installationRoot = installation._root;
            var target = installation.Installation();
            await LiveActors.Run(async actors =>
            {
                var actor = actors.Start(target.Executable);
                await installation.StopOwnedAsync();
                await actor.Process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                observedExit = actor.Process.HasExited;
                Assert.True(observedExit);
                Assert.True(File.Exists(target.Executable));
                ActorLog($"Actor {actor.Process.Id} actually exited {actor.Process.ExitCode}; installation still exists before DisposeAsync deletion.");
            });
        }
        Assert.True(observedExit);
        Assert.False(Directory.Exists(installationRoot));
    }

    [Fact]
    public async Task RenamedApphostKeepsBoundAssemblyFilesAndActuallyRestarts()
    {
        var (target, receipt) = ChangedInstallation();
        var templateDirectory = Path.GetDirectoryName(Fixture)!;
        foreach (var suffix in new[] { ".dll", ".deps.json", ".runtimeconfig.json" })
        {
            var boundName = "QueueLoom.UpdateFixture" + suffix;
            Assert.Equal(File.ReadAllBytes(Path.Combine(templateDirectory, boundName)), File.ReadAllBytes(Path.Combine(_root, boundName)));
            Assert.False(File.Exists(Path.Combine(_root, _candidateName + suffix)));
        }
        Assert.NotEqual(Path.GetFileName(Fixture), Path.GetFileName(target.Executable));
        var result = await Own(QueueLoom.App.Services.UpdateRestart.RunAsync(
            QueueLoom.App.Services.UpdateRestart.ReceiptPath(target), receipt.Id, int.MaxValue, 0));
        Assert.Equal(0, result);
        Assert.True(File.Exists(Path.Combine(_root, "updated-started.txt")));
        Assert.False(File.Exists(QueueLoom.App.Services.UpdateRestart.ReceiptPath(target)));
        ActorLog($"Real renamed apphost restart confirmed for {target.Executable}; original bound DLL/deps/runtimeconfig names and bytes preserved.");
    }

    [Fact]
    public async Task OtherLiveInstallationIsNotInspectedByThisInstallation()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Windows event/process ownership controls."); return; }
        await using var other = new UpdateRestartProcessTests();
        var ownTarget = Installation();
        var otherTarget = other.Installation();
        await LiveActors.Run(async actors =>
        {
            var own = actors.Start(ownTarget.Executable);
            var foreign = actors.Start(otherTarget.Executable);
            var inspected = new List<int>();
            _openCandidate = (candidate, root) =>
            {
                inspected.Add(candidate.Id);
                // Observe the actual boundary, without changing its ownership decision.
                return OwnedProcess.Open(candidate, root);
            };
            try
            {
                var ids = OwnedIds();
                Assert.Contains(own.Process.Id, ids);
                Assert.DoesNotContain(foreign.Process.Id, ids);
                Assert.False(own.Process.HasExited);
                Assert.False(foreign.Process.HasExited);
                ActorLog($"Inspected candidates [{string.Join(',', inspected)}]; own {own.Process.Id}; foreign {foreign.Process.Id}; foreign ownership rejected and both actors still live.");
                Assert.DoesNotContain(foreign.Process.Id, inspected);
                await StopOwnedAsync();
                await own.Process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                Assert.False(foreign.Process.HasExited);
            }
            finally { _openCandidate = OwnedProcess.Open; }
        });
    }

    private static void ActorLog(string message) =>
        TestContext.Current.TestOutputHelper?.WriteLine(SensitiveDataRedactor.Redact(message));

    /// <summary>Only real, test-created actors; named events provide readiness/release without sleep races.</summary>
    private sealed class LiveActors
    {
        private readonly List<LiveActor> _actors = [];

        public LiveActor Start(string executable, string? parentExecutable = null)
        {
            var actor = new LiveActor(executable);
            _actors.Add(actor); // Register before launch/readiness can fail.
            actor.Launch(parentExecutable);
            return actor;
        }

        public static async Task Run(Func<LiveActors, Task> body)
        {
            var actors = new LiveActors();
            Exception? primary = null;
            try { await body(actors); }
            catch (Exception error) { primary = error; throw; }
            finally
            {
                Exception? firstCleanupFailure = null;
                foreach (var actor in actors._actors.AsEnumerable().Reverse())
                {
                    try { await actor.Finish(); }
                    catch (Exception error) { firstCleanupFailure ??= error; ActorLog("Actor cleanup failure: " + error); }
                }
                if (primary is null && firstCleanupFailure is not null)
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(firstCleanupFailure).Throw();
            }
        }
    }

    private sealed class LiveActor
    {
        private readonly string _executable;
        private readonly string _readyName = "Local\\QueueLoomActorReady-" + Guid.NewGuid().ToString("N");
        private readonly string _releaseName = "Local\\QueueLoomActorRelease-" + Guid.NewGuid().ToString("N");
        private readonly string _identity;
        private readonly EventWaitHandle _ready;
        private readonly EventWaitHandle _release;
        private Process? _process;
        private OwnedProcess? _owned;
        private OwnedProcess? _ownedParent;
        public Process Process => _process!;
        public Process? Parent { get; private set; }

        public LiveActor(string executable)
        {
            if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException();
            _executable = executable;
            _identity = Path.Combine(Path.GetDirectoryName(executable)!, ".actor-" + Guid.NewGuid().ToString("N") + ".json");
            _ready = new EventWaitHandle(false, EventResetMode.ManualReset, _readyName);
            _release = new EventWaitHandle(false, EventResetMode.ManualReset, _releaseName);
        }

        public void Launch(string? parentExecutable)
        {
            var start = new ProcessStartInfo(parentExecutable ?? _executable)
            { UseShellExecute = false, CreateNoWindow = true, WorkingDirectory = Path.GetDirectoryName(parentExecutable ?? _executable)! };
            if (parentExecutable is not null)
            {
                start.ArgumentList.Add("--spawn-outside");
                start.ArgumentList.Add(_executable);
            }
            foreach (var argument in new[] { "--test-event-process", _readyName, _releaseName, _identity }) start.ArgumentList.Add(argument);
            var started = System.Diagnostics.Process.Start(start)!;
            _ = started.Handle;
            if (parentExecutable is null)
            {
                _process = started;
                _owned = Capture(started, _executable);
            }
            else
            {
                Parent = started;
                _ownedParent = Capture(started, parentExecutable);
            }
            Assert.True(_ready.WaitOne(TimeSpan.FromSeconds(30)), "Controlled actor did not signal readiness.");
            using var stream = new FileStream(_identity, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
            using var json = JsonDocument.Parse(stream);
            var identity = json.RootElement;
            if (_process is null)
            {
                _process = System.Diagnostics.Process.GetProcessById(identity.GetProperty("Pid").GetInt32());
                _ = _process.Handle;
            }
            Assert.Equal(identity.GetProperty("Pid").GetInt32(), _process.Id);
            Assert.Equal(identity.GetProperty("StartTicks").GetInt64(), _process.StartTime.ToUniversalTime().Ticks);
            Assert.Equal(_executable, identity.GetProperty("Executable").GetString());
            _owned ??= Capture(_process, _executable);
            ActorLog($"Ready actor PID {_process.Id}, birth {_process.StartTime.ToUniversalTime().Ticks}, executable {_executable}, retained process handle {_process.Handle}; parent {Parent?.Id}.");
        }

        private static OwnedProcess Capture(Process process, string executable) =>
            OwnedProcess.Open(process, Path.GetFullPath(Path.GetDirectoryName(executable)!) + Path.DirectorySeparatorChar)
            ?? throw new InvalidOperationException("The controlled actor was not owned by its test directory.");

        public async Task Finish()
        {
            try
            {
                _release.Set();
                if (_process is not null)
                {
                    try { await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10)); }
                    catch (TimeoutException) { _owned?.Stop(); throw; }
                    ActorLog($"Observed actual actor exit PID {_process.Id}, exit {_process.ExitCode} before installation deletion.");
                }
            }
            finally
            {
                try
                {
                    if (Parent is not null)
                    {
                        _ownedParent?.Stop();
                        await Parent.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                        ActorLog($"Observed actual parent exit PID {Parent.Id}, exit {Parent.ExitCode}.");
                    }
                }
                finally
                {
                    _owned?.Dispose(); _ownedParent?.Dispose();
                    _process?.Dispose(); Parent?.Dispose();
                    _ready.Dispose(); _release.Dispose();
                }
            }
        }
    }
}
