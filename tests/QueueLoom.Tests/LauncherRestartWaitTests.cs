using System.ComponentModel;
using System.Diagnostics;
using QueueLoom.Core.Updates;

namespace QueueLoom.Tests;

// The restart launcher waited at most two minutes for the closing application and then failed: an application slow to
// close (a large backup) closed, and no new version started. On Linux and macOS it did not check the process identity
// either, so a process that reused the PID was waited for. It now waits up to half an hour and checks an identity
// taken while the application is known to be alive: the kernel creation time on Windows, the boot-relative start
// ticks on Linux (wall-clock changes do not move them). Only a confirmed absence or a replaced identity ends the wait.
public sealed class LauncherRestartWaitTests
{
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan Long = TimeSpan.FromSeconds(10);

    [Fact]
    public void TheWaitIsLongEnoughForASlowClose() =>
        Assert.True(ApplicationExitWait.DefaultLimit >= TimeSpan.FromMinutes(30));

    // Each reading is a number (the identity read), "unreadable" (the read throws while the process is alive) or "gone".
    // The first reading is taken by Identify; the last one repeats.
    [Theory]
    [InlineData(ApplicationExitWait.Result.StillRunning, false, "unreadable", "5", "5")]   // identity learned late: still the application
    [InlineData(ApplicationExitWait.Result.StillRunning, false, "unreadable")]             // never readable: keep waiting
    [InlineData(ApplicationExitWait.Result.Exited, false, "unreadable", "gone")]           // confirmed disappearance
    [InlineData(ApplicationExitWait.Result.StillRunning, false, "5", "5")]                 // unchanged known identity
    [InlineData(ApplicationExitWait.Result.StillRunning, false, "5", "unreadable", "5")]   // a failed read in between proves nothing
    [InlineData(ApplicationExitWait.Result.Exited, false, "5", "6")]                       // genuine reuse
    [InlineData(ApplicationExitWait.Result.Exited, false, "5", "unreadable", "6")]         // reuse seen after a failed read
    [InlineData(ApplicationExitWait.Result.Exited, true, "unreadable", "6")]               // comparable reference (Windows): another process
    [InlineData(ApplicationExitWait.Result.StillRunning, true, "unreadable", "5")]         // comparable reference matches once readable
    [InlineData(ApplicationExitWait.Result.Exited, true, "6")]                             // comparable reference differs at once
    public async Task OnlyAConfirmedAbsenceOrAReplacedIdentityEndsTheWait(ApplicationExitWait.Result expected, bool comparable, params string[] readings)
    {
        var index = 0;
        long? Probe(int _)
        {
            var reading = readings[Math.Min(index++, readings.Length - 1)];
            return reading switch
            {
                "gone" => null,
                "unreadable" => throw new Win32Exception("process information disappeared"),
                _ => long.Parse(reading, System.Globalization.CultureInfo.InvariantCulture)
            };
        }

        // The current process stands in for a live application: an unreadable read finds it present.
        var watch = ApplicationExitWait.Identify(Environment.ProcessId, 5, Probe, comparable, TimeProvider.System);
        var result = await watch.WaitAsync(expected == ApplicationExitWait.Result.StillRunning ? TimeSpan.FromMilliseconds(400) : Long);

        Assert.Equal(expected, result);
    }

    // The process exits between the lookup and the identity read: the read fails and a lookup confirms the exit.
    [Fact]
    public async Task AFailedReadOfAFinishedProcessIsAnExit()
    {
        await HeldFixture.RunAsync(async fixture =>
        {
            var id = fixture.Id;
            await fixture.ReleaseAsync();

            Assert.Equal(ApplicationExitWait.ReadingKind.Gone,
                ApplicationExitWait.Inspect(_ => throw new Win32Exception("process information disappeared"), id).Kind);
            Assert.Equal(ApplicationExitWait.ReadingKind.Unreadable,
                ApplicationExitWait.Inspect(_ => throw new Win32Exception("access denied"), Environment.ProcessId).Kind);
        });
    }

    // A live application whose wall-clock start time reads differently (the clock was corrected) is still waited for on
    // Linux; on Windows the creation time is a kernel value and a different one means another process.
    [Fact]
    public async Task AWallClockDifferenceDoesNotEndTheWaitOnLinux()
    {
        using var self = Process.GetCurrentProcess();
        var shifted = self.StartTime.ToUniversalTime().AddMinutes(-5).Ticks;

        var result = await ApplicationExitWait.Identify(self.Id, shifted).WaitAsync(Short);

        Assert.Equal(OperatingSystem.IsWindows() ? ApplicationExitWait.Result.Exited : ApplicationExitWait.Result.StillRunning, result);
    }

    [Fact]
    public void LinuxStartTicksAreStable()
    {
        if (!OperatingSystem.IsLinux()) return;

        var first = ApplicationExitWait.LinuxStartTicks(Environment.ProcessId);

        Assert.NotNull(first);
        Assert.Equal(first, ApplicationExitWait.LinuxStartTicks(Environment.ProcessId));
    }

    // A real application, identified while it is held alive, then released: the wait sees it gone.
    [Fact]
    public async Task AReleasedApplicationIsSeenToExit()
    {
        await HeldFixture.RunAsync(async fixture =>
        {
            var watch = ApplicationExitWait.Identify(fixture.Id, fixture.StartTicks);
            Assert.Equal(ApplicationExitWait.Result.StillRunning, await watch.WaitAsync(Short));

            await fixture.ReleaseAsync();

            Assert.Equal(ApplicationExitWait.Result.Exited, await watch.WaitAsync(Long));
        });
    }

    // A holder nobody releases ends with the watchdog's failure code, never as if it had been released.
    [Fact]
    public async Task AForgottenHolderFailsVisibly()
    {
        await HeldFixture.RunAsync(async fixture => Assert.Equal(72, await fixture.WaitForExitCodeAsync()), watchdogMilliseconds: 300);
    }

    // A fixture that cannot get ready (its lock folder does not exist) fails the start and leaves nothing running.
    [Fact]
    public async Task AFixtureThatCannotGetReadyIsReportedAndStopped()
    {
        var error = await Assert.ThrowsAnyAsync<Exception>(() => HeldFixture.StartAsync(lockInMissingFolder: true));

        Assert.Contains("exited before it was ready", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The update fixture holding a lock until released, owned from the moment it starts: a failed start, a failed
    /// release or a failed test still stops it (within a bound), checks that it stopped, and only then removes its folder.
    /// A cleanup failure is reported without hiding the test's own failure.
    /// </summary>
    private sealed class HeldFixture
    {
        /// <summary>
        /// Runs a test body with a fixture it owns. A cleanup failure fails a passing test; after a failing body it is
        /// reported as a warning, so the body's own failure is the one shown.
        /// </summary>
        public static async Task RunAsync(Func<HeldFixture, Task> body, int? watchdogMilliseconds = null)
        {
            var fixture = await StartAsync(watchdogMilliseconds);
            Exception? bodyFailure = null;
            try
            {
                await body(fixture);
            }
            catch (Exception failure)
            {
                bodyFailure = failure;
                throw;
            }
            finally
            {
                if (await fixture.CleanUpAsync() is { } cleanup)
                {
                    if (bodyFailure is null) throw cleanup;
                    TestContext.Current.AddWarning("Fixture cleanup also failed: " + cleanup.Message);
                }
            }
        }

        private static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);
        private readonly Process _process;
        private readonly string _root;
        private readonly string _lock;

        private HeldFixture(Process process, string root, string lockFile)
        {
            _process = process;
            _root = root;
            _lock = lockFile;
        }

        public int Id => _process.Id;

        public long StartTicks { get; private init; }

        private static string Executable => Path.Combine(AppContext.BaseDirectory, "UpdateFixture",
            "QueueLoom.UpdateFixture" + (OperatingSystem.IsWindows() ? ".exe" : ""));

        public static async Task<HeldFixture> StartAsync(int? watchdogMilliseconds = null, bool lockInMissingFolder = false)
        {
            var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "exit-wait " + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(root);
            var lockFile = lockInMissingFolder ? Path.Combine(root, "missing", "app.lock") : Path.Combine(root, "app.lock");
            var start = new ProcessStartInfo(Executable) { UseShellExecute = false };
            foreach (var argument in new[] { "--hold-lock", lockFile, "until-released" }) start.ArgumentList.Add(argument);
            if (watchdogMilliseconds is { } watchdog) start.Environment["QUEUELOOM_FIXTURE_WATCHDOG_MS"] = watchdog.ToString(System.Globalization.CultureInfo.InvariantCulture);
            Process? process = null;
            try
            {
                process = Process.Start(start) ?? throw new InvalidOperationException("The fixture did not start.");
                var fixture = new HeldFixture(process, root, lockFile) { StartTicks = process.StartTime.ToUniversalTime().Ticks };
                try
                {
                    var waiting = Stopwatch.StartNew();
                    while (!File.Exists(lockFile + ".held"))
                    {
                        if (process.HasExited) throw new InvalidOperationException($"The fixture exited before it was ready ({process.ExitCode}).");
                        if (waiting.Elapsed > Bound) throw new TimeoutException("The fixture did not get ready.");
                        await Task.Delay(25);
                    }
                    return fixture;
                }
                catch (Exception failure)
                {
                    if (await fixture.CleanUpAsync() is { } cleanup)
                    {
                        throw new AggregateException("The fixture failed to start, and its cleanup failed too.", failure, cleanup);
                    }
                    throw;
                }
            }
            catch when (process is null)
            {
                Directory.Delete(root, recursive: true);
                throw;
            }
        }

        /// <summary>Releases the holder and requires a clean exit (code 0) within the bound.</summary>
        public async Task ReleaseAsync()
        {
            await File.WriteAllTextAsync(_lock + ".release", "go");
            Assert.Equal(0, await WaitForExitCodeAsync());
        }

        public async Task<int> WaitForExitCodeAsync()
        {
            using var deadline = new CancellationTokenSource(Bound);
            await _process.WaitForExitAsync(deadline.Token);
            return _process.ExitCode;
        }

        /// <summary>Stops the fixture within the bound and removes its folder once it has stopped; returns what failed.</summary>
        private async Task<Exception?> CleanUpAsync()
        {
            try
            {
                if (!_process.HasExited)
                {
                    _process.Kill(entireProcessTree: true);
                    using var deadline = new CancellationTokenSource(Bound);
                    try { await _process.WaitForExitAsync(deadline.Token); }
                    catch (OperationCanceledException)
                    {
                        return new InvalidOperationException($"Fixture {_process.Id} did not stop after being killed; its folder is kept: {_root}");
                    }
                }
                // Terminated: the folder can go.
                Directory.Delete(_root, recursive: true);
                return null;
            }
            catch (Exception failure) when (failure is IOException or UnauthorizedAccessException or InvalidOperationException or Win32Exception)
            {
                return failure;
            }
            finally
            {
                _process.Dispose();
            }
        }
    }
}
