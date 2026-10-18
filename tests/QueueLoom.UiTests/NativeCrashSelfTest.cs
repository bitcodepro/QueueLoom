using System.Runtime.InteropServices;

namespace QueueLoom.UiTests;

/// <summary>
/// Proves the emulator job's crash diagnostics: run by hand (workflow_dispatch, crash_diagnostics_self_test) it aborts
/// this test host natively, as the intermittent "double free" did, so the job must keep the dump, analyse it against
/// this apphost and still fail. In every other run it does nothing.
/// </summary>
public sealed class NativeCrashSelfTest
{
    [Fact]
    public void AbortsTheHostOnlyWhenAsked()
    {
        if (Environment.GetEnvironmentVariable("QUEUELOOM_NATIVE_CRASH_SELF_TEST") != "1" || !OperatingSystem.IsLinux())
        {
            return;
        }
        Abort();
    }

    [DllImport("libc", EntryPoint = "abort")]
    private static extern void Abort();
}
