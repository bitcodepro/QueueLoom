using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using Microsoft.Win32.SafeHandles;
using QueueLoom.Core.IO;

internal static class HandoffAcknowledgementFixture
{
    private static SafeFileHandle? held;
    private static string? caseRoot;
    private static string? mode;
    internal static int Start(QueueLoom.App.Services.UpdateTarget target)
    {
        try { return QueueLoom.App.Services.UpdateRestart.Start(target); }
        catch (Exception error)
        {
            var root = Environment.GetEnvironmentVariable("QL_PROBE_ROOT");
            if (root is not null) File.WriteAllText(Path.Combine(root, "reader-error.json"), JsonSerializer.Serialize(new
            { Pid = Environment.ProcessId, Type = error.GetType().FullName, HResult = $"0x{unchecked((uint)error.HResult):X8}", NativeCode = error.HResult & 0xffff, error.Message,
              FailedAtUtcTicks = DateTime.UtcNow.Ticks }));
            // The tests assert the code an unhandled exception gives (0xE0434352) and the complete error on stderr. Ending
            // here instead of through the runtime's crash path keeps both, without waiting on system crash handling
            // (error reporting), which on a busy runner kept the process alive well past the test's bound.
            Console.Error.WriteLine("Unhandled exception. " + error);
            Console.Error.Flush();
            Environment.Exit(unchecked((int)0xE0434352));
            throw; // not reached
        }
    }
    internal static void Configure(string[] args)
    {
        caseRoot = Environment.GetEnvironmentVariable("QL_PROBE_ROOT");
        mode = Environment.GetEnvironmentVariable("QL_PROBE_MODE");
        if (!OperatingSystem.IsWindows() || caseRoot is null || args.Length == 0 || args[0] != "--update-helper") return;
        var expected = Path.GetFullPath(Path.Combine(caseRoot, ".queueloom-update.json.helper"));
        var field = typeof(SafeFileWriter).GetField("BeforePublish", BindingFlags.NonPublic | BindingFlags.Static)!;
        var slot = (AsyncLocal<Action<string, string>?>)field.GetValue(null)!;
        slot.Value = (temporary, path) =>
        {
            if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException();
            if (Path.GetFullPath(path) != expected) return;
            var original = File.ReadAllText(temporary);
            if (mode == "wrong-pid") File.WriteAllText(temporary, "0");
            if (mode == "malformed-pid") File.WriteAllText(temporary, "not-a-pid");
            uint access = mode == "write" ? 0x40000000u : mode == "read" ? 0x80000000u : 0x00010000u;
            if (mode is not ("wrong-pid" or "malformed-pid" or "helper-exit"))
            {
                held = CreateFile(temporary, access, FileShare.Read | FileShare.Write | FileShare.Delete,
                    IntPtr.Zero, 3, 0x80, IntPtr.Zero);
                if (held.IsInvalid) throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());
                if (mode == "released") { held.Dispose(); held = null; }
            }
            using var self = Process.GetCurrentProcess();
            File.WriteAllText(Path.Combine(caseRoot, "holder.json"), JsonSerializer.Serialize(new
            {
                Pid = self.Id, StartTicks = self.StartTime.ToUniversalTime().Ticks, Executable = Environment.ProcessPath,
                Mode = mode, Temporary = temporary, Target = path, PublishedExpectedPid = original,
                RequestedAccess = mode is "wrong-pid" or "malformed-pid" or "helper-exit" ? "none" : $"0x{access:X8}", Share = "0x00000007 (READ|WRITE|DELETE)",
                HeldAcrossActualPublish = held is not null, Handle = held?.DangerousGetHandle().ToString("X")
            }));
            using var ready = EventWaitHandle.OpenExisting(Environment.GetEnvironmentVariable("QL_PROBE_READY")!);
            ready.Set();
            if (mode == "helper-exit")
            {
                using var blocked = EventWaitHandle.OpenExisting(Environment.GetEnvironmentVariable("QL_PROBE_RELEASE")!);
                if (!blocked.WaitOne(TimeSpan.FromSeconds(60))) throw new TimeoutException("Missing-ack diagnostic release was not signalled.");
            }
            // Return into the UNMODIFIED SafeFileWriter.Publish and its actual File.Move.
        };
    }
    internal static void AfterHelper(string[] args, int code)
    {
        if (!OperatingSystem.IsWindows() || caseRoot is null || args.Length == 0 || args[0] != "--update-helper") return;
        SafeFileWriter.WriteText(Path.Combine(caseRoot, "helper-finished.json"),
            JsonSerializer.Serialize(new { Pid = Environment.ProcessId, Code = code }), narrowGroup: false);
        using var release = EventWaitHandle.OpenExisting(Environment.GetEnvironmentVariable("QL_PROBE_RELEASE")!);
        if (!release.WaitOne(TimeSpan.FromSeconds(60))) throw new TimeoutException("Diagnostic release event was not signalled.");
        held?.Dispose();
    }
    internal static void RecordGui(string[] args)
    {
        var root = Environment.GetEnvironmentVariable("QL_PROBE_ROOT");
        if (root is null || args.Length == 0 || args[0] != "--update-startup") return;
        using var self = Process.GetCurrentProcess();
        SafeFileWriter.WriteText(Path.Combine(root, "gui.json"), JsonSerializer.Serialize(new
        { Pid = self.Id, StartTicks = self.StartTime.ToUniversalTime().Ticks, Executable = Environment.ProcessPath }), narrowGroup: false);
    }
    [DllImport("kernel32.dll", EntryPoint = "CreateFileW", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFile(string path, uint access, FileShare share, IntPtr security,
        uint creation, uint flags, IntPtr template);
}
