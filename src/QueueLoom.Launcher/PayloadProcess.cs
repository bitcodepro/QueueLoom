using System.ComponentModel;
using System.Diagnostics;
using System.IO.Pipes;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace QueueLoom.Launcher;

/// <summary>Redirected payload lifetime, including Windows paths beyond CreateProcess's implicit module limit.</summary>
internal sealed class PayloadProcess : IDisposable
{
    private readonly Process? _managed;
    private readonly SafeProcessHandle? _native;
    private readonly int _pid;
    private readonly long _created;
    public Stream Input { get; }
    public Stream Output { get; }
    public Stream Error { get; }

    private PayloadProcess(Process process)
    {
        _managed = process;
        Input = process.StandardInput.BaseStream;
        Output = process.StandardOutput.BaseStream;
        Error = process.StandardError.BaseStream;
    }

    private PayloadProcess(SafeProcessHandle process, int pid, long created,
        Stream input, Stream output, Stream error)
    { _native = process; _pid = pid; _created = created; Input = input; Output = output; Error = error; }

    public static PayloadProcess Start(ProcessStartInfo start)
    {
        if (!OperatingSystem.IsWindows() || start.FileName.Length < 260)
            return new PayloadProcess(Process.Start(start) ?? throw new IOException("The application process could not start."));
        return StartWindows(start);
    }

    private static PayloadProcess StartWindows(ProcessStartInfo start)
    {
        var input = new AnonymousPipeServerStream(PipeDirection.Out, HandleInheritability.Inheritable);
        AnonymousPipeServerStream? output = null, error = null;
        SafeProcessHandle? process = null;
        try
        {
            output = new AnonymousPipeServerStream(PipeDirection.In, HandleInheritability.Inheritable);
            error = new AnonymousPipeServerStream(PipeDirection.In, HandleInheritability.Inheritable);
            foreach (var pipe in new[] { input, output, error })
            {
                // Keep parent pipe ends non-inheritable so they cannot keep stdin/stdout alive.
                // CreateProcess also inherits any other inheritable launcher handles.
                if (!SetHandleInformation(pipe.SafePipeHandle, 1, 0) || !SetHandleInformation(pipe.ClientSafePipeHandle, 1, 1))
                    throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            var info = new StartupInfo
            {
                Size = Marshal.SizeOf<StartupInfo>(), Flags = 0x100,
                Input = input.ClientSafePipeHandle.DangerousGetHandle(),
                Output = output.ClientSafePipeHandle.DangerousGetHandle(),
                Error = error.ClientSafePipeHandle.DangerousGetHandle()
            };
            var command = new StringBuilder(Quote(start.FileName));
            foreach (var argument in start.ArgumentList) command.Append(' ').Append(Quote(argument));
            var environment = string.Join('\0', start.Environment.Where(item => item.Value is not null)
                .OrderBy(item => item.Key, StringComparer.OrdinalIgnoreCase).Select(item => item.Key + "=" + item.Value)) + "\0\0";
            var block = Marshal.StringToHGlobalUni(environment);
            ProcessInfo child;
            try
            {
                var executable = start.FileName.StartsWith(@"\\?\", StringComparison.Ordinal) ? start.FileName
                    : start.FileName.StartsWith(@"\\", StringComparison.Ordinal) ? @"\\?\UNC\" + start.FileName[2..]
                    : @"\\?\" + start.FileName;
                // Process.Start passes a null application name, whose module token remains limited to MAX_PATH.
                // An explicit name supports the verified long path without changing installation identity or cwd.
                // lpCurrentDirectory is limited to MAX_PATH; the stable entry's installation.Root is assumed to stay below that limit.
                if (!CreateProcess(executable, command, 0, 0, true, 0x08000400, block, start.WorkingDirectory, ref info, out child))
                    throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            finally { Marshal.FreeHGlobal(block); }
            process = new SafeProcessHandle(child.Process, ownsHandle: true);
            CloseHandle(child.Thread);
            input.DisposeLocalCopyOfClientHandle();
            output.DisposeLocalCopyOfClientHandle();
            error.DisposeLocalCopyOfClientHandle();
            if (!GetProcessTimes(process, out var created, out _, out _, out _))
                throw new Win32Exception(Marshal.GetLastWin32Error());
            return new PayloadProcess(process, (int)child.Pid, created, input, output, error);
        }
        catch
        {
            if (process is not null)
            {
                TerminateProcess(process, 1);
                WaitForSingleObject(process, uint.MaxValue);
                process.Dispose();
            }
            input.DisposeLocalCopyOfClientHandle();
            output?.DisposeLocalCopyOfClientHandle();
            error?.DisposeLocalCopyOfClientHandle();
            input.Dispose(); output?.Dispose(); error?.Dispose();
            throw;
        }
    }

    private static string Quote(string argument)
    {
        var result = new StringBuilder("\"");
        var slashes = 0;
        foreach (var character in argument)
        {
            if (character == '\\') { slashes++; continue; }
            result.Append('\\', character == '"' ? slashes * 2 + 1 : slashes).Append(character);
            slashes = 0;
        }
        return result.Append('\\', slashes * 2).Append('"').ToString();
    }

    public bool HasExited => _managed?.HasExited ?? WaitForSingleObject(_native!, 0) == 0;
    public int ExitCode
    {
        get
        {
            if (_managed is not null) return _managed.ExitCode;
            if (!GetExitCodeProcess(_native!, out var code)) throw new Win32Exception(Marshal.GetLastWin32Error());
            return unchecked((int)code);
        }
    }
    public Task WaitForExitAsync() => _managed?.WaitForExitAsync() ?? Task.Run(() =>
    {
        if (WaitForSingleObject(_native!, uint.MaxValue) != 0) throw new Win32Exception(Marshal.GetLastWin32Error());
    });

    public void Kill()
    {
        if (_managed is not null) { _managed.Kill(entireProcessTree: true); return; }
        if (HasExited) return;
        try
        {
            using var child = Process.GetProcessById(_pid);
            // Bind any process-tree operation to the original native creation time, never a reused PID.
            if (GetProcessTimes(child.SafeHandle, out var created, out _, out _, out _) && created == _created)
                child.Kill(entireProcessTree: true);
        }
        catch (ArgumentException) when (HasExited) { }
        catch (InvalidOperationException) when (HasExited) { }
        if (!HasExited && !TerminateProcess(_native!, 1)) throw new Win32Exception(Marshal.GetLastWin32Error());
    }

    public void Dispose()
    {
        if (_managed is not null) { _managed.Dispose(); return; }
        Input.Dispose(); Output.Dispose(); Error.Dispose(); _native!.Dispose();
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct StartupInfo
    {
        public int Size;
        public nint Reserved, Desktop, Title;
        public int X, Y, Width, Height, Columns, Rows, Fill;
        public uint Flags;
        public ushort ShowWindow, ReservedBytes;
        public nint ReservedBuffer, Input, Output, Error;
    }
    [StructLayout(LayoutKind.Sequential)]
    private struct ProcessInfo { public nint Process, Thread; public uint Pid, ThreadId; }

    [DllImport("kernel32.dll", EntryPoint = "CreateProcessW", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CreateProcess(string applicationName, StringBuilder commandLine, nint processAttributes,
        nint threadAttributes, [MarshalAs(UnmanagedType.Bool)] bool inheritHandles, uint flags, nint environment,
        string directory, ref StartupInfo startup, out ProcessInfo process);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SetHandleInformation(SafePipeHandle handle, uint mask, uint flags);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint WaitForSingleObject(SafeProcessHandle handle, uint milliseconds);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetExitCodeProcess(SafeProcessHandle handle, out uint code);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetProcessTimes(SafeProcessHandle handle, out long creation, out long exit, out long kernel, out long user);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool TerminateProcess(SafeProcessHandle handle, uint code);
    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CloseHandle(nint handle);
}
