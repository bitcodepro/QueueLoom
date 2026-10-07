using System.ComponentModel;
using System.Runtime.InteropServices;

namespace QueueLoom.Core.Updates;

/// <summary>Publish new names, never overwrite the stable entry or a committed state record.</summary>
internal static class DurableInstallFile
{
    internal static void WriteNew(string path, ReadOnlySpan<byte> bytes, Action? flushed = null)
    {
        var temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        // Interrupted temporary files are evidence; readers never consider them committed records.
        using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
        {
            stream.Write(bytes);
            stream.Flush(flushToDisk: true);
        }
        flushed?.Invoke();
        MoveNew(temporary, path);
    }

    internal static void MoveNew(string source, string destination)
    {
        if (File.Exists(destination) || Directory.Exists(destination)) throw new IOException("The installation name is already in use.");
        if (OperatingSystem.IsWindows())
        {
            if (!MoveFileEx(ExtendedWindowsPath(source), ExtendedWindowsPath(destination), 8 /* MOVEFILE_WRITE_THROUGH; no replacement or copy fallback */))
                throw new IOException("Publishing installation state failed.", new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        else
        {
            if (Directory.Exists(source)) Directory.Move(source, destination);
            else File.Move(source, destination);
            FlushDirectory(Path.GetDirectoryName(destination)!);
            if (Path.GetDirectoryName(source) != Path.GetDirectoryName(destination)) FlushDirectory(Path.GetDirectoryName(source)!);
        }
    }

    // FileStream normalizes long Windows paths; direct Win32 calls need the same extended syntax.
    private static string ExtendedWindowsPath(string path)
    {
        var full = Path.GetFullPath(path);
        if (full.StartsWith(@"\\?\", StringComparison.Ordinal)) return full;
        return full.StartsWith(@"\\", StringComparison.Ordinal)
            ? @"\\?\UNC\" + full[2..] : @"\\?\" + full;
    }

    internal static void FlushDirectory(string directory)
    {
        if (OperatingSystem.IsWindows()) return; // New names use the write-through rename above.
        var descriptor = Open(directory, 0);
        if (descriptor < 0) throw new IOException("Opening the installation directory for synchronization failed.");
        try
        {
            if (Fsync(descriptor) != 0) throw new IOException("Synchronizing installation metadata failed.");
        }
        finally { Close(descriptor); }
    }

    [DllImport("kernel32.dll", EntryPoint = "MoveFileExW", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool MoveFileEx(string source, string destination, int flags);
    [DllImport("libc", EntryPoint = "open", SetLastError = true)] private static extern int Open(string path, int flags);
    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)] private static extern int Fsync(int descriptor);
    [DllImport("libc", EntryPoint = "close")] private static extern int Close(int descriptor);
}
