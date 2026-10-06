using System.Diagnostics;
using System.Globalization;
using QueueLoom.Core.Updates;

namespace QueueLoom.Launcher;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            var installation = new VersionInstallation(Environment.ProcessPath ?? throw new IOException("The launcher path is unavailable."));
            if (args.Length > 0 && args[0] == "--launcher-restart")
            {
                if (args.Length != 4 || !Guid.TryParseExact(args[3], "N", out _)) throw new InvalidDataException("Invalid restart handoff.");
                installation.EnsureBootstrap();
                // Readiness precedes waiting for the application, so its orderly close can begin.
                var ready = Path.Combine(installation.Store, "restart-" + args[3] + ".ready");
                installation.RejectOwnedPath(ready);
                using (var file = new FileStream(ready, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                { file.WriteByte(1); file.Flush(flushToDisk: true); }
                try
                {
                    using var parent = Process.GetProcessById(int.Parse(args[1], CultureInfo.InvariantCulture));
                    var start = long.Parse(args[2], CultureInfo.InvariantCulture);
                    if (!OperatingSystem.IsWindows() || parent.StartTime.ToUniversalTime().Ticks == start)
                        await parent.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(2));
                }
                catch (ArgumentException) { /* Already stopped. */ }
                args = [];
            }
            var selection = installation.SelectForLaunch();
            installation.CleanupStaging();
            return await RunAsync(installation, selection, args);
        }
        catch (Exception error)
        {
            // Standard output belongs exclusively to MCP. No fallback guesses at an executable in a damaged store.
            await Console.Error.WriteLineAsync("QueueLoom could not launch: " + error.Message);
            return 1;
        }
    }

    private static async Task<int> RunAsync(VersionInstallation installation, LaunchSelection selection, string[] args)
    {
        var start = new ProcessStartInfo(selection.Executable)
        {
            UseShellExecute = false, WorkingDirectory = installation.Root, CreateNoWindow = true,
            RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true
        };
        foreach (var argument in args) start.ArgumentList.Add(argument);
        start.Environment[VersionInstallation.ContextLauncher] = installation.Launcher;
        start.Environment[VersionInstallation.ContextLauncherPid] = Environment.ProcessId.ToString(CultureInfo.InvariantCulture);
        start.Environment[VersionInstallation.ContextVersion] = selection.Version.Id;
        start.Environment[VersionInstallation.ContextManifest] = selection.Version.ManifestSha256;
        if (selection.Attempt is null) start.Environment.Remove(VersionInstallation.ContextAttempt);
        else start.Environment[VersionInstallation.ContextAttempt] = selection.Attempt;
        Process process;
        try { process = Process.Start(start) ?? throw new IOException("The application process could not start."); }
        catch (Exception failure) when (selection.OwnsAttempt && failure is System.ComponentModel.Win32Exception or IOException)
        {
            installation.Recover(selection);
            return await RunAsync(installation, installation.SelectForLaunch(), args);
        }
        using var child = process;
        var error = child.StandardError.BaseStream.CopyToAsync(Console.OpenStandardError());
        // A pending MCP candidate must acknowledge before consuming the client's first request or exposing
        // protocol bytes. Otherwise a failed startup could lose that request when falling back.
        if (selection.Attempt is not null)
        {
            var startup = Stopwatch.StartNew();
            while (!installation.HasAcknowledgement(selection) && !child.HasExited && startup.Elapsed < TimeSpan.FromSeconds(60))
                await Task.Delay(25);
            if (installation.HasAcknowledgement(selection)) installation.Confirm(selection);
            else if (selection.OwnsAttempt)
            {
                if (!child.HasExited) { child.Kill(entireProcessTree: true); await child.WaitForExitAsync(); }
                await error;
                installation.Recover(selection);
                // Reuse the same stable entry's verified known-good payload; preserve the caller's arguments.
                return await RunAsync(installation, installation.SelectForLaunch(), args);
            }
        }
        var output = child.StandardOutput.BaseStream.CopyToAsync(Console.OpenStandardOutput());
        var input = ForwardInputAsync(child);
        await child.WaitForExitAsync();
        await Task.WhenAll(output, error);
        // stdin may still be open at the client after the payload exits. Never hold the exit code waiting for it.
        _ = input.ContinueWith(task => _ = task.Exception, CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return child.ExitCode;
    }

    private static async Task ForwardInputAsync(Process child)
    {
        try
        {
            await Console.OpenStandardInput().CopyToAsync(child.StandardInput.BaseStream);
            child.StandardInput.Close();
        }
        catch (Exception error) when (error is IOException or ObjectDisposedException or InvalidOperationException) { }
    }
}
