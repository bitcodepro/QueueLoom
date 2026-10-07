namespace QueueLoom.Core.Updates;

/// <summary>Validated context injected by the stable launcher, never an arbitrary data-directory hint.</summary>
public sealed class PayloadLaunch
{
    private static readonly Lazy<PayloadLaunch?> Context = new(ReadCurrent);
    private readonly VersionReference _version;
    private readonly string? _attempt;
    private PayloadLaunch(VersionInstallation installation, VersionReference version, string? attempt)
    { Installation = installation; _version = version; _attempt = attempt; }

    public static PayloadLaunch? Current => Context.Value;
    public VersionInstallation Installation { get; }
    public int LauncherProcessId
    {
        get
        {
            var pid = int.Parse(Environment.GetEnvironmentVariable(VersionInstallation.ContextLauncherPid) ?? "",
                System.Globalization.CultureInfo.InvariantCulture);
            using var process = System.Diagnostics.Process.GetProcessById(pid);
            if (process.HasExited || !string.Equals(process.MainModule?.FileName, Installation.Launcher,
                OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
                throw new InvalidDataException("The legacy handoff does not belong to the running launcher.");
            return pid;
        }
    }
    public bool Acknowledge()
    {
        if (_attempt is null) return false;
        Installation.Acknowledge(_version, _attempt);
        return true;
    }

    private static PayloadLaunch? ReadCurrent()
    {
        var launcher = Environment.GetEnvironmentVariable(VersionInstallation.ContextLauncher);
        if (string.IsNullOrEmpty(launcher)) return null;
        var installation = new VersionInstallation(launcher);
        var version = new VersionReference(Environment.GetEnvironmentVariable(VersionInstallation.ContextVersion) ?? "",
            Environment.GetEnvironmentVariable(VersionInstallation.ContextManifest) ?? "");
        var expected = installation.Verify(version);
        var actual = Environment.ProcessPath ?? throw new InvalidDataException("The payload process path is unavailable.");
        if (!string.Equals(ProcessIdentityPath(actual), ProcessIdentityPath(expected), OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
            throw new InvalidDataException("The launch context does not belong to this payload.");
        return new PayloadLaunch(installation, version, Environment.GetEnvironmentVariable(VersionInstallation.ContextAttempt));
    }

    private static string ProcessIdentityPath(string path)
    {
        path = Path.GetFullPath(path);
        // CreateProcess with an explicit extended path reports that same prefix in Environment.ProcessPath.
        // Normalize only equivalent DOS/UNC spellings; the verified installation and manifest remain authoritative.
        if (OperatingSystem.IsWindows())
        {
            if (path.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase)) return @"\\" + path[8..];
            if (path.StartsWith(@"\\?\", StringComparison.Ordinal) && path.Length >= 7 &&
                char.IsAsciiLetter(path[4]) && path[5] == ':' && path[6] == '\\') return path[4..];
        }
        return path;
    }

    public string ExternalBackupPath(string directory)
    {
        var full = Path.GetFullPath(directory);
        var relative = Path.GetRelativePath(Installation.Store, full);
        if (relative == "." || (!Path.IsPathRooted(relative) && relative != ".." && !relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal)))
            return Path.Combine(Installation.Root, "backups", "version-custom", relative == "." ? "root" : relative);
        return full;
    }
}
