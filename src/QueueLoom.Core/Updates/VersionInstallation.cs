using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;

namespace QueueLoom.Core.Updates;

public sealed record PayloadFile(string Sha256, int UnixMode);
public sealed record PayloadManifest(int Protocol, string Id, string Rid, string Version, Dictionary<string, PayloadFile> Files);
public sealed record BootstrapDescriptor(int Protocol, string Id, string Rid, string ManifestSha256, string? ArchiveSha256);
public sealed record VersionReference(string Id, string ManifestSha256);
public sealed record ActivationState(VersionReference Active, VersionReference Previous, bool Pending,
    string? Attempt = null, int OwnerPid = 0, long OwnerStartTicks = 0);
public sealed record LaunchSelection(string Executable, VersionReference Version, string? Attempt, bool OwnsAttempt);

/// <summary>
/// Protocol v1: immutable payloads and append-only state. The stable launcher and its bootstrap are never
/// replaced by an automatic update. Missing/torn state cannot remove the launch entry or select an arbitrary file.
/// </summary>
public sealed class VersionInstallation
{
    public const int Protocol = 1;
    public const string DescriptorName = "QueueLoom.bootstrap.json";
    public const string ArchiveName = "QueueLoom.bootstrap.zip";
    public const string ContextLauncher = "QUEUELOOM_LAUNCHER_PATH";
    public const string ContextVersion = "QUEUELOOM_PAYLOAD_ID";
    public const string ContextManifest = "QUEUELOOM_PAYLOAD_MANIFEST";
    public const string ContextAttempt = "QUEUELOOM_STARTUP_ATTEMPT";
    public const string ContextLauncherPid = "QUEUELOOM_LAUNCHER_PID";
    private sealed record StateEnvelope(ActivationState State, string Sha256);
    private readonly Action<string>? _checkpoint;
    private readonly BootstrapDescriptor _bootstrap;
    private readonly string _descriptorPath;
    private readonly string? _bundle;
    private readonly string _storeBoundary;

    public VersionInstallation(string launcher, Action<string>? checkpoint = null)
    {
        Launcher = Path.GetFullPath(launcher);
        _checkpoint = checkpoint;
        var executableDirectory = Path.GetDirectoryName(Launcher)!;
        var macOS = new DirectoryInfo(executableDirectory);
        _bundle = macOS.Name == "MacOS" && macOS.Parent?.Name == "Contents" &&
            macOS.Parent.Parent?.Name.EndsWith(".app", StringComparison.OrdinalIgnoreCase) == true
            ? macOS.Parent.Parent.FullName : null;
        Root = _bundle is null ? executableDirectory : Path.GetDirectoryName(_bundle)!;
        var adjacentStore = Path.Combine(Root, _bundle is null ? "QueueLoom.versions" : Path.GetFileNameWithoutExtension(_bundle) + ".versions");
        _descriptorPath = _bundle is null ? Path.Combine(Root, DescriptorName) : Path.Combine(_bundle, "Contents", "Resources", DescriptorName);
        RejectLinks(_descriptorPath, Root);
        _bootstrap = ReadDescriptor(_descriptorPath);
        if (_bootstrap.Rid != CurrentRid()) throw new InvalidDataException("The bootstrap belongs to another platform.");
        var dataOverride = Environment.GetEnvironmentVariable("QUEUELOOM_DATA_DIRECTORY");
        var localData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.DoNotVerify);
        if (string.IsNullOrWhiteSpace(localData) && string.IsNullOrWhiteSpace(dataOverride))
            throw new IOException("The operating system did not provide a local application-data path.");
        var userData = Path.GetFullPath(string.IsNullOrWhiteSpace(dataOverride) ? Path.Combine(localData, "QueueLoom") : dataOverride);
        var userStore = Path.Combine(userData, "launcher-installations", HashBytes(System.Text.Encoding.UTF8.GetBytes(
            OperatingSystem.IsWindows() ? Launcher.ToUpperInvariant() : Launcher)));
        // Keep an existing store authoritative, even if permissions later change. A fresh read-only portable
        // install bootstraps in per-user data instead of needing elevated access to Applications/Program Files.
        var useUserStore = Directory.Exists(userStore) || (!Directory.Exists(adjacentStore) && !CanWrite(Root));
        Store = useUserStore ? userStore : adjacentStore;
        _storeBoundary = useUserStore ? userData : Root;
    }

    public string Launcher { get; }
    public string Root { get; }
    public string Store { get; }
    public string? Bundle => _bundle;
    public string Rid => _bootstrap.Rid;
    public string DataAnchor => _bundle is null ? Root : Path.Combine(_bundle, "Contents", "MacOS");
    public VersionReference Bootstrap => new(_bootstrap.Id, _bootstrap.ManifestSha256);
    public bool HasPendingActivation()
    {
        using var ownership = Own();
        return ReadState().Pending;
    }

    public void RejectOwnedPath(string path)
    {
        var relative = Path.GetRelativePath(Store, Path.GetFullPath(path));
        var inStore = relative == "." || (!Path.IsPathRooted(relative) && relative != ".." &&
            !relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal));
        RejectLinks(path, inStore ? _storeBoundary : Root);
    }

    private static bool CanWrite(string directory)
    {
        try
        {
            using var probe = new FileStream(Path.Combine(directory, ".queueloom-write-" + Guid.NewGuid().ToString("N")),
                FileMode.CreateNew, FileAccess.Write, FileShare.None, 1, FileOptions.DeleteOnClose);
            return true;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException) { return false; }
    }
    // An emulated x64 launcher must keep using x64 payloads (Rosetta / Windows on ARM).
    public static string CurrentRid() => RuntimeInformation.ProcessArchitecture switch
    {
        Architecture.X64 when OperatingSystem.IsWindows() => "win-x64",
        Architecture.X64 when OperatingSystem.IsLinux() => "linux-x64",
        Architecture.X64 when OperatingSystem.IsMacOS() => "osx-x64",
        Architecture.Arm64 when OperatingSystem.IsMacOS() => "osx-arm64",
        _ => throw new PlatformNotSupportedException("This launcher platform is not supported.")
    };

    public static bool HasDescriptor(string launcher)
    {
        var directory = Path.GetDirectoryName(Path.GetFullPath(launcher))!;
        return File.Exists(Path.Combine(directory, DescriptorName)) ||
            File.Exists(Path.Combine(directory, "..", "Resources", DescriptorName));
    }

    private string VersionDirectory(VersionReference version)
    {
        ValidateReference(version);
        if (version.Id == _bootstrap.Id)
        {
            if (version.ManifestSha256 != _bootstrap.ManifestSha256) throw new InvalidDataException("Bootstrap identity was changed.");
            if (_bundle is not null) return Path.Combine(_bundle, "Contents", "Resources", "initial");
        }
        return Path.Combine(Store, "versions", version.Id);
    }

    private string PayloadExecutable(string directory) => _bootstrap.Rid.StartsWith("osx-", StringComparison.Ordinal)
        ? Path.Combine(directory, "payload", "QueueLoom.app", "Contents", "MacOS", "QueueLoom")
        : Path.Combine(directory, "payload", OperatingSystem.IsWindows() ? "QueueLoom.exe" : "QueueLoom");

    private FileStream Own()
    {
        RejectOwnedPath(Store);
        CreateDirectory(Store);
        RejectOwnedPath(Path.Combine(Store, "installation.lock"));
        var deadline = Stopwatch.StartNew();
        while (true)
        {
            try { return new FileStream(Path.Combine(Store, "installation.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None); }
            catch (IOException) when (deadline.Elapsed < TimeSpan.FromSeconds(30)) { Thread.Sleep(20); }
        }
    }

    private static void CreateDirectory(string path)
    {
        var parent = Path.GetDirectoryName(path)!;
        if (!Directory.Exists(parent)) CreateDirectory(parent);
        if (OperatingSystem.IsWindows()) Directory.CreateDirectory(path);
        else Directory.CreateDirectory(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        DurableInstallFile.FlushDirectory(parent);
    }

    public void EnsureBootstrap()
    {
        using var ownership = Own();
        EnsureBootstrapOwned();
    }

    private void EnsureBootstrapOwned()
    {
        var destination = VersionDirectory(Bootstrap);
        if (Directory.Exists(destination)) { Verify(Bootstrap); return; }
        if (_bundle is not null) throw new InvalidDataException("The initial application bundle is missing.");
        var archive = Path.Combine(Root, ArchiveName);
        RejectLinks(archive, Root);
        if (_bootstrap.ArchiveSha256 is null || Hash(archive) != _bootstrap.ArchiveSha256)
            throw new InvalidDataException("The initial payload archive is missing or damaged.");
        var staged = NewStaging();
        Extract(archive, staged);
        _checkpoint?.Invoke("bootstrap-copied");
        VerifyDirectory(staged, Bootstrap);
        _checkpoint?.Invoke("bootstrap-verified");
        PublishVersion(staged, Bootstrap);
        _checkpoint?.Invoke("bootstrap-published");
    }

    public void StagePackage(string packageDirectory)
    {
        using var ownership = Own();
        EnsureBootstrapOwned();
        var current = ReadState();
        if (current.Pending) throw new IOException("An update is awaiting startup confirmation; launch QueueLoom to complete or recover it.");
        var sourceDescriptor = _bundle is null ? Path.Combine(packageDirectory, DescriptorName)
            : Path.Combine(packageDirectory, "QueueLoom.app", "Contents", "Resources", DescriptorName);
        RejectLinks(sourceDescriptor, Path.GetFullPath(packageDirectory));
        var descriptor = ReadDescriptor(sourceDescriptor);
        if (descriptor.Rid != Rid) throw new InvalidDataException("The update belongs to another platform.");
        var version = new VersionReference(descriptor.Id, descriptor.ManifestSha256);
        if (version.Id == current.Active.Id) throw new IOException("This application version is already installed.");
        var staged = NewStaging();
        if (_bundle is null)
        {
            var archive = Path.Combine(packageDirectory, ArchiveName);
            RejectLinks(archive, Path.GetFullPath(packageDirectory));
            if (descriptor.ArchiveSha256 is null || Hash(archive) != descriptor.ArchiveSha256)
                throw new InvalidDataException("The update payload archive is damaged.");
            Extract(archive, staged);
        }
        else CopyTree(Path.Combine(Path.GetDirectoryName(sourceDescriptor)!, "initial"), staged,
            Path.GetFullPath(packageDirectory));
        _checkpoint?.Invoke("stage-copied");
        VerifyDirectory(staged, version);
        _checkpoint?.Invoke("stage-verified");
        PublishVersion(staged, version);
        _checkpoint?.Invoke("version-published");
        // Pin the previous verified known-good version before exposing the new candidate.
        Verify(current.Active);
        Commit(new ActivationState(version, current.Active, true), "activate");
        _checkpoint?.Invoke("activation-published");
    }

    private string NewStaging()
    {
        var parent = Path.Combine(Store, "staging");
        RejectOwnedPath(parent);
        CreateDirectory(parent);
        var id = Guid.NewGuid().ToString("N");
        var path = Path.Combine(parent, id);
        CreateDirectory(path);
        DurableInstallFile.WriteNew(Path.Combine(path, ".staging-owner"), System.Text.Encoding.ASCII.GetBytes(id));
        return path;
    }

    private void PublishVersion(string staged, VersionReference version)
    {
        var destination = VersionDirectory(version);
        RejectOwnedPath(destination);
        CreateDirectory(Path.GetDirectoryName(destination)!);
        if (Directory.Exists(destination)) { Verify(version); return; }
        DurableInstallFile.MoveNew(staged, destination);
    }

    public LaunchSelection SelectForLaunch()
    {
        using var ownership = Own();
        EnsureBootstrapOwned();
        var state = ReadState();
        try { Verify(state.Active); }
        catch (Exception error) when (error is IOException or InvalidDataException or UnauthorizedAccessException or JsonException)
        { state = RecoverOwned(state); }
        if (!state.Pending) return new(PayloadExecutable(VersionDirectory(state.Active)), state.Active, null, false);
        if (state.Attempt is not null && HasAcknowledgement(state))
        {
            state = ConfirmOwned(state);
            return new(PayloadExecutable(VersionDirectory(state.Active)), state.Active, null, false);
        }
        if (state.Attempt is not null && !OwnerAlive(state))
        {
            state = RecoverOwned(state);
            return new(PayloadExecutable(VersionDirectory(state.Active)), state.Active, null, false);
        }
        var ownsAttempt = state.Attempt is null;
        if (ownsAttempt)
        {
            using var process = Process.GetCurrentProcess();
            state = state with { Attempt = Guid.NewGuid().ToString("N"), OwnerPid = process.Id,
                OwnerStartTicks = process.StartTime.ToUniversalTime().Ticks };
            Commit(state, "attempt");
            _checkpoint?.Invoke("attempt-published");
        }
        return new(PayloadExecutable(VersionDirectory(state.Active)), state.Active, state.Attempt, ownsAttempt);
    }

    public bool HasAcknowledgement(LaunchSelection selection)
    {
        if (selection.Attempt is null) return true;
        return HasAcknowledgement(new ActivationState(selection.Version, Bootstrap, true, selection.Attempt));
    }

    private bool HasAcknowledgement(ActivationState state)
    {
        if (state.Attempt is null) return false;
        var path = AcknowledgementPath(state.Attempt);
        RejectOwnedPath(path);
        try { return File.ReadAllText(path) == state.Active.Id + ":" + state.Active.ManifestSha256; }
        catch (IOException) { return false; }
    }

    public void Acknowledge(VersionReference version, string attempt)
    {
        using var ownership = Own();
        var state = ReadState();
        if (!state.Pending && state.Active == version && HasAcknowledgement(new ActivationState(version, Bootstrap, true, attempt)))
        {
            Verify(version); // A second legitimate payload can finish acknowledgement after confirmation.
            return;
        }
        if (!state.Pending || state.Attempt != attempt || state.Active != version)
            throw new InvalidDataException("Startup acknowledgement does not match the active attempt.");
        Verify(version);
        var path = AcknowledgementPath(attempt);
        RejectOwnedPath(path);
        CreateDirectory(Path.GetDirectoryName(path)!);
        if (!File.Exists(path)) DurableInstallFile.WriteNew(path,
            System.Text.Encoding.ASCII.GetBytes(version.Id + ":" + version.ManifestSha256));
        if (!HasAcknowledgement(state)) throw new InvalidDataException("Startup acknowledgement is damaged.");
        _checkpoint?.Invoke("ack-published");
    }

    private string AcknowledgementPath(string attempt)
    {
        if (!IsId(attempt)) throw new InvalidDataException("Invalid startup attempt.");
        return Path.Combine(Store, "acknowledgements", attempt + ".ready");
    }

    public void Confirm(LaunchSelection selection)
    {
        using var ownership = Own();
        var state = ReadState();
        if (state.Pending && state.Active == selection.Version && state.Attempt == selection.Attempt && HasAcknowledgement(state))
            ConfirmOwned(state);
    }

    private ActivationState ConfirmOwned(ActivationState state)
    {
        var confirmed = new ActivationState(state.Active, state.Previous, false);
        Commit(confirmed, "confirm");
        _checkpoint?.Invoke("confirmation-published");
        return confirmed;
    }

    public void Recover(LaunchSelection selection)
    {
        using var ownership = Own();
        var state = ReadState();
        if (state.Pending && state.Active == selection.Version && state.Attempt == selection.Attempt)
            RecoverOwned(state);
    }

    private ActivationState RecoverOwned(ActivationState state)
    {
        var previous = state.Previous;
        try { Verify(previous); }
        catch (Exception error) when (error is IOException or InvalidDataException or UnauthorizedAccessException or JsonException) { previous = Bootstrap; Verify(previous); }
        var recovered = new ActivationState(previous, Bootstrap, false);
        Commit(recovered, "recover");
        return recovered;
    }

    private ActivationState ReadState()
    {
        var damaged = false;
        foreach (var file in StateFiles().OrderByDescending(file => file.Sequence))
        {
            try
            {
                RejectOwnedPath(file.Path);
                var envelope = ReadJson<StateEnvelope>(file.Path);
                if (envelope.State is null) throw new InvalidDataException("Activation state is missing.");
                if (envelope.Sha256 != HashBytes(JsonSerializer.SerializeToUtf8Bytes(envelope.State)))
                    throw new InvalidDataException("Activation record checksum mismatch.");
                ValidateReference(envelope.State.Active);
                ValidateReference(envelope.State.Previous);
                if (envelope.State.Attempt is not null && (!IsId(envelope.State.Attempt) || !envelope.State.Pending || envelope.State.OwnerPid <= 0))
                    throw new InvalidDataException("Invalid activation attempt.");
                if (!damaged || !envelope.State.Pending) return envelope.State;
            }
            catch (Exception error) when (error is IOException or InvalidDataException or UnauthorizedAccessException or JsonException) { damaged = true; }
        }
        return new ActivationState(Bootstrap, Bootstrap, false);
    }

    private IEnumerable<(string Path, long Sequence)> StateFiles()
    {
        var directory = Path.Combine(Store, "state");
        RejectOwnedPath(directory);
        if (!Directory.Exists(directory)) yield break;
        foreach (var file in Directory.EnumerateFiles(directory, "*.json"))
            if (long.TryParse(Path.GetFileNameWithoutExtension(file), NumberStyles.None, CultureInfo.InvariantCulture, out var sequence) &&
                sequence > 0 && Path.GetFileName(file) == sequence.ToString("D20", CultureInfo.InvariantCulture) + ".json")
                yield return (file, sequence);
    }

    private void Commit(ActivationState state, string phase)
    {
        var directory = Path.Combine(Store, "state");
        RejectOwnedPath(directory);
        CreateDirectory(directory);
        var sequence = checked(StateFiles().Select(file => file.Sequence).DefaultIfEmpty(0).Max() + 1);
        var path = Path.Combine(directory, sequence.ToString("D20", CultureInfo.InvariantCulture) + ".json");
        var envelope = new StateEnvelope(state, HashBytes(JsonSerializer.SerializeToUtf8Bytes(state)));
        DurableInstallFile.WriteNew(path, JsonSerializer.SerializeToUtf8Bytes(envelope), () => _checkpoint?.Invoke(phase + "-state-flushed"));
        _checkpoint?.Invoke(phase + "-state-published");
    }

    public void CleanupStaging()
    {
        using var ownership = Own();
        _checkpoint?.Invoke("cleanup-start");
        var parent = Path.Combine(Store, "staging");
        RejectOwnedPath(parent);
        if (Directory.Exists(parent))
            foreach (var directory in Directory.EnumerateDirectories(parent))
            {
                var id = Path.GetFileName(directory);
                RejectOwnedPath(directory);
                var marker = Path.Combine(directory, ".staging-owner");
                RejectOwnedPath(marker);
                if (!IsId(id) || !File.Exists(marker) || File.ReadAllText(marker) != id) continue;
                // Refuse links anywhere before recursively deleting this owned incomplete staging directory.
                InspectTree(directory, directory);
                Directory.Delete(directory, recursive: true);
                DurableInstallFile.FlushDirectory(parent);
            }
        _checkpoint?.Invoke("cleanup-finished");
        // Protocol v1 deliberately retains committed versions and state history, including failed candidates.
    }

    public string Verify(VersionReference version)
    {
        var directory = VersionDirectory(version);
        VerifyDirectory(directory, version);
        return PayloadExecutable(directory);
    }

    private void VerifyDirectory(string directory, VersionReference version)
    {
        RejectOwnedPath(directory);
        var manifestPath = Path.Combine(directory, "manifest.json");
        RejectOwnedPath(manifestPath);
        if (Hash(manifestPath) != version.ManifestSha256) throw new InvalidDataException("Payload manifest is missing or changed.");
        var manifest = ReadJson<PayloadManifest>(manifestPath);
        if (manifest.Protocol != Protocol || manifest.Id != version.Id || manifest.Rid != Rid || manifest.Files is null || manifest.Files.Count is 0 or > 10000)
            throw new InvalidDataException("Invalid payload manifest.");
        var payload = Path.Combine(directory, "payload");
        var actual = InspectTree(payload, payload).Select(file => Path.GetRelativePath(payload, file).Replace('\\', '/')).ToHashSet(StringComparer.Ordinal);
        if (!actual.SetEquals(manifest.Files.Keys)) throw new InvalidDataException("Payload file list does not match the manifest.");
        foreach (var (relative, expected) in manifest.Files)
        {
            ValidateRelative(relative);
            if (expected is null || !IsHash(expected.Sha256)) throw new InvalidDataException("Invalid payload digest.");
            var path = Path.Combine(payload, relative.Replace('/', Path.DirectorySeparatorChar));
            if (Hash(path) != expected.Sha256) throw new InvalidDataException("Payload file is missing or changed.");
            if (!OperatingSystem.IsWindows() && (int)File.GetUnixFileMode(path) != expected.UnixMode)
                throw new InvalidDataException("Payload permissions are changed.");
        }
        var executable = PayloadExecutable(directory);
        if (!File.Exists(executable) || (!OperatingSystem.IsWindows() && (File.GetUnixFileMode(executable) & UnixFileMode.UserExecute) == 0))
            throw new InvalidDataException("The payload executable is not runnable.");
    }

    private void Extract(string archive, string destination)
    {
        using var zip = ZipFile.OpenRead(archive);
        var names = new HashSet<string>(OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
        foreach (var entry in zip.Entries)
        {
            var name = entry.FullName.TrimEnd('/');
            ValidateRelative(name);
            if ((name != "manifest.json" && name != "payload" && !name.StartsWith("payload/", StringComparison.Ordinal)) ||
                ((entry.ExternalAttributes >> 16) & 0xF000) == 0xA000 || !names.Add(name))
                throw new InvalidDataException("Unexpected bootstrap archive entry.");
            var path = Path.Combine(destination, name.Replace('/', Path.DirectorySeparatorChar));
            if (entry.FullName.EndsWith('/')) { CreateDirectory(path); continue; }
            CreateDirectory(Path.GetDirectoryName(path)!);
            using (var input = entry.Open())
            using (var output = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            { input.CopyTo(output); output.Flush(flushToDisk: true); }
        }
        // Permissions are part of the pinned manifest, not inferred from the ZIP's optional host attributes.
        var manifest = ReadJson<PayloadManifest>(Path.Combine(destination, "manifest.json"));
        if (!OperatingSystem.IsWindows()) foreach (var (name, metadata) in manifest.Files)
        {
            ValidateRelative(name);
            if ((metadata.UnixMode & ~0x1FF) != 0) throw new InvalidDataException("Unsupported payload mode.");
            File.SetUnixFileMode(Path.Combine(destination, "payload", name.Replace('/', Path.DirectorySeparatorChar)), (UnixFileMode)metadata.UnixMode);
        }
        FlushTree(destination);
    }

    private static void CopyTree(string source, string destination, string boundary)
    {
        RejectLinks(source, boundary);
        foreach (var directory in Directory.EnumerateDirectories(source))
        { RejectLinks(directory, boundary); CreateDirectory(Path.Combine(destination, Path.GetFileName(directory))); CopyTree(directory, Path.Combine(destination, Path.GetFileName(directory)), boundary); }
        foreach (var file in Directory.EnumerateFiles(source))
        {
            RejectLinks(file, boundary);
            var target = Path.Combine(destination, Path.GetFileName(file));
            using (var input = File.OpenRead(file))
            using (var output = new FileStream(target, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            { input.CopyTo(output); output.Flush(flushToDisk: true); }
            if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(target, File.GetUnixFileMode(file));
        }
        DurableInstallFile.FlushDirectory(destination);
    }

    private static void FlushTree(string directory)
    {
        foreach (var child in Directory.EnumerateDirectories(directory)) FlushTree(child);
        DurableInstallFile.FlushDirectory(directory);
    }

    private static IEnumerable<string> InspectTree(string directory, string boundary)
    {
        RejectLinks(directory, boundary);
        foreach (var file in Directory.EnumerateFiles(directory)) { RejectLinks(file, boundary); yield return file; }
        foreach (var child in Directory.EnumerateDirectories(directory))
        { RejectLinks(child, boundary); foreach (var file in InspectTree(child, boundary)) yield return file; }
    }

    private static BootstrapDescriptor ReadDescriptor(string path)
    {
        var descriptor = ReadJson<BootstrapDescriptor>(path);
        if (descriptor.Protocol != Protocol || !IsId(descriptor.Id) || !IsHash(descriptor.ManifestSha256) ||
            (descriptor.ArchiveSha256 is not null && !IsHash(descriptor.ArchiveSha256)))
            throw new InvalidDataException("This package requires another launcher protocol or has a damaged descriptor; install a new portable package separately.");
        return descriptor;
    }

    private static T ReadJson<T>(string path)
    {
        if (new FileInfo(path).Length > 1024 * 1024) throw new InvalidDataException("Installation metadata is too large.");
        return JsonSerializer.Deserialize<T>(File.ReadAllText(path)) ?? throw new InvalidDataException("Installation metadata is empty.");
    }

    private static void ValidateReference(VersionReference reference)
    {
        if (reference is null || !IsId(reference.Id) || !IsHash(reference.ManifestSha256)) throw new InvalidDataException("Invalid version reference.");
    }

    private static bool IsId(string? id) => id is not null && Guid.TryParseExact(id, "N", out _) && id == id.ToLowerInvariant();
    private static bool IsHash(string? hash) => hash is { Length: 64 } && hash.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
    private static string Hash(string path) { using var stream = File.OpenRead(path); return HashBytes(SHA256.HashData(stream), alreadyHashed: true); }
    private static string HashBytes(ReadOnlySpan<byte> bytes, bool alreadyHashed = false) => Convert.ToHexString(alreadyHashed ? bytes : SHA256.HashData(bytes)).ToLowerInvariant();

    private static void ValidateRelative(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || path.Contains('\\') || path.Contains(':') || Path.IsPathRooted(path) ||
            path.Split('/').Any(part => part is "" or "." or ".." || part.Any(c => c < 32 || "<>\"|?*".Contains(c)) ||
                part.EndsWith('.') || part.EndsWith(' ') || IsDeviceName(part)))
            throw new InvalidDataException("Unexpected payload path.");
    }

    private static bool IsDeviceName(string part)
    {
        var name = part.Split('.')[0].ToUpperInvariant();
        return name is "CON" or "PRN" or "AUX" or "NUL" ||
            (name.Length == 4 && (name.StartsWith("COM", StringComparison.Ordinal) || name.StartsWith("LPT", StringComparison.Ordinal)) && name[3] is >= '1' and <= '9');
    }

    public static void RejectLinks(string path, string boundary)
    {
        var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(boundary));
        var current = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        var relative = Path.GetRelativePath(root, current);
        if (Path.IsPathRooted(relative) || relative == ".." || relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new InvalidDataException("Installation path leaves its root.");
        while (true)
        {
            if ((File.Exists(current) || Directory.Exists(current)) && (File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0)
                throw new InvalidDataException("Installation paths cannot be filesystem links.");
            if (string.Equals(current, root, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)) return;
            current = Path.GetDirectoryName(current) ?? throw new InvalidDataException("Installation boundary is missing.");
        }
    }

    private static bool OwnerAlive(ActivationState state)
    {
        try
        {
            using var process = Process.GetProcessById(state.OwnerPid);
            var difference = Math.Abs(process.StartTime.ToUniversalTime().Ticks - state.OwnerStartTicks);
            return !process.HasExited && difference <= (OperatingSystem.IsWindows() ? 0 : TimeSpan.TicksPerSecond);
        }
        catch (Exception error) when (error is ArgumentException or InvalidOperationException) { return false; }
    }
}
