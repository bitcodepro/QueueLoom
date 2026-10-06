using System.Diagnostics;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.Core.Updates;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class StableLauncherTests
{
    [Theory]
    [InlineData("bootstrap-copied")]
    [InlineData("bootstrap-verified")]
    [InlineData("bootstrap-published")]
    public void InterruptedFreshInstallRestartsThroughTheSameEntry(string boundary)
    {
        if (OperatingSystem.IsMacOS()) { Assert.Skip("The Mac bootstrap is embedded and verified without archive extraction."); return; }
        using var fixture = new InstallationFixture();
        Assert.Throws<Interrupted>(() => new VersionInstallation(fixture.Launcher, At(boundary)).EnsureBootstrap());
        Assert.True(File.Exists(fixture.Launcher));
        var selection = new VersionInstallation(fixture.Launcher).SelectForLaunch();
        Assert.Equal(fixture.Initial.Id, selection.Version.Id);
        Assert.Equal(fixture.LauncherBytes, File.ReadAllBytes(fixture.Launcher));
    }

    [Theory]
    [InlineData("stage-copied")]
    [InlineData("stage-verified")]
    [InlineData("version-published")]
    [InlineData("activate-state-flushed")]
    [InlineData("activate-state-published")]
    [InlineData("activation-published")]
    public void InterruptedActivationNeverMovesTheStableEntry(string boundary)
    {
        using var fixture = new InstallationFixture();
        var update = fixture.Package("second");
        Assert.Throws<Interrupted>(() => new VersionInstallation(fixture.Launcher, At(boundary)).StagePackage(update.Path));
        Assert.Equal(fixture.LauncherBytes, File.ReadAllBytes(fixture.Launcher));
        var installation = new VersionInstallation(fixture.Launcher);
        var selection = installation.SelectForLaunch();
        Assert.Contains(selection.Version.Id, new[] { fixture.Initial.Id, update.Descriptor.Id });
        Assert.True(File.Exists(selection.Executable));
        Assert.True(File.Exists(installation.Verify(installation.Bootstrap)));
    }

    [Theory]
    [InlineData("attempt-state-flushed")]
    [InlineData("attempt-state-published")]
    [InlineData("attempt-published")]
    [InlineData("ack-published")]
    [InlineData("confirm-state-flushed")]
    [InlineData("confirm-state-published")]
    [InlineData("confirmation-published")]
    [InlineData("cleanup-start")]
    [InlineData("cleanup-finished")]
    public async Task KillingTheOnlyTransactionProcessAllowsRecoveryAfterAllProcessesStop(string boundary)
    {
        using var fixture = new InstallationFixture();
        var update = fixture.Package("second");
        new VersionInstallation(fixture.Launcher).StagePackage(update.Path);
        var operation = boundary.StartsWith("attempt-", StringComparison.Ordinal) ? "launch"
            : boundary.StartsWith("cleanup-", StringComparison.Ordinal) ? "cleanup" : "ack";
        using var child = fixture.StartFixture("--version-interrupt", fixture.Launcher, operation, boundary);
        var barrier = Path.Combine(fixture.Root, operation + ".barrier");
        await WaitFor(barrier, child);
        child.Kill(entireProcessTree: true);
        await child.WaitForExitAsync();
        // There are no remaining helper/installer processes: recovery is performed by a new launch.
        Assert.Equal(fixture.LauncherBytes, File.ReadAllBytes(fixture.Launcher));
        var installation = new VersionInstallation(fixture.Launcher);
        var selection = installation.SelectForLaunch();
        var acknowledged = boundary.Contains("ack", StringComparison.Ordinal) || boundary.StartsWith("confirm", StringComparison.Ordinal);
        if (operation == "launch" && boundary != "attempt-state-flushed") Assert.Equal(fixture.Initial.Id, selection.Version.Id);
        if (acknowledged) Assert.Equal(update.Descriptor.Id, selection.Version.Id);
        Assert.True(File.Exists(selection.Executable));
        Assert.True(File.Exists(installation.Verify(installation.Bootstrap)));
    }

    [Theory]
    [InlineData("missing")]
    [InlineData("partial")]
    [InlineData("digest")]
    [InlineData("path")]
    [InlineData("null")]
    public void MissingOrCorruptNewestStateFallsBackToVerifiedConfirmedHistory(string damage)
    {
        using var fixture = new InstallationFixture();
        var installation = new VersionInstallation(fixture.Launcher);
        var second = fixture.Package("second");
        installation.StagePackage(second.Path);
        var selected = installation.SelectForLaunch();
        installation.Acknowledge(selected.Version, selected.Attempt!);
        installation.Confirm(selected);
        var third = fixture.Package("third");
        installation.StagePackage(third.Path);
        var last = Directory.GetFiles(Path.Combine(installation.Store, "state"), "*.json").Order(StringComparer.Ordinal).Last();
        if (damage == "missing") File.Delete(last);
        else if (damage == "partial") File.WriteAllText(last, "{\"State\":");
        else if (damage == "digest") File.WriteAllText(last, File.ReadAllText(last).Replace(third.Descriptor.Id, Guid.NewGuid().ToString("N"), StringComparison.Ordinal));
        else if (damage == "null") File.WriteAllText(last, "{\"State\":null,\"Sha256\":\"invalid\"}");
        else
        {
            var node = System.Text.Json.Nodes.JsonNode.Parse(File.ReadAllText(last))!;
            var state = node["State"]!.Deserialize<ActivationState>()!;
            state = state with { Active = state.Active with { Id = "../../outside" } };
            // A matching checksum does not make an out-of-root path a valid version reference.
            File.WriteAllText(last, JsonSerializer.Serialize(new { State = state,
                Sha256 = Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(state))).ToLowerInvariant() }));
        }
        Assert.Equal(second.Descriptor.Id, new VersionInstallation(fixture.Launcher).SelectForLaunch().Version.Id);
        Assert.Equal(fixture.LauncherBytes, File.ReadAllBytes(fixture.Launcher));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MissingOrChangedCandidateCannotBeExecuted(bool changed)
    {
        using var fixture = new InstallationFixture();
        var installation = new VersionInstallation(fixture.Launcher);
        installation.StagePackage(fixture.Package("candidate").Path);
        var selection = installation.SelectForLaunch();
        if (changed) File.WriteAllText(selection.Executable, "tampered");
        else File.Delete(selection.Executable);
        Assert.Equal(fixture.Initial.Id, new VersionInstallation(fixture.Launcher).SelectForLaunch().Version.Id);
    }

    [Fact]
    public void PackagePathsAndProtocolAreValidatedBeforeActivation()
    {
        using var fixture = new InstallationFixture();
        var installation = new VersionInstallation(fixture.Launcher);
        var package = fixture.Package("bad");
        var descriptorFile = InstallationFixture.DescriptorFile(package.Path);
        File.WriteAllText(descriptorFile, JsonSerializer.Serialize(package.Descriptor with { Protocol = 2 }));
        Assert.Throws<InvalidDataException>(() => installation.StagePackage(package.Path));
        Assert.Equal(fixture.Initial.Id, installation.SelectForLaunch().Version.Id);
        Assert.Equal(fixture.LauncherBytes, File.ReadAllBytes(fixture.Launcher));
    }

    [Fact]
    public void CorruptBackupAndStateNeverCauseAnUnverifiedExecutableToRun()
    {
        using var fixture = new InstallationFixture();
        var installation = new VersionInstallation(fixture.Launcher);
        installation.EnsureBootstrap();
        File.WriteAllText(installation.Verify(installation.Bootstrap), "corrupt");
        Assert.Throws<InvalidDataException>(() => new VersionInstallation(fixture.Launcher).SelectForLaunch());
    }

    [Fact]
    public void ProductionUpdaterStagesTheVersionWithoutReplacingLauncherOrUserFiles()
    {
        using var fixture = new InstallationFixture();
        File.WriteAllText(Path.Combine(fixture.Root, "user-notes.txt"), "keep");
        Directory.CreateDirectory(Path.Combine(fixture.Root, "backups"));
        File.WriteAllText(Path.Combine(fixture.Root, "backups", "message.json"), "keep message");
        AppUpdater.Install(AppUpdater.TargetFor(VersionInstallation.CurrentRid(), fixture.Launcher), fixture.Package("next").Path);
        Assert.Equal(fixture.LauncherBytes, File.ReadAllBytes(fixture.Launcher));
        Assert.Equal("keep", File.ReadAllText(Path.Combine(fixture.Root, "user-notes.txt")));
        Assert.Equal("keep message", File.ReadAllText(Path.Combine(fixture.Root, "backups", "message.json")));
    }

    [Fact]
    public async Task RealLauncherForwardsArgumentsMcpStreamsAndExitCodeThroughFreshInstallAndUpgrade()
    {
        using var fixture = new InstallationFixture();
        var first = await fixture.RunLauncher("--payload-fixture", "--mcp", "argument with spaces", "--exit-7");
        Assert.Equal(7, first.Exit);
        Assert.Contains("argument with spaces", first.Output, StringComparison.Ordinal);
        Assert.Contains("client input", first.Output, StringComparison.Ordinal);
        Assert.Equal("fixture stderr", first.Error);
        var update = fixture.Package("second");
        new VersionInstallation(fixture.Launcher).StagePackage(update.Path);
        var second = await fixture.RunLauncher("--payload-fixture", "--mcp");
        Assert.Equal(0, second.Exit);
        Assert.Contains(update.Descriptor.Id, second.Output, StringComparison.Ordinal);
        Assert.Contains(fixture.Launcher.Replace("\\", "\\\\", StringComparison.Ordinal), second.Output, StringComparison.Ordinal);
        Assert.Equal(update.Descriptor.Id, new VersionInstallation(fixture.Launcher).SelectForLaunch().Version.Id);
    }

    [Fact]
    public async Task FailedMcpCandidateDoesNotConsumeTheClientsRequestBeforeFallback()
    {
        using var fixture = new InstallationFixture();
        var update = fixture.Package("bad-startup");
        new VersionInstallation(fixture.Launcher).StagePackage(update.Path);
        var result = await fixture.RunLauncher("--payload-fixture", "--mcp", "--fail-version=" + update.Descriptor.Id);
        Assert.Equal(0, result.Exit);
        Assert.Contains("client input", result.Output, StringComparison.Ordinal);
        Assert.Contains(fixture.Initial.Id, result.Output, StringComparison.Ordinal);
        Assert.DoesNotContain(update.Descriptor.Id + "\"}", result.Output, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("bootstrap", "bootstrap-copied")]
    [InlineData("bootstrap", "bootstrap-published")]
    [InlineData("stage", "stage-copied")]
    [InlineData("stage", "version-published")]
    [InlineData("stage", "activate-state-flushed")]
    [InlineData("stage", "activation-published")]
    public async Task PreparationAndActivationRecoverWithoutTheKilledInstaller(string operation, string boundary)
    {
        if (OperatingSystem.IsMacOS() && operation == "bootstrap") { Assert.Skip("Embedded Mac bootstrap requires no preparation process."); return; }
        using var fixture = new InstallationFixture();
        var command = operation == "bootstrap" ? operation : fixture.Package("next").Path;
        using var child = fixture.StartFixture("--version-interrupt", fixture.Launcher, command, boundary);
        try
        {
            await WaitFor(Path.IsPathRooted(command) ? command + ".barrier" : Path.Combine(fixture.Root, command + ".barrier"), child);
        }
        finally { if (!child.HasExited) { child.Kill(entireProcessTree: true); await child.WaitForExitAsync(); } }
        var installation = new VersionInstallation(fixture.Launcher);
        Assert.True(File.Exists(installation.SelectForLaunch().Executable));
        Assert.True(File.Exists(installation.Verify(installation.Bootstrap)));
        Assert.Equal(fixture.LauncherBytes, File.ReadAllBytes(fixture.Launcher));
    }

    [Fact]
    public void EmbeddedMacBootstrapAndLaterVersionsStayOutsideTheStableBundle()
    {
        if (!OperatingSystem.IsMacOS()) { Assert.Skip("Native macOS bundle layout."); return; }
        using var fixture = new InstallationFixture();
        var installation = new VersionInstallation(fixture.Launcher);
        var bootstrap = installation.SelectForLaunch();
        Assert.StartsWith(installation.Bundle! + Path.DirectorySeparatorChar, bootstrap.Executable, StringComparison.Ordinal);
        installation.StagePackage(fixture.Package("next").Path);
        Assert.StartsWith(installation.Store + Path.DirectorySeparatorChar, installation.SelectForLaunch().Executable, StringComparison.Ordinal);
        Assert.Equal(fixture.LauncherBytes, File.ReadAllBytes(fixture.Launcher));
    }

    [Fact]
    public async Task FreshReadOnlyInstallationUsesAnExternalPrivateStore()
    {
        if (OperatingSystem.IsWindows()) { Assert.Skip("Tests Unix permissions without altering Windows ACLs."); return; }
        using var fixture = new InstallationFixture();
        var original = File.GetUnixFileMode(fixture.Root);
        try
        {
            File.SetUnixFileMode(fixture.Root, UnixFileMode.UserRead | UnixFileMode.UserExecute);
            var installation = new VersionInstallation(fixture.Launcher);
            Assert.NotEqual(Path.Combine(fixture.Root, "QueueLoom.versions"), installation.Store);
            Assert.False(installation.Store.StartsWith(fixture.Root + Path.DirectorySeparatorChar, StringComparison.Ordinal));
            var result = await fixture.RunLauncher("--payload-fixture", "--mcp");
            Assert.Equal(0, result.Exit);
            Assert.Contains("client input", result.Output, StringComparison.Ordinal);
        }
        finally { File.SetUnixFileMode(fixture.Root, original); }
    }

    [Fact]
    public void StaleAcknowledgementCannotConfirmAnotherCandidateAndCleanupKeepsCommittedVersions()
    {
        using var fixture = new InstallationFixture();
        var installation = new VersionInstallation(fixture.Launcher);
        installation.StagePackage(fixture.Package("first").Path);
        var first = installation.SelectForLaunch();
        installation.Recover(first);
        installation.StagePackage(fixture.Package("second").Path);
        var second = installation.SelectForLaunch();
        Assert.Throws<InvalidDataException>(() => installation.Acknowledge(first.Version, first.Attempt!));
        Assert.False(installation.HasAcknowledgement(second));
        installation.CleanupStaging();
        Assert.True(File.Exists(installation.Verify(first.Version)));
        Assert.True(File.Exists(installation.Verify(installation.Bootstrap)));
        Assert.Throws<IOException>(() => installation.StagePackage(fixture.Package("third").Path));
    }

    [Theory]
    [InlineData("payload/../../escaped")]
    [InlineData("payload/CON")]
    [InlineData("payload/a\\b")]
    [InlineData("unexpected.txt")]
    public void RehashedArchiveStillRejectsPathsOutsideItsPayload(string name)
    {
        if (OperatingSystem.IsMacOS()) { Assert.Skip("The Mac payload uses bounded bundle copying, not a bootstrap ZIP."); return; }
        using var fixture = new InstallationFixture();
        var package = fixture.Package("bad-path");
        var archive = Path.Combine(package.Path, VersionInstallation.ArchiveName);
        using (var zip = ZipFile.Open(archive, ZipArchiveMode.Update))
        using (var writer = new StreamWriter(zip.CreateEntry(name).Open())) writer.Write("must never escape");
        File.WriteAllText(InstallationFixture.DescriptorFile(package.Path), JsonSerializer.Serialize(package.Descriptor with
        { ArchiveSha256 = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(archive))).ToLowerInvariant() }));
        var installation = new VersionInstallation(fixture.Launcher);
        Assert.Throws<InvalidDataException>(() => installation.StagePackage(package.Path));
        Assert.Equal(fixture.Initial.Id, installation.SelectForLaunch().Version.Id);
        Assert.Equal(fixture.LauncherBytes, File.ReadAllBytes(fixture.Launcher));
    }

    [Fact]
    public void CleanupRetainsUnownedDirectoriesAndRejectsLinkedState()
    {
        using var fixture = new InstallationFixture();
        var installation = new VersionInstallation(fixture.Launcher);
        installation.EnsureBootstrap();
        var unowned = Path.Combine(installation.Store, "staging", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(unowned);
        File.WriteAllText(Path.Combine(unowned, "user-file"), "keep");
        installation.CleanupStaging();
        Assert.Equal("keep", File.ReadAllText(Path.Combine(unowned, "user-file")));
        if (!OperatingSystem.IsWindows())
        {
            Directory.CreateSymbolicLink(Path.Combine(installation.Store, "state"), unowned);
            Assert.Throws<InvalidDataException>(() => installation.SelectForLaunch());
            Assert.Equal("keep", File.ReadAllText(Path.Combine(unowned, "user-file")));
        }
    }

    [Fact]
    public void SameAcknowledgementIsIdempotentAfterItsVersionWasConfirmed()
    {
        using var fixture = new InstallationFixture();
        var installation = new VersionInstallation(fixture.Launcher);
        installation.StagePackage(fixture.Package("next").Path);
        var first = installation.SelectForLaunch();
        var second = installation.SelectForLaunch();
        Assert.False(second.OwnsAttempt);
        installation.Acknowledge(first.Version, first.Attempt!);
        installation.Confirm(first);
        installation.Acknowledge(second.Version, second.Attempt!);
        Assert.Throws<InvalidDataException>(() => installation.Acknowledge(first.Version, Guid.NewGuid().ToString("N")));
        Assert.False(installation.HasPendingActivation());
    }

    [Fact]
    public async Task ConcurrentLaunchersDoNotShareAnUnconfirmedPayloadAttempt()
    {
        using var fixture = new InstallationFixture();
        new VersionInstallation(fixture.Launcher).StagePackage(fixture.Package("next").Path);
        var barrier = Path.Combine(fixture.Root, "startup barrier");
        var first = fixture.RunLauncher("--payload-fixture", "--mcp", "--ack-barrier=" + barrier);
        Task<(int Exit, string Output, string Error)>? second = null;
        try
        {
            for (var i = 0; i < 1000 && !File.Exists(barrier + ".pids"); i++) await Task.Delay(10);
            Assert.True(File.Exists(barrier + ".pids"));
            second = fixture.RunLauncher("--payload-fixture", "--mcp", "--ack-barrier=" + barrier);
            await Task.Delay(500);
            Assert.Single(File.ReadAllLines(barrier + ".pids"));
        }
        finally
        {
            File.WriteAllText(barrier + ".release", "ready");
            await first;
            if (second is not null) await second;
        }
        Assert.Equal(0, (await first).Exit);
        if (second is not null) Assert.Equal(0, (await second).Exit);
        Assert.False(new VersionInstallation(fixture.Launcher).HasPendingActivation());
    }

    private sealed class Interrupted : Exception;
    private static Action<string> At(string boundary) => point => { if (point == boundary) throw new Interrupted(); };
    private static async Task WaitFor(string path, Process process)
    {
        for (var i = 0; i < 1000 && !File.Exists(path) && !process.HasExited; i++) await Task.Delay(10);
        Assert.True(File.Exists(path), "The isolated process did not reach its interruption barrier.");
    }

    internal sealed class InstallationFixture : IDisposable
    {
        private readonly TemporaryDirectory _temporary = new();
        public string Root { get; }
        public string Launcher { get; }
        public BootstrapDescriptor Initial { get; }
        public byte[] LauncherBytes { get; }
        private static string ExecutableName => OperatingSystem.IsWindows() ? "QueueLoom.exe" : "QueueLoom";
        public InstallationFixture()
        {
            Root = Path.Combine(_temporary.Path, "portable installation with spaces");
            Directory.CreateDirectory(Root);
            var launcherDirectory = OperatingSystem.IsMacOS() ? Path.Combine(Root, "QueueLoom.app", "Contents", "MacOS") : Root;
            Directory.CreateDirectory(launcherDirectory);
            var source = Path.Combine(AppContext.BaseDirectory, "LauncherFixture");
            foreach (var file in Directory.GetFiles(source)) File.Copy(file, Path.Combine(launcherDirectory, Path.GetFileName(file)));
            Launcher = Path.Combine(launcherDirectory, ExecutableName);
            File.Copy(Path.Combine(source, OperatingSystem.IsWindows() ? "QueueLoom.Launcher.exe" : "QueueLoom.Launcher"), Launcher);
            if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(Launcher, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            LauncherBytes = File.ReadAllBytes(Launcher);
            var initial = Package("initial");
            Initial = initial.Descriptor;
            if (OperatingSystem.IsMacOS())
                CopyDirectory(Path.Combine(initial.Path, "QueueLoom.app", "Contents", "Resources"), Path.Combine(Root, "QueueLoom.app", "Contents", "Resources"));
            else
            {
                File.Copy(DescriptorFile(initial.Path), Path.Combine(Root, VersionInstallation.DescriptorName));
                File.Copy(Path.Combine(initial.Path, VersionInstallation.ArchiveName), Path.Combine(Root, VersionInstallation.ArchiveName));
            }
        }

        public (string Path, BootstrapDescriptor Descriptor) Package(string name)
        {
            var package = Path.Combine(_temporary.Path, name + "-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(package);
            var content = OperatingSystem.IsMacOS() ? Path.Combine(package, "QueueLoom.app", "Contents", "Resources", "initial")
                : Path.Combine(_temporary.Path, "content-" + Guid.NewGuid().ToString("N"));
            var payload = Path.Combine(content, "payload");
            var executableDirectory = OperatingSystem.IsMacOS() ? Path.Combine(payload, "QueueLoom.app", "Contents", "MacOS") : payload;
            Directory.CreateDirectory(executableDirectory);
            var source = Path.Combine(AppContext.BaseDirectory, "UpdateFixture");
            foreach (var file in Directory.GetFiles(source)) File.Copy(file, Path.Combine(executableDirectory, Path.GetFileName(file)));
            File.Copy(Path.Combine(source, OperatingSystem.IsWindows() ? "QueueLoom.UpdateFixture.exe" : "QueueLoom.UpdateFixture"), Path.Combine(executableDirectory, ExecutableName));
            if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(Path.Combine(executableDirectory, ExecutableName), UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            var metadata = Directory.GetFiles(payload, "*", SearchOption.AllDirectories).ToDictionary(file => Path.GetRelativePath(payload, file).Replace('\\', '/'),
                file => new PayloadFile(Digest(file), OperatingSystem.IsWindows() ? 420 : (int)File.GetUnixFileMode(file)), StringComparer.Ordinal);
            var manifest = new PayloadManifest(1, Guid.NewGuid().ToString("N"), VersionInstallation.CurrentRid(), name, metadata!);
            var manifestFile = Path.Combine(content, "manifest.json");
            File.WriteAllText(manifestFile, JsonSerializer.Serialize(manifest));
            var archive = Path.Combine(package, VersionInstallation.ArchiveName);
            if (!OperatingSystem.IsMacOS()) ZipFile.CreateFromDirectory(content, archive);
            var descriptor = new BootstrapDescriptor(1, manifest.Id, manifest.Rid, Digest(manifestFile), OperatingSystem.IsMacOS() ? null : Digest(archive));
            File.WriteAllText(DescriptorFile(package), JsonSerializer.Serialize(descriptor));
            return (package, descriptor);
        }

        public Process StartFixture(params string[] args)
        {
            var start = new ProcessStartInfo(Path.Combine(AppContext.BaseDirectory, "UpdateFixture",
                OperatingSystem.IsWindows() ? "QueueLoom.UpdateFixture.exe" : "QueueLoom.UpdateFixture"))
            { WorkingDirectory = Root, UseShellExecute = false, CreateNoWindow = true };
            foreach (var arg in args) start.ArgumentList.Add(arg);
            return Process.Start(start)!;
        }

        public async Task<(int Exit, string Output, string Error)> RunLauncher(params string[] args)
        {
            var start = new ProcessStartInfo(Launcher) { WorkingDirectory = Root, UseShellExecute = false, CreateNoWindow = true,
                RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true };
            start.Environment["QUEUELOOM_DATA_DIRECTORY"] = Path.Combine(_temporary.Path, "isolated data");
            foreach (var arg in args) start.ArgumentList.Add(arg);
            using var process = Process.Start(start)!;
            var output = process.StandardOutput.ReadToEndAsync();
            var error = process.StandardError.ReadToEndAsync();
            await process.StandardInput.WriteLineAsync("client input");
            process.StandardInput.Close();
            try { await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20)); }
            finally { if (!process.HasExited) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(); } }
            return (process.ExitCode, await output, await error);
        }
        private static string Digest(string file) { using var stream = File.OpenRead(file); return Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant(); }
        internal static string DescriptorFile(string package) => OperatingSystem.IsMacOS()
            ? Path.Combine(package, "QueueLoom.app", "Contents", "Resources", VersionInstallation.DescriptorName)
            : Path.Combine(package, VersionInstallation.DescriptorName);
        private static void CopyDirectory(string source, string destination)
        {
            Directory.CreateDirectory(destination);
            foreach (var file in Directory.GetFiles(source))
            {
                var target = Path.Combine(destination, Path.GetFileName(file));
                File.Copy(file, target);
                if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(target, File.GetUnixFileMode(file));
            }
            foreach (var child in Directory.GetDirectories(source)) CopyDirectory(child, Path.Combine(destination, Path.GetFileName(child)));
        }
        public void Dispose() => _temporary.Dispose();
    }
}
