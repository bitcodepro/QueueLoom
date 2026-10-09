using QueueLoom.App.Services;
using QueueLoom.Core.Updates;

if (args.Length >= 3 && args[0] == "--spawn-outside")
{
    // An installation process with a child running from elsewhere: test cleanup must stop the first, not the second.
    var outside = new System.Diagnostics.ProcessStartInfo(args[1]) { UseShellExecute = false };
    foreach (var argument in args.Skip(2)) outside.ArgumentList.Add(argument);
    using var spawned = System.Diagnostics.Process.Start(outside)!;
    PublishMarker(Path.Combine(AppContext.BaseDirectory, "spawned.pid"), spawned.Id.ToString(System.Globalization.CultureInfo.InvariantCulture));
    await Task.Delay(TimeSpan.FromMinutes(2));
    return 0;
}
if (args.Length == 3 && args[0] == "--stage-version")
{
    new VersionInstallation(args[1]).StagePackage(args[2]);
    return 0;
}
if (args.Length is 4 or 5 && args[0] == "--legacy-handoff")
{
    using var storageLock = new FileStream(args[2], FileMode.Create, FileAccess.ReadWrite, FileShare.None);
    var helperPid = UpdateRestart.Start(new UpdateTarget(VersionInstallation.CurrentRid(), Path.GetDirectoryName(args[1])!, args[1], null));
    using var helper = System.Diagnostics.Process.GetProcessById(helperPid);
    PublishMarker(args[2] + ".helper.json", System.Text.Json.JsonSerializer.Serialize(new
    {
        Pid = helper.Id, StartTicks = helper.StartTime.ToUniversalTime().Ticks
    }));
    if (args.Length == 5)
    {
        // Keep the sender (and therefore its waiting helper) alive until the smoke owns the helper handle.
        var claiming = System.Diagnostics.Stopwatch.StartNew();
        while (!File.Exists(args[4]))
        {
            if (claiming.Elapsed > TimeSpan.FromSeconds(15)) return 71;
            await Task.Delay(10);
        }
    }
    // The legacy sender stays alive after readiness, as the application does until its orderly close.
    await Task.Delay(int.Parse(args[3], System.Globalization.CultureInfo.InvariantCulture));
    return 0;
}

if (args.Length >= 4 && args[0] == "--version-interrupt")
{
    var installation = new VersionInstallation(args[1], point =>
    {
        if (point != args[3]) return;
        File.WriteAllText(args[2] + ".barrier", point);
        while (true) Thread.Sleep(20); // Only the isolated test process is terminated at this barrier.
    });
    switch (args[2])
    {
        case "bootstrap": installation.EnsureBootstrap(); break;
        case "launch": installation.SelectForLaunch(); break;
        case "cleanup": installation.CleanupStaging(); break;
        case "ack":
            var selection = installation.SelectForLaunch();
            installation.Acknowledge(selection.Version, selection.Attempt!);
            installation.Confirm(selection);
            break;
        default: installation.StagePackage(args[2]); break;
    }
    return 0;
}

if (args.Length > 0 && args[0] == "--payload-fixture")
{
    var context = PayloadLaunch.Current;
    if (args.Contains("--fail-before-ack") || args.Contains("--fail-version=" + Environment.GetEnvironmentVariable(VersionInstallation.ContextVersion))) return 31;
    if (args.FirstOrDefault(arg => arg.StartsWith("--ack-barrier=", StringComparison.Ordinal)) is { } barrierArgument)
    {
        var barrier = barrierArgument["--ack-barrier=".Length..];
        File.AppendAllText(barrier + ".pids", Environment.ProcessId + "\n");
        while (!File.Exists(barrier + ".release")) await Task.Delay(10);
    }
    context?.Acknowledge();
    var line = await Console.In.ReadLineAsync();
    Console.WriteLine(System.Text.Json.JsonSerializer.Serialize(new
    {
        args, input = line, launcher = context?.Installation.Launcher,
        root = context?.Installation.Root, version = Environment.GetEnvironmentVariable(VersionInstallation.ContextVersion)
    }));
    Console.Error.Write("fixture stderr");
    return args.Contains("--exit-7") ? 7 : 0;
}

// Harmless secret-tool substitute. Its files and process are owned by one isolated test directory.
if (args.Length > 0 && args[0] == "store")
{
    var secretRoot = AppContext.BaseDirectory;
    var value = await Console.In.ReadToEndAsync();
    var first = Path.Combine(secretRoot, "first-store.pid");
    if (!File.Exists(first))
    {
        // Published whole (temporary file, then rename), so the test never reads a half-written PID. A test can ask
        // for an unreadable one ("bad-marker") to check its own cleanup when it cannot learn the PID.
        var pid = File.Exists(Path.Combine(secretRoot, "bad-marker"))
            ? "not-a-pid"
            : Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture);
        File.WriteAllText(first + ".tmp", pid);
        File.Move(first + ".tmp", first);
        var wait = System.Diagnostics.Stopwatch.StartNew();
        while (!File.Exists(Path.Combine(secretRoot, "release-first-store")))
        {
            if (wait.Elapsed > TimeSpan.FromSeconds(20)) return 71;
            await Task.Delay(10);
        }
        File.WriteAllText(Path.Combine(secretRoot, "stored"), value);
        File.WriteAllText(Path.Combine(secretRoot, "first-store.done"), "done");
    }
    else File.WriteAllText(Path.Combine(secretRoot, "stored"), value);
    return 0;
}

if (args.Length == 6 && args[0] == "--append-history")
{
    var store = new QueueLoom.Infrastructure.Persistence.JsonLinesDeadLetterHistoryStore(args[1]);
    var profile = Guid.Parse(args[2]);
    await store.ReadAsync(profile, DateTimeOffset.MinValue);
    File.WriteAllText(args[3] + ".ready", "ready");
    var wait = System.Diagnostics.Stopwatch.StartNew();
    while (!File.Exists(args[4]))
    {
        if (wait.Elapsed > TimeSpan.FromSeconds(20)) return 71;
        await Task.Delay(10);
    }
    File.WriteAllText(args[3] + ".started", "started");
    await store.AppendAsync(new QueueLoom.Core.Monitoring.DeadLetterHistorySample(DateTimeOffset.UtcNow, profile,
        "Test", long.Parse(args[5], System.Globalization.CultureInfo.InvariantCulture), new Dictionary<string, long>()));
    File.WriteAllText(args[3] + ".done", "done");
    return 0;
}

if (args.Length == 5 && args[0] == "--export-diagnostics")
{
    var preview = new DiagnosticsPreview($"Report from {args[3]}\n", $"{{\"export\":\"{args[3]}\"}}");
    File.WriteAllText(args[2] + ".ready", "ready");
    var wait = System.Diagnostics.Stopwatch.StartNew();
    while (!File.Exists(args[4]))
    {
        if (wait.Elapsed > TimeSpan.FromSeconds(20)) return 71;
        await Task.Delay(5);
    }
    try
    {
        await preview.SaveAsync(args[1]);
        File.WriteAllText(args[2] + ".result", "saved");
    }
    catch (IOException)
    {
        File.WriteAllText(args[2] + ".result", "collision");
    }
    return 0;
}

if (args.Length == 5 && args[0] == "--update-settings")
{
    using var store = new QueueLoom.Infrastructure.Persistence.JsonAppSettingsStore(
        QueueLoom.Infrastructure.Persistence.QueueLoomPaths.ForRoot(args[1]));
    File.WriteAllText(args[3] + ".started", "started");
    await store.UpdateAsync(settings =>
    {
        File.WriteAllText(args[3] + ".read", settings.Theme.ToString());
        if (args[2] == "theme")
        {
            var wait = System.Diagnostics.Stopwatch.StartNew();
            while (!File.Exists(args[4]))
            {
                if (wait.Elapsed > TimeSpan.FromSeconds(15)) throw new TimeoutException("Settings transaction barrier timed out.");
                Thread.Sleep(10);
            }
            return settings with { Theme = QueueLoom.Core.Settings.AppThemePreference.Light };
        }
        return settings with { MonitorIntervalSeconds = 321 };
    });
    File.WriteAllText(args[3] + ".done", "done");
    return 0;
}

if (args.Length == 4 && args[0] == "--claim-schedule")
{
    var store = new QueueLoom.Infrastructure.Persistence.JsonScheduledResendStore(
        QueueLoom.Infrastructure.Persistence.QueueLoomPaths.ForRoot(args[1]));
    var job = store.Load().Single();
    File.WriteAllText(args[2] + ".ready", "ready");
    var wait = System.Diagnostics.Stopwatch.StartNew();
    while (!File.Exists(args[3]))
    {
        if (wait.Elapsed > TimeSpan.FromSeconds(15)) return 71;
        await Task.Delay(25);
    }
    File.WriteAllText(args[2] + ".result", store.TryRemove(job).ToString());
    return 0;
}
if (args.Length > 0 && args[0] == "--hold-lock")
{
    using var locked = new FileStream(args[1], FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
    File.WriteAllText(args[1] + ".held", "ready");
    // "until-released": held until the test writes <file>.release (bounded), instead of for a fixed time.
    if (args[2] == "until-released")
    {
        var held = System.Diagnostics.Stopwatch.StartNew();
        while (!File.Exists(args[1] + ".release") && held.Elapsed < TimeSpan.FromSeconds(60)) await Task.Delay(10);
    }
    else await Task.Delay(int.Parse(args[2], System.Globalization.CultureInfo.InvariantCulture));
    return 0;
}
if (UpdateRestart.HandleArguments(args, out var code))
{
    // Fixture-only gate after the real helper has removed the receipt, before its process exits.
    // Production binaries do not read this environment variable or contain this gate.
    if (args[0] == "--update-helper" && code == 0 &&
        Environment.GetEnvironmentVariable("QUEUELOOM_SMOKE_FIXTURE_GATE") is { } gate)
    {
        PublishMarker(gate + ".helper.json", System.Text.Json.JsonSerializer.Serialize(new
        {
            Pid = Environment.ProcessId, StartTicks = System.Diagnostics.Process.GetCurrentProcess().StartTime.ToUniversalTime().Ticks,
            Receipt = args[1], Executable = Environment.ProcessPath
        }));
        var wait = System.Diagnostics.Stopwatch.StartNew();
        while (!File.Exists(gate + ".release"))
        {
            if (wait.Elapsed > TimeSpan.FromSeconds(60)) return 71;
            await Task.Delay(10);
        }
        return int.Parse(File.ReadAllText(gate + ".release"), System.Globalization.CultureInfo.InvariantCulture);
    }
    return code;
}
var root = AppContext.BaseDirectory;
// An ordinary launch of this harmless fixture stands in for startup that needs the parent's storage lock.
try
{
    using var storage = new FileStream(Path.Combine(root, "storage.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
}
catch (IOException)
{
    File.WriteAllText(Path.Combine(root, "startup-blocked.txt"), "Previous process still owns storage");
    return 41;
}
if (args.Length > 0 && args[0] == "--update-startup")
{
    if (File.Exists(Path.Combine(root, "fail-startup"))) return 23;
    if (File.Exists(Path.Combine(root, "hang-startup"))) await Task.Delay(Timeout.Infinite);
    File.WriteAllText(Path.Combine(root, "updated-started.txt"), Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));
    UpdateRestart.AcknowledgeStartup();
    if (Environment.GetEnvironmentVariable("QUEUELOOM_SMOKE_FIXTURE_GATE") is { } gate)
    {
        PublishMarker(gate + ".gui.pid", Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Task.Delay(TimeSpan.FromSeconds(60)); // Stopped by the smoke script's bounded installation cleanup.
    }
    else await Task.Delay(1000);
}
else
{
    // A test can hold the restarted previous version before it finishes starting, to check its own cleanup.
    if (File.Exists(Path.Combine(root, "hold-recovery")))
    {
        PublishMarker(Path.Combine(root, "recovery-held.txt"), Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            gate: Path.Combine(root, "gate-marker-publication"));
        var held = System.Diagnostics.Stopwatch.StartNew();
        while (!File.Exists(Path.Combine(root, "release-recovery")) && held.Elapsed < TimeSpan.FromSeconds(60)) await Task.Delay(20);
    }
    File.WriteAllText(Path.Combine(root, "recovered-started.txt"), "Previous application restarted");
}
return 0;

// A marker a test reads as soon as it exists: written and closed under a temporary name, then renamed, so a reader
// never sees it partial or still open. With a gate, the temporary file waits for the test before it is renamed.
static void PublishMarker(string marker, string content, string? gate = null)
{
    var temporary = marker + ".tmp";
    File.WriteAllText(temporary, content);
    if (gate is not null && File.Exists(gate))
    {
        var waited = System.Diagnostics.Stopwatch.StartNew();
        while (!File.Exists(gate + ".open") && waited.Elapsed < TimeSpan.FromSeconds(60)) Thread.Sleep(10);
    }
    File.Move(temporary, marker);
}

namespace QueueLoom.App.Services
{
    public sealed record UpdateTarget(string Rid, string InstallDirectory, string Executable, string? Bundle);
}
