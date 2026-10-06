using QueueLoom.App.Services;
using QueueLoom.Core.Updates;

if (args.Length == 3 && args[0] == "--stage-version")
{
    new VersionInstallation(args[1]).StagePackage(args[2]);
    return 0;
}
if (args.Length == 4 && args[0] == "--legacy-handoff")
{
    using var storageLock = new FileStream(args[2], FileMode.Create, FileAccess.ReadWrite, FileShare.None);
    UpdateRestart.Start(new UpdateTarget(VersionInstallation.CurrentRid(), Path.GetDirectoryName(args[1])!, args[1], null));
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
        File.WriteAllText(first, Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));
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
    store.Read(profile, DateTimeOffset.MinValue);
    File.WriteAllText(args[3] + ".ready", "ready");
    var wait = System.Diagnostics.Stopwatch.StartNew();
    while (!File.Exists(args[4]))
    {
        if (wait.Elapsed > TimeSpan.FromSeconds(20)) return 71;
        await Task.Delay(10);
    }
    File.WriteAllText(args[3] + ".started", "started");
    store.Append(new QueueLoom.Core.Monitoring.DeadLetterHistorySample(DateTimeOffset.UtcNow, profile,
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
    await Task.Delay(int.Parse(args[2], System.Globalization.CultureInfo.InvariantCulture));
    return 0;
}
if (UpdateRestart.HandleArguments(args, out var code)) return code;
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
    await Task.Delay(1000);
}
else File.WriteAllText(Path.Combine(root, "recovered-started.txt"), "Previous application restarted");
return 0;

namespace QueueLoom.App.Services
{
    public sealed record UpdateTarget(string Rid, string InstallDirectory, string Executable, string? Bundle);
}
