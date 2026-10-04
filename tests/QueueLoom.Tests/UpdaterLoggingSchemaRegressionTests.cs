using System.Formats.Tar;
using System.IO.Compression;
using System.Net;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using QueueLoom.App.Services;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Logging;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class UpdaterLoggingSchemaRegressionTests
{
    private static readonly UpdateCheckResult Update =
        new(new Version(9, 1, 0), "v9.1.0", new Uri("https://github.com/bitcodepro/QueueLoom/releases/tag/v9.1.0"));

    // A verified package must not wait in a folder another local account can rename or replace before Install moves
    // it into the program folder (the default root used to be /tmp/QueueLoom-update, which anyone can pre-create).
    [Fact]
    public async Task Updater_DownloadRootOwnedByAnotherAccountIsRefusedBeforeDownloading()
    {
        // /tmp is the real shape of the attack: a world-writable folder this account does not own. As root every
        // folder is ours to change, so the check is meaningful (and harmless to /tmp) only for an ordinary account.
        if (OperatingSystem.IsWindows() || Environment.IsPrivilegedProcess || !Directory.Exists("/tmp")) return;
        using var directory = new TemporaryDirectory();
        var handler = new PackageHandler(TarGz(("QueueLoom", "new program")));
        var updater = new AppUpdater(new HttpClient(handler), "/tmp");
        var app = Path.Combine(directory.Path, "app");
        Directory.CreateDirectory(app);
        string? staged = null;
        try
        {
            var error = await Assert.ThrowsAsync<UpdateStageException>(async () => staged = await updater.DownloadAsync(Update,
                new UpdateTarget("linux-x64", app, Path.Combine(app, "QueueLoom"), null), null, CancellationToken.None));

            Assert.Contains("other users", error.Message, StringComparison.Ordinal);
            Assert.Equal(0, handler.Requests);
        }
        finally
        {
            if (staged is not null) Directory.Delete(Path.GetDirectoryName(staged)!, recursive: true);
        }
    }

    [Fact]
    public async Task Updater_SharedModeDownloadRootIsMadePrivateBeforeAnythingIsStaged()
    {
        if (OperatingSystem.IsWindows()) return;
        using var directory = new TemporaryDirectory();
        var root = Path.Combine(directory.Path, "download-root");
        Directory.CreateDirectory(root);
        File.SetUnixFileMode(root, (UnixFileMode)0b111_111_111); // drwxrwxrwx
        var updater = new AppUpdater(new HttpClient(new PackageHandler(TarGz(("QueueLoom", "new program")))), root);
        var app = Path.Combine(directory.Path, "app");
        Directory.CreateDirectory(app);

        await updater.DownloadAsync(Update, new UpdateTarget("linux-x64", app, Path.Combine(app, "QueueLoom"), null), null, CancellationToken.None);

        Assert.Equal((UnixFileMode)0, File.GetUnixFileMode(root) & (UnixFileMode.GroupWrite | UnixFileMode.OtherWrite));
    }

    [Fact]
    public async Task Updater_NewDownloadFoldersArePrivateToTheCurrentUser()
    {
        if (OperatingSystem.IsWindows()) return;
        using var directory = new TemporaryDirectory();
        var root = Path.Combine(directory.Path, "new-root");
        var package = TarGz(("QueueLoom", "new program"));
        var updater = new AppUpdater(new HttpClient(new PackageHandler(package)), root);
        var app = Path.Combine(directory.Path, "app");
        Directory.CreateDirectory(app);

        var staging = await updater.DownloadAsync(Update, new UpdateTarget("linux-x64", app, Path.Combine(app, "QueueLoom"), null),
            null, CancellationToken.None);

        const UnixFileMode others = UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute |
                                    UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;
        Assert.Equal((UnixFileMode)0, File.GetUnixFileMode(root) & others);
        Assert.Equal((UnixFileMode)0, File.GetUnixFileMode(Path.GetDirectoryName(staging)!) & others);
    }

    [Fact]
    public void Updater_DefaultLinuxDownloadRootIsNotTheSharedTemporaryFolder()
    {
        if (!OperatingSystem.IsLinux()) return;
        var updater = new AppUpdater(new HttpClient());
        var root = (string)typeof(AppUpdater).GetField("_downloadRoot", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(updater)!;

        Assert.False(Path.GetFullPath(root).StartsWith(Path.GetFullPath(Path.GetTempPath()), StringComparison.Ordinal),
            $"Update downloads are staged in the shared temporary folder: {root}");
    }

    // QueueLoom runs for weeks in the tray; retention that only runs at construction never prunes anything.
    [Fact]
    public void Logging_RetentionAlsoRunsWhenTheDayChangesInALongRunningProcess()
    {
        using var directory = new TemporaryDirectory();
        var now = new DateTimeOffset(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);
        using var provider = new FileLoggerProvider(directory.Path, retainedDays: 14, clock: () => now);
        var logger = provider.CreateLogger("tray");
        logger.LogInformation("first day");
        var firstDay = provider.CurrentFilePath;

        now = now.AddDays(30);
        logger.LogInformation("a month later");

        Assert.False(File.Exists(firstDay), "A 30-day-old log survived a 14-day retention window.");
        Assert.Contains("a month later", File.ReadAllText(provider.CurrentFilePath), StringComparison.Ordinal);
    }

    // The desktop app and every MCP server process write the same daily log file.
    [Fact]
    public async Task Logging_ConcurrentProcessesWritingTheSameDailyLogKeepEveryLine()
    {
        using var directory = new TemporaryDirectory();
        var now = new DateTimeOffset(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);
        // Separate providers have separate in-process locks, exactly like the app and an MCP server process.
        using var desktop = new FileLoggerProvider(directory.Path, clock: () => now);
        using var mcp = new FileLoggerProvider(directory.Path, clock: () => now);
        const int lines = 1500;
        using var start = new ManualResetEventSlim();
        Task Write(FileLoggerProvider provider, string name) => Task.Factory.StartNew(() =>
        {
            var logger = provider.CreateLogger(name);
            start.Wait();
            for (var index = 0; index < lines; index++) logger.LogInformation("line {Index:D5} {Padding}", index, new string('x', 200));
        }, TaskCreationOptions.LongRunning);
        var writers = new[] { Write(desktop, "desktop"), Write(mcp, "mcp") };
        start.Set();
        await Task.WhenAll(writers);

        var text = File.ReadAllLines(desktop.CurrentFilePath);
        Assert.Equal(lines, text.Count(line => line.Contains("] desktop: line ", StringComparison.Ordinal)));
        Assert.Equal(lines, text.Count(line => line.Contains("] mcp: line ", StringComparison.Ordinal)));
        Assert.All(text, line => Assert.Matches(@"^\d{4}-\d{2}-\d{2} .* \[INF\] (desktop|mcp): line \d{5} x{200}$", line));
    }

    // Schema loading is recursive; without a limit a crafted or generated schema overflows the stack, which kills the
    // desktop app or the MCP server (which reloads the persisted schema path at every start) instead of reporting an error.
    [Fact]
    public void Schemas_DeeplyNestedProtoMessagesAreAControlledSchemaError()
    {
        const int depth = 400;
        var text = new StringBuilder("syntax = \"proto3\";\n");
        for (var level = 0; level < depth; level++) text.Append("message M").Append(level).Append(" { ");
        text.Append("string leaf = 1; ");
        for (var level = 0; level < depth; level++) text.Append("} ");

        var error = Assert.Throws<ProtoSchemaException>(() => ProtoSchemaSet.FromProtoFiles([("deep.proto", text.ToString())]));
        Assert.Contains("nested", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Schemas_DeeplyNestedOneofGroupsAreAControlledSchemaError()
    {
        const int depth = 400;
        var text = new StringBuilder("syntax = \"proto3\";\nmessage Root { ");
        for (var level = 0; level < depth; level++) text.Append("oneof g").Append(level).Append(" { ");
        text.Append("string leaf = 1; ");
        for (var level = 0; level < depth; level++) text.Append("} ");
        text.Append('}');

        var error = Assert.Throws<ProtoSchemaException>(() => ProtoSchemaSet.FromProtoFiles([("deep.proto", text.ToString())]));
        Assert.Contains("nested", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Schemas_DeeplyNestedDescriptorSetIsAControlledSchemaError()
    {
        const int depth = 400;
        // DescriptorProto { name = 1; nested_type = 3 }, innermost first.
        var message = LengthDelimited(1, Encoding.UTF8.GetBytes("M"));
        for (var level = 0; level < depth; level++)
            message = [.. LengthDelimited(1, Encoding.UTF8.GetBytes("M")), .. LengthDelimited(3, message)];
        var file = LengthDelimited(4, message);
        var set = LengthDelimited(1, file);

        var error = Assert.Throws<ProtoSchemaException>(() => ProtoSchemaSet.FromDescriptorSet(set, "deep.desc"));
        Assert.Contains("nested", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    // Picking a .proto loads its whole folder tree; repositories often hold a database volume owned by another account.
    [Fact]
    public void Schemas_UnreadableSubfolderDoesNotDiscardTheSchemasBesideIt()
    {
        if (OperatingSystem.IsWindows() || Environment.IsPrivilegedProcess) return;
        using var directory = new TemporaryDirectory();
        File.WriteAllText(Path.Combine(directory.Path, "orders.proto"), "syntax = \"proto3\"; package shop; message Order { string id = 1; }");
        var volume = Path.Combine(directory.Path, "pgdata");
        Directory.CreateDirectory(volume);
        File.SetUnixFileMode(volume, UnixFileMode.None);
        try
        {
            var schemas = ProtoSchemaSet.Load(directory.Path);

            Assert.NotNull(schemas.FindMessage("shop.Order"));
        }
        finally
        {
            File.SetUnixFileMode(volume, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
    }

    [Fact]
    public void Schemas_RealisticNestingStillLoads()
    {
        var text = new StringBuilder("syntax = \"proto3\";\n");
        for (var level = 0; level < 20; level++) text.Append("message M").Append(level).Append(" { ");
        text.Append("string leaf = 1; ");
        for (var level = 0; level < 20; level++) text.Append("} ");

        var schemas = ProtoSchemaSet.FromProtoFiles([("nested.proto", text.ToString())]);
        Assert.Equal(20, schemas.Messages.Count);
    }

    private static byte[] LengthDelimited(int number, byte[] value) => [.. Varint((ulong)(number << 3 | 2)), .. Varint((ulong)value.Length), .. value];

    private static byte[] Varint(ulong value)
    {
        var bytes = new List<byte>();
        do
        {
            var current = (byte)(value & 0x7F);
            value >>= 7;
            bytes.Add(value == 0 ? current : (byte)(current | 0x80));
        } while (value != 0);
        return [.. bytes];
    }

    private static byte[] TarGz(params (string Path, string Content)[] files)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
        using (var tar = new TarWriter(gzip))
        {
            foreach (var (path, content) in files)
            {
                tar.WriteEntry(new PaxTarEntry(TarEntryType.RegularFile, "./" + path)
                {
                    DataStream = new MemoryStream(Encoding.UTF8.GetBytes(content))
                });
            }
        }
        return output.ToArray();
    }

    private sealed class PackageHandler(byte[] package) : HttpMessageHandler
    {
        public int Requests;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Requests);
            var path = request.RequestUri!.AbsolutePath;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = path.EndsWith(".sha256", StringComparison.Ordinal)
                    ? new StringContent($"{Convert.ToHexStringLower(SHA256.HashData(package))}  {Path.GetFileName(path)[..^7]}")
                    : new ByteArrayContent(package)
            });
        }
    }
}
