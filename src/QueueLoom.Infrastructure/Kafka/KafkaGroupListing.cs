using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using QueueLoom.Core.Processes;

namespace QueueLoom.Infrastructure.Kafka;

/// <summary>
/// Lists a cluster's consumer groups in a child process. librdkafka 2.15 has freed the ListConsumerGroups result twice
/// (seen once on CI, never reproduced), which aborts the whole process; in a child the app only loses the lag. The
/// child uses the same ListConsumerGroups request, so KIP-848 groups keep their type and state, which the classic
/// ListGroups request reports as Dead.
/// </summary>
public static partial class KafkaGroupListing
{
    /// <summary>The argument that runs the executable as the listing child instead of the app.</summary>
    public const string HelperArgument = "--kafka-list-consumer-groups";

    private static readonly TimeSpan ChildGrace = TimeSpan.FromSeconds(15);

    /// <summary>
    /// How to start the listing child, set by an executable that handles <see cref="HelperArgument"/>; without it
    /// groups are listed in this process.
    /// </summary>
    public static Func<ProcessStartInfo>? HelperStart { get; set; }

    /// <summary>Lets the running executable list groups in a child of itself (the app, or <c>dotnet app.dll</c>).</summary>
    public static void UseThisExecutable(string? entryAssemblyPath)
    {
        var processPath = Environment.ProcessPath;
        if (string.IsNullOrEmpty(processPath)) return;
        var hosted = string.Equals(Path.GetFileNameWithoutExtension(processPath), "dotnet", StringComparison.OrdinalIgnoreCase);
        if (hosted && string.IsNullOrEmpty(entryAssemblyPath)) return;
        HelperStart = () =>
        {
            var start = new ProcessStartInfo(processPath);
            if (hosted) start.ArgumentList.Add(entryAssemblyPath!);
            start.ArgumentList.Add(HelperArgument);
            return start;
        };
    }

    /// <summary>The child's work: reads the request from standard input and writes the groups to standard output.</summary>
    public static int RunHelper(Stream input, Stream output)
    {
        // The app's Main is an STA thread, where a blocking wait (in Confluent's calls and its Dispose) goes through
        // COM, and COM fails (0x8007007A) when the executable's path is beyond MAX_PATH, which the launcher supports.
        // The work runs on its own (MTA) thread; this one only sleeps, which does not involve COM.
        var work = new Thread(() => List(input, output)) { IsBackground = true, Name = "Kafka group listing" };
        work.Start();
        while (work.IsAlive)
        {
            Thread.Sleep(10);
        }
        return 0;
    }

    private static void List(Stream input, Stream output)
    {
        var request = JsonSerializer.Deserialize(input, ListingJson.Default.ListingRequest)
                      ?? throw new InvalidDataException("No listing request.");
        IAdminClient admin;
        try
        {
            admin = new AdminClientBuilder(new AdminClientConfig(request.Config)).Build();
        }
        catch (Exception exception) when (exception is ArgumentException or KafkaException or InvalidOperationException)
        {
            // A client that cannot be created (an invalid setting) is an answer, not a crash; nothing was sent.
            Answer(output, new ListingResponse(null, exception.Message));
            return;
        }
        using var owned = admin;
        ListingResponse response;
        try
        {
            var groups = admin.ListConsumerGroupsAsync(new ListConsumerGroupsOptions { RequestTimeout = TimeSpan.FromMilliseconds(request.TimeoutMs) })
                .GetAwaiter().GetResult().Valid;
            response = new ListingResponse(groups.Select(group => new ListedGroup(group.GroupId, group.State.ToString(), group.Type.ToString(), group.IsSimpleConsumerGroup)).ToList(), null);
        }
        catch (KafkaException exception)
        {
            response = new ListingResponse(null, KafkaWorkspace.ReasonOf(exception));
        }
        // Answered before the client is disposed: a failed listing corrupts the heap when librdkafka cleans up.
        Answer(output, response);
    }

    private static void Answer(Stream output, ListingResponse response)
    {
        JsonSerializer.Serialize(output, response, ListingJson.Default.ListingResponse);
        output.Flush();
    }

    /// <summary>Lists the groups in a child when one is set up, otherwise with <paramref name="admin"/>.</summary>
    internal static async Task<IReadOnlyList<ListedGroup>> ListAsync(IAdminClient? admin, ClientConfig? config, TimeSpan timeout,
        CancellationToken cancellationToken, Func<ProcessStartInfo>? helperStart = null)
    {
        if ((helperStart ?? HelperStart) is not { } helper)
        {
            ArgumentNullException.ThrowIfNull(admin);
            var groups = (await admin.ListConsumerGroupsAsync(new ListConsumerGroupsOptions { RequestTimeout = timeout }).ConfigureAwait(false)).Valid;
            return groups.Select(group => new ListedGroup(group.GroupId, group.State.ToString(), group.Type.ToString(), group.IsSimpleConsumerGroup)).ToList();
        }

        ArgumentNullException.ThrowIfNull(config);
        var start = helper();
        start.RedirectStandardInput = true;
        start.RedirectStandardOutput = true;
        start.RedirectStandardError = true;
        start.UseShellExecute = false;
        start.CreateNoWindow = true;
        if (string.IsNullOrEmpty(start.WorkingDirectory))
        {
            // The app's own folder can be beyond MAX_PATH, which CreateProcess refuses as a current directory.
            start.WorkingDirectory = Path.GetTempPath();
        }
        PayloadProcess child;
        try
        {
            // PayloadProcess starts executables at Windows paths beyond MAX_PATH, as the launcher does for the app.
            child = PayloadProcess.Start(start);
        }
        catch (Exception exception) when (exception is Win32Exception or IOException or InvalidOperationException or UnauthorizedAccessException)
        {
            // Lag is extra: a child that cannot start costs only the lag, never the topics.
            throw new GroupListingException($"the listing process could not start ({exception.Message})");
        }
        using var owned = child;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(timeout + ChildGrace);
        try
        {
            var error = new StreamReader(child.Error).ReadToEndAsync(deadline.Token);
            var output = new StreamReader(child.Output).ReadToEndAsync(deadline.Token);
            // The config carries the SASL password: it goes through the pipe, never the command line.
            try
            {
                await JsonSerializer.SerializeAsync(child.Input,
                    new ListingRequest(config.ToDictionary(pair => pair.Key, pair => pair.Value), (int)timeout.TotalMilliseconds),
                    ListingJson.Default.ListingRequest, deadline.Token).ConfigureAwait(false);
                child.Input.Close();
            }
            catch (IOException)
            {
                // The child ended before reading the request (a broken pipe); its exit code says why, below.
            }
            await child.WaitForExitAsync().WaitAsync(deadline.Token).ConfigureAwait(false);
            var text = await output.ConfigureAwait(false);
            var errorText = await error.ConfigureAwait(false);
            // A failed listing can abort the child after it answered (the double free happens when librdkafka cleans
            // up), so a complete answer counts whatever the exit code.
            if (string.IsNullOrWhiteSpace(text))
            {
                throw new GroupListingException(Stopped(child.ExitCode, errorText));
            }
            ListingResponse? response;
            try
            {
                response = JsonSerializer.Deserialize(text, ListingJson.Default.ListingResponse);
            }
            catch (JsonException) when (child.ExitCode != 0)
            {
                throw new GroupListingException(Stopped(child.ExitCode, errorText));
            }
            if (response is null) throw new GroupListingException("the listing process gave no answer");
            return response.Groups ?? throw new GroupListingException(response.Error ?? "unknown error");
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new GroupListingException("the listing process did not answer in time");
        }
        catch (JsonException)
        {
            throw new GroupListingException("the listing process gave an answer that could not be read");
        }
        finally
        {
            if (!child.HasExited)
            {
                try { child.Kill(); } catch (Exception exception) when (exception is InvalidOperationException or Win32Exception) { }
            }
        }
    }

    /// <summary>Why the child stopped: its exit code and its last own error line (librdkafka's "%3|..." logs are noise).</summary>
    private static string Stopped(int exitCode, string errorText)
    {
        var lines = errorText.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(line => !line.StartsWith('%') && !line.StartsWith("at ", StringComparison.Ordinal)).ToArray();
        // An unhandled exception names itself on its first line; anything else, the last line says most.
        var last = lines.FirstOrDefault(line => line.StartsWith("Unhandled exception.", StringComparison.Ordinal)) ?? lines.LastOrDefault();
        return last is null
            ? $"the listing process stopped with exit code {exitCode}"
            : $"the listing process stopped with exit code {exitCode}: {(last.Length > 200 ? last[..200] + "…" : last)}";
    }

    internal sealed record ListedGroup(string GroupId, string State, string Type, bool IsSimple);

    internal sealed record ListingRequest(Dictionary<string, string> Config, int TimeoutMs);

    internal sealed record ListingResponse(List<ListedGroup>? Groups, string? Error);

    /// <summary>Groups could not be listed; the message is the reason.</summary>
    internal sealed class GroupListingException(string reason) : Exception(reason);

    [JsonSerializable(typeof(ListingRequest))]
    [JsonSerializable(typeof(ListingResponse))]
    internal sealed partial class ListingJson : JsonSerializerContext;
}
