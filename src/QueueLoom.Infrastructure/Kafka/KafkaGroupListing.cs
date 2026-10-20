using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

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
        var request = JsonSerializer.Deserialize(input, ListingJson.Default.ListingRequest)
                      ?? throw new InvalidDataException("No listing request.");
        ListingResponse response;
        using (var admin = new AdminClientBuilder(new AdminClientConfig(request.Config)).Build())
        {
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
        }
        JsonSerializer.Serialize(output, response, ListingJson.Default.ListingResponse);
        output.Flush();
        return 0;
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
        using var child = Process.Start(start) ?? throw new GroupListingException("the listing process did not start");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(timeout + ChildGrace);
        try
        {
            var error = child.StandardError.ReadToEndAsync(deadline.Token);
            var output = child.StandardOutput.ReadToEndAsync(deadline.Token);
            // The config carries the SASL password: it goes through the pipe, never the command line.
            try
            {
                await JsonSerializer.SerializeAsync(child.StandardInput.BaseStream,
                    new ListingRequest(config.ToDictionary(pair => pair.Key, pair => pair.Value), (int)timeout.TotalMilliseconds),
                    ListingJson.Default.ListingRequest, deadline.Token).ConfigureAwait(false);
                child.StandardInput.Close();
            }
            catch (IOException)
            {
                // The child ended before reading the request (a broken pipe); its exit code says why, below.
            }
            await child.WaitForExitAsync(deadline.Token).ConfigureAwait(false);
            var text = await output.ConfigureAwait(false);
            await error.ConfigureAwait(false);
            // A failed listing can abort the child after it answered (the double free happens when librdkafka cleans
            // up), so a complete answer counts whatever the exit code.
            if (string.IsNullOrWhiteSpace(text))
            {
                throw new GroupListingException($"the listing process stopped with exit code {child.ExitCode}");
            }
            ListingResponse? response;
            try
            {
                response = JsonSerializer.Deserialize(text, ListingJson.Default.ListingResponse);
            }
            catch (JsonException) when (child.ExitCode != 0)
            {
                throw new GroupListingException($"the listing process stopped with exit code {child.ExitCode}");
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
                try { child.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
            }
        }
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
