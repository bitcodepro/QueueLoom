using System.Runtime.InteropServices;
using System.Text;

namespace QueueLoom.Tests;

/// <summary>
/// librdkafka's in-process mock cluster, driven through the C API the Confluent.Kafka package ships. It is a real Kafka
/// protocol server on localhost that can answer requests with errors, so tests can refuse an idempotent producer ID or
/// keep a send from ever being acknowledged, without a broker.
/// </summary>
internal sealed class KafkaMockCluster : IDisposable
{
    private readonly IntPtr _library;
    private readonly IntPtr _handle;
    private readonly IntPtr _cluster;

    public KafkaMockCluster()
    {
        _library = LoadLibrdkafka();
        var errors = new StringBuilder(512);
        var conf = Function<ConfNew>("rd_kafka_conf_new")();
        _handle = Function<New>("rd_kafka_new")(0, conf, errors, (UIntPtr)errors.Capacity);
        if (_handle == IntPtr.Zero) throw new InvalidOperationException("rd_kafka_new failed: " + errors);
        _cluster = Function<ClusterNew>("rd_kafka_mock_cluster_new")(_handle, 1);
        if (_cluster == IntPtr.Zero) throw new InvalidOperationException("rd_kafka_mock_cluster_new failed.");
        BootstrapServers = Marshal.PtrToStringAnsi(Function<Bootstraps>("rd_kafka_mock_cluster_bootstraps")(_cluster))!;
    }

    public string BootstrapServers { get; }

    public void CreateTopic(string topic) =>
        Check(Function<TopicCreate>("rd_kafka_mock_topic_create")(_cluster, topic, 1, 1));

    /// <summary>The next <paramref name="count"/> requests of the API key are answered with the error, unapplied.</summary>
    public void FailRequests(short apiKey, int error, int count) =>
        Function<PushErrors>("rd_kafka_mock_push_request_errors_array")(_cluster, apiKey, (UIntPtr)count, Enumerable.Repeat(error, count).ToArray());

    public void Dispose()
    {
        Function<ClusterDestroy>("rd_kafka_mock_cluster_destroy")(_cluster);
        Function<Destroy>("rd_kafka_destroy")(_handle);
    }

    public const short ProduceApi = 0;
    public const short InitProducerIdApi = 22;
    public const int ClusterAuthorizationFailed = 31;
    public const int RequestTimedOut = 7;

    private static void Check(int error)
    {
        if (error != 0) throw new InvalidOperationException($"librdkafka mock call failed with error {error}.");
    }

    private T Function<T>(string name) where T : Delegate =>
        Marshal.GetDelegateForFunctionPointer<T>(NativeLibrary.GetExport(_library, name));

    // The same native library Confluent.Kafka loads; a second copy in the process only serves the mock's sockets.
    private static IntPtr LoadLibrdkafka()
    {
        var native = Path.Combine(AppContext.BaseDirectory, "runtimes", RuntimeInformation.RuntimeIdentifier, "native");
        if (!Directory.Exists(native))
        {
            var architecture = RuntimeInformation.ProcessArchitecture.ToString().ToLowerInvariant();
            var os = OperatingSystem.IsWindows() ? "win" : OperatingSystem.IsMacOS() ? "osx" : "linux";
            native = Path.Combine(AppContext.BaseDirectory, "runtimes", $"{os}-{architecture}", "native");
        }
        string[] names = OperatingSystem.IsWindows() ? ["librdkafka.dll"]
            : OperatingSystem.IsMacOS() ? ["librdkafka.dylib"]
            : ["librdkafka.so", "centos8-librdkafka.so"];
        foreach (var name in names)
        {
            if (NativeLibrary.TryLoad(Path.Combine(native, name), typeof(KafkaMockCluster).Assembly,
                    DllImportSearchPath.UseDllDirectoryForDependencies, out var handle)) return handle;
        }
        throw new DllNotFoundException($"librdkafka was not found in {native}.");
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate IntPtr ConfNew();
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate IntPtr New(int type, IntPtr conf, StringBuilder errors, UIntPtr size);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate IntPtr ClusterNew(IntPtr handle, int brokers);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate IntPtr Bootstraps(IntPtr cluster);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate int TopicCreate(IntPtr cluster, [MarshalAs(UnmanagedType.LPStr)] string topic, int partitions, int replication);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate void PushErrors(IntPtr cluster, short apiKey, UIntPtr count, int[] errors);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate void ClusterDestroy(IntPtr cluster);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate void Destroy(IntPtr handle);
}
