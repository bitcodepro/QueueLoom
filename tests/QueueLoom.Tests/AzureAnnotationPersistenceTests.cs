using System.Text.Json;
using System.Text.Json.Nodes;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

/// <summary>Sender-set Azure AMQP message annotations survive every place a message is written to disk and read back.</summary>
public sealed class AzureAnnotationPersistenceTests
{
    internal static Dictionary<string, object?> AllTypes() => new(StringComparer.Ordinal)
    {
        ["null"] = null, ["bool"] = true, ["byte"] = (byte)200, ["sbyte"] = (sbyte)-5, ["short"] = (short)-300,
        ["ushort"] = (ushort)60000, ["int"] = -70000, ["uint"] = uint.MaxValue, ["long"] = long.MinValue, ["ulong"] = ulong.MaxValue,
        ["float"] = 1.1f, ["floatNaN"] = float.NaN, ["double"] = 0.1 + 0.2, ["nan"] = double.NaN,
        ["inf"] = double.PositiveInfinity, ["-inf"] = double.NegativeInfinity, ["decimal"] = 79228162514264337593543950335m,
        ["char"] = 'é', ["string"] = "text \"quoted\"", ["guid"] = Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        ["utc"] = new DateTime(2026, 10, 24, 1, 2, 3, DateTimeKind.Utc).AddTicks(4567),
        ["offset"] = new DateTimeOffset(2026, 10, 24, 1, 2, 3, TimeSpan.FromHours(5.5)),
        ["span"] = TimeSpan.FromTicks(-123456789012), ["uri"] = new Uri("amqp://host/path?q=1"), ["relative"] = new Uri("a/b", UriKind.Relative),
        ["bytes"] = new byte[] { 0, 255, 7 }, ["ints"] = new[] { 1, 2, 3 },
        ["list"] = new List<object?> { "a", 1L, null, new Dictionary<string, object?> { ["deep"] = new List<object?> { double.NaN } } },
        ["map"] = new Dictionary<object, object?> { [1] = "one", ["two"] = 2.5f }
    };

    /// <summary>The typed JSON form names every nested type and value, so equal forms mean equal annotations.</summary>
    internal static void AssertSame(IReadOnlyDictionary<string, object?>? expected, IReadOnlyDictionary<string, object?>? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected!.Keys.Order(StringComparer.Ordinal), actual!.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(AzureAnnotationJson.Encode(expected, out _)!.ToJsonString(),
            AzureAnnotationJson.Encode(expected.Keys.ToDictionary(key => key, key => actual[key]), out var skipped)!.ToJsonString());
        Assert.Empty(skipped);
        foreach (var (name, value) in expected) Assert.Equal(value?.GetType(), actual[name]?.GetType());
    }

    [Fact]
    public void Encoder_RoundTripsEveryAmqpTypeExactly()
    {
        var annotations = AllTypes();
        var encoded = AzureAnnotationJson.Encode(annotations, out var skipped);
        Assert.Empty(skipped);
        var decoded = AzureAnnotationJson.Decode(JsonDocument.Parse(encoded!.ToJsonString()).RootElement);

        AssertSame(annotations, decoded);
        Assert.True(double.IsNaN((double)decoded!["nan"]!));
        Assert.Equal(double.NegativeInfinity, decoded["-inf"]);
        Assert.Equal(ulong.MaxValue, decoded["ulong"]);
        Assert.Equal(79228162514264337593543950335m, decoded["decimal"]);
        Assert.Equal(DateTimeKind.Utc, ((DateTime)decoded["utc"]!).Kind);
        Assert.Equal(annotations["utc"], decoded["utc"]);
        Assert.Equal(annotations["offset"], decoded["offset"]);
        Assert.Equal(new byte[] { 0, 255, 7 }, decoded["bytes"]);
        Assert.Equal(new[] { 1, 2, 3 }, Assert.IsType<int[]>(decoded["ints"]));
        var list = Assert.IsType<List<object?>>(decoded["list"]);
        var nested = Assert.IsType<Dictionary<string, object?>>(list[3]);
        Assert.True(double.IsNaN((double)Assert.IsType<List<object?>>(nested["deep"])[0]!));
        Assert.Equal("one", Assert.IsType<Dictionary<object, object?>>(decoded["map"])[1]);
        Assert.Equal("a/b", ((Uri)decoded["relative"]!).OriginalString);
    }

    [Fact]
    public void Encoder_SkipsOnlyTheAnnotationWhoseTypeHasNoLosslessForm()
    {
        var annotations = new Dictionary<string, object?>
        {
            ["kept"] = 5, ["odd"] = new object(), ["nestedOdd"] = new List<object?> { 1, new Version(1, 2) }
        };
        var deep = new List<object?>();
        var current = deep;
        for (var level = 0; level < AzureAnnotationJson.MaximumNesting + 2; level++) { var next = new List<object?>(); current.Add(next); current = next; }
        annotations["tooDeep"] = deep;

        var encoded = AzureAnnotationJson.Encode(annotations, out var skipped);

        Assert.Equal(["kept"], encoded!.Select(pair => pair.Key));
        Assert.Equal(3, skipped.Count);
        Assert.Contains(skipped, warning => warning.StartsWith("odd: ", StringComparison.Ordinal) && warning.Contains("System.Object", StringComparison.Ordinal));
        Assert.Contains(skipped, warning => warning.StartsWith("nestedOdd: ", StringComparison.Ordinal) && warning.Contains("System.Version", StringComparison.Ordinal));
        Assert.Contains(skipped, warning => warning.StartsWith("tooDeep: ", StringComparison.Ordinal));
        Assert.Equal(5, AzureAnnotationJson.Decode(encoded)!["kept"]);
        Assert.Null(AzureAnnotationJson.Encode(new Dictionary<string, object?> { ["odd"] = new object() }, out _));
        Assert.Null(AzureAnnotationJson.Decode((JsonElement?)null));
        Assert.Throws<InvalidDataException>(() => AzureAnnotationJson.Decode(JsonDocument.Parse("""{"a":{"type":"whatever","value":1}}""").RootElement));
    }

    private static BrowsedMessage Message(IReadOnlyDictionary<string, object?>? annotations = null) =>
        new(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 7, "body"u8.ToArray(),
            new EditableMessageProperties(MessageId: "original"), [new MessageApplicationProperty("tenant", ApplicationPropertyType.String, "x")])
        { AzureMessageAnnotations = annotations ?? AllTypes() };

    private static ServiceBusProfile Profile() => ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development,
        AuthenticationSettings.Entra(), "isolated.servicebus.windows.net", accessMode: ProfileAccessMode.ReadWrite);

    private static ScheduledResend Job(ServiceBusProfile profile, BrowsedMessage message) =>
        new(Guid.NewGuid(), profile.Id, profile.Name, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, ResendMode.Copy, 0, "isolated",
            [ScheduledResendItem.From(new ResendItem(message, message.Source, message.CreateDraft()))])
        { ConfigurationIdentity = ScheduledResend.IdentityFor(profile) };

    [Fact]
    public void ScheduledResend_KeepsAnnotationsAcrossSaveAndReload()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var message = Message();
        new JsonScheduledResendStore(paths).Save([Job(Profile(), message)]);

        var draft = Assert.Single(Assert.Single(new JsonScheduledResendStore(paths).Load()).Items).Message;

        AssertSame(message.AzureMessageAnnotations, draft.AzureMessageAnnotations);
    }

    [Fact]
    public void ScheduledResend_RecordsAnAnnotationItCannotKeepAndSavesTheRest()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var store = new JsonScheduledResendStore(paths);
        store.Save([Job(Profile(), Message(new Dictionary<string, object?> { ["kept"] = "yes", ["odd"] = new object() }))]);

        var draft = Assert.Single(Assert.Single(store.Load()).Items).Message;

        Assert.Equal("yes", Assert.Single(draft.AzureMessageAnnotations!).Value);
        Assert.Contains("odd: ", File.ReadAllText(store.FilePath), StringComparison.Ordinal);
    }

    [Fact]
    public void ScheduledResend_WithAnAnnotationItCannotKeep_IsRemovedAsAddedAndKeepsItsRecordAcrossRewrites()
    {
        using var directory = new TemporaryDirectory();
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(directory.Path));
        var profile = Profile();
        var job = Job(profile, Message(new Dictionary<string, object?> { ["kept"] = "yes", ["odd"] = new object() }));
        var other = Job(profile, Message());
        store.Add(job);
        store.Add(other);
        Assert.True(store.TryRemove(other));
        Assert.Contains("odd: ", File.ReadAllText(store.FilePath), StringComparison.Ordinal);

        Assert.True(store.TryRemove(job));
        Assert.Empty(store.Load());
    }

    [Fact]
    public async Task BatchReplayAndDurableResend_SendTheAnnotationsTheyStored()
    {
        using var directory = new TemporaryDirectory();
        var profile = Profile();
        var message = Message();
        await using var workspace = new ViewModelStateTests.FakeWorkspace();
        await workspace.ConnectAsync(profile);
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
        var plan = await store.CreateAsync(profile.Id, message.Source, [(message.CreateDraft(), "isolated")], true, 50, default,
            profile.EndpointDisplay, ScheduledResend.IdentityFor(profile));

        await new BatchReplayStore(store.RootDirectory).RunAsync(plan, workspace, () => true, null, default);

        AssertSame(message.AzureMessageAnnotations, Assert.Single(workspace.SentMessages).Message.AzureMessageAnnotations);
        var copies = new BatchReplayStore(Path.Combine(directory.Path, "copies"));
        var copyPlan = await copies.CreateResendAsync(profile.Id, [new ResendItem(message, message.Source, message.CreateDraft())],
            ResendMode.Copy, 50, profile.EndpointDisplay, ScheduledResend.IdentityFor(profile), "Resend", default);
        await new BatchReplayStore(copies.RootDirectory).RunItemsAsync(copyPlan, [0], false, workspace, () => true, null, default);
        AssertSame(message.AzureMessageAnnotations, workspace.SentMessages[^1].Message.AzureMessageAnnotations);
    }

    [Fact]
    public async Task Backup_KeepsAnnotationsThroughRestore()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var message = Message();
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(Profile(), DateTimeOffset.UtcNow, default);
        await session.BackupAsync(message, default);
        var repository = new JsonDeadLetterBackupRepository(paths);

        var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));

        AssertSame(message.AzureMessageAnnotations, restored.AzureMessageAnnotations);
        AssertSame(message.AzureMessageAnnotations, restored.CreateDraft().AzureMessageAnnotations);
    }

    [Fact]
    public async Task Backup_OfAReceivedAzureMessageKeepsOnlySenderAnnotations()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(body: new BinaryData("body"u8.ToArray()), messageId: "m", sequenceNumber: 3);
        var raw = received.GetRawAmqpMessage().MessageAnnotations;
        raw["sender"] = double.NaN;
        raw["x-opt-custom-broker"] = "broker";
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(Profile(), DateTimeOffset.UtcNow, default);
        await session.BackupAsync(received, ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, default);
        var repository = new JsonDeadLetterBackupRepository(paths);

        var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));

        Assert.True(double.IsNaN((double)Assert.Single(restored.AzureMessageAnnotations!, pair => pair.Key == "sender").Value!));
        Assert.Single(restored.AzureMessageAnnotations!);
    }

    [Fact]
    public async Task Backup_WrittenBeforeAnnotationsLoadsWithNone()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var session = await new DeadLetterJsonBackupStore(paths).CreateSessionAsync(Profile(), DateTimeOffset.UtcNow, default);
        var file = await session.BackupAsync(Message(), default);
        var json = JsonNode.Parse(File.ReadAllText(file))!.AsObject();
        Assert.True(json.Remove("azureMessageAnnotations"));
        File.WriteAllText(file, json.ToJsonString());
        var repository = new JsonDeadLetterBackupRepository(paths);

        Assert.Null((await repository.LoadAsync(Assert.Single(await repository.ListAsync()))).AzureMessageAnnotations);
    }

    [Fact]
    public async Task JsonExport_KeepsAnnotationsThroughImport()
    {
        var message = Message();
        using var output = new MemoryStream();
        await MessageExport.WriteJsonAsync(output, [new ExportedMessage("isolated", message)], default);

        var imported = Assert.Single(await MessageImport.ReadAsync(new MemoryStream(output.ToArray())));

        AssertSame(message.AzureMessageAnnotations, imported.Draft.AzureMessageAnnotations);
    }

    [Fact]
    public async Task JsonExport_OfAMessageWithoutAnnotationsWritesNone()
    {
        using var output = new MemoryStream();
        await MessageExport.WriteJsonAsync(output, [new ExportedMessage("isolated", Message() with { AzureMessageAnnotations = null })], default);

        Assert.DoesNotContain("azureMessageAnnotations", System.Text.Encoding.UTF8.GetString(output.ToArray()), StringComparison.Ordinal);
        Assert.Null(Assert.Single(await MessageImport.ReadAsync(new MemoryStream(output.ToArray()))).Draft.AzureMessageAnnotations);
    }
}
