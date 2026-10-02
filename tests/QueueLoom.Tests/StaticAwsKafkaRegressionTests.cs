using System.Reflection;
using System.Text.Json;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.SimpleNotificationService;
using Confluent.Kafka;
using QueueLoom.App.Serialization;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Tests.Infrastructure;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.Tests;

public sealed class StaticAwsKafkaRegressionTests
{
    [Theory]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task FutureUnsupportedScheduling_RejectsBeforeAnySdkSend(bool topic, bool fifo)
    {
        using var fixture = new AwsFixture();
        var target = topic ? ServiceBusEntityReference.Topic(fifo ? "events.fifo" : "events") : ServiceBusEntityReference.Queue("orders.fifo");
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Send(target, fixture.Draft(future: true)));
        Assert.Empty(fixture.Sqs.Sends);
        Assert.Empty(fixture.Sns.Sends);
    }

    [Fact]
    public async Task StandardSqs_PreservesDelayAndPastUnsupportedSchedulesSendImmediately()
    {
        using var fixture = new AwsFixture();
        await fixture.Send(ServiceBusEntityReference.Queue("orders"), fixture.Draft(future: true));
        Assert.Equal(120, Assert.Single(fixture.Sqs.Sends).DelaySeconds);
        await fixture.Send(ServiceBusEntityReference.Queue("orders.fifo"), fixture.Draft(future: false));
        await fixture.Send(ServiceBusEntityReference.Topic("events"), fixture.Draft(future: false));
        Assert.Equal(2, fixture.Sqs.Sends.Count);
        Assert.Single(fixture.Sns.Sends);
    }

    [Fact]
    public async Task KafkaPositionNames_BrowseInspectorComposerCsvAndRawReplayAreIndependent()
    {
        var headers = new Headers();
        headers.Add("kafka.partition", "user-partition"u8.ToArray());
        headers.Add("kafka.offset", new byte[] { 0xff, 0x00 });
        headers.Add("kafka.partition", null);
        var record = new ConsumeResult<byte[]?, byte[]?> { Topic = "events", Partition = 3, Offset = 42,
            Message = new() { Key = [0xfe], Value = "body"u8.ToArray(), Headers = headers } };
        var browsed = KafkaMessageMapper.FromKafka(record, ServiceBusEntityReference.Queue("events"), ServiceBusSubQueue.Active);
        Assert.Equal(new LogPosition(3, 42), browsed.Position);
        var vm = new MessageItemViewModel(browsed);
        using var inspector = JsonDocument.Parse(vm.ApplicationPropertiesJson);
        using var runtime = JsonDocument.Parse(vm.PropertiesJson);
        Assert.Equal(3, runtime.RootElement.GetProperty("runtime").GetProperty("position").GetProperty("Partition").GetInt32());
        Assert.Equal(42, runtime.RootElement.GetProperty("runtime").GetProperty("position").GetProperty("Offset").GetInt64());
        Assert.Contains("partition 3", vm.MessageId, StringComparison.Ordinal);
        Assert.Contains("offset 42", vm.MessageId, StringComparison.Ordinal);
        var draft = browsed.CreateDraft();
        var composer = ApplicationPropertiesJson.Deserialize(ApplicationPropertiesJson.Serialize(draft.ApplicationProperties));
        Assert.Equal(draft.ApplicationProperties, composer);
        using var csv = new StringWriter();
        await MessageExport.WriteCsvAsync(csv, [new("Fake", browsed)], default);
        Assert.Contains("kafka.offset", csv.ToString(), StringComparison.Ordinal);
        Assert.Contains(",3,42,text,body", csv.ToString(), StringComparison.Ordinal);
        using var json = new MemoryStream();
        await MessageExport.WriteJsonAsync(json, [new("Fake", browsed)], default);
        using var exported = JsonDocument.Parse(json.ToArray());
        Assert.Equal(3, exported.RootElement[0].GetProperty("partition").GetInt32());
        Assert.Equal(42, exported.RootElement[0].GetProperty("offset").GetInt64());
        var replay = KafkaMessageMapper.ToKafka(draft);
        Assert.Equal(record.Message.Key, replay.Key);
        Assert.Equal(record.Message.Value, replay.Value);
        Assert.Equal(headers.Select(h => (h.Key, h.GetValueBytes() is null ? null : Convert.ToBase64String(h.GetValueBytes()))),
            replay.Headers.Select(h => (h.Key, h.GetValueBytes() is null ? null : Convert.ToBase64String(h.GetValueBytes()))));
        // An explicit composer edit replaces both raw occurrences, including a null header.
        var edited = new MessageDraft(draft.Body, draft.Properties,
            composer.Select(p => p.Name == "kafka.partition" ? p with { Value = "edited" } : p)) { KafkaEnvelope = draft.KafkaEnvelope };
        Assert.Equal("edited", System.Text.Encoding.UTF8.GetString(KafkaMessageMapper.ToKafka(edited).Headers.Single(h => h.Key == "kafka.partition").GetValueBytes()));
    }

    private sealed class AwsFixture : IDisposable
    {
        private readonly AwsSqsSnsWorkspace _workspace;
        public SqsFake Sqs { get; } = new();
        public SnsFake Sns { get; } = new();
        private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-10-02T00:00:00Z");
        public AwsFixture()
        {
            _workspace = new AwsSqsSnsWorkspace(new NullVault(), new Clock());
            void Set(string name, object value) => typeof(AwsSqsSnsWorkspace).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(_workspace, value);
            Set("_sqs", Sqs); Set("_sns", Sns);
            Set("_index", new AwsTopologyIndex(new[] { "orders", "orders.fifo" }.Select(name =>
                AwsQueueInfo.From("https://fake.invalid/" + name, new Dictionary<string, string> { ["QueueArn"] = "arn:aws:sqs:us-east-1:0:" + name })),
                new[] { "events", "events.fifo" }.Select(name => AwsTopicInfo.From("arn:aws:sns:us-east-1:0:" + name, []))));
        }
        public MessageDraft Draft(bool future) => new(EditableMessageBody.Empty, new EditableMessageProperties(SessionId: "group",
            ScheduledEnqueueTime: Now.AddSeconds(future ? 120 : -120)));
        public Task Send(ServiceBusEntityReference target, MessageDraft draft) => (Task)typeof(AwsSqsSnsWorkspace)
            .GetMethod("SendCoreAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(_workspace,
                [new ServiceBusTopology(Now), target, draft, CancellationToken.None])!;
        public void Dispose() { Sqs.Dispose(); Sns.Dispose(); }
        private sealed class Clock : TimeProvider { public override DateTimeOffset GetUtcNow() => Now; }
    }
    private sealed class NullVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult(false);
    }
    private sealed class SqsFake() : AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public List<Amazon.SQS.Model.SendMessageRequest> Sends { get; } = [];
        public override Task<SendMessageResponse> SendMessageAsync(Amazon.SQS.Model.SendMessageRequest request, CancellationToken cancellationToken = default)
        { Sends.Add(request); return Task.FromResult(new SendMessageResponse()); }
    }
    private sealed class SnsFake() : AmazonSimpleNotificationServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public List<Sns.PublishRequest> Sends { get; } = [];
        public override Task<Sns.PublishResponse> PublishAsync(Sns.PublishRequest request, CancellationToken cancellationToken = default)
        { Sends.Add(request); return Task.FromResult(new Sns.PublishResponse()); }
    }
}
