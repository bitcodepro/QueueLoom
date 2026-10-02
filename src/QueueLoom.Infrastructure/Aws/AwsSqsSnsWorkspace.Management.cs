using System.Globalization;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Aws;

/// <summary>Creating, changing and deleting SQS queues, with a dead-letter queue connected by a redrive policy.</summary>
public sealed partial class AwsSqsSnsWorkspace
{
    private const QueueSettingFlags SqsSettings =
        QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount | QueueSettingFlags.LockDuration;

    public override QueueManagementCapabilities? QueueManagement => new("queue", SqsSettings, SqsSettings, CanCreateDeadLetterQueue: true,
        UpdateNote: "Time to live is the SQS retention period (1 minute to 14 days); the lock is the visibility timeout (up to 12 hours).");

    public override async Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default)
    {
        using var operation = await EnterReadOperationAsync(cancellationToken).ConfigureAwait(false);
        var url = await QueueUrlAsync(queue, cancellationToken).ConfigureAwait(false);
        var attributes = (await Sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest { QueueUrl = url, AttributeNames = ["All"] },
            cancellationToken).ConfigureAwait(false)).Attributes;
        return new QueueSettings(
            Seconds(attributes, "MessageRetentionPeriod"),
            MaxReceiveCount(attributes.GetValueOrDefault("RedrivePolicy")),
            Seconds(attributes, "VisibilityTimeout"));
    }

    public override Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            ArgumentNullException.ThrowIfNull(definition);
            var fifo = definition.Name.EndsWith(".fifo", StringComparison.Ordinal);
            var attributes = Attributes(definition.Settings);
            if (fifo)
            {
                attributes["FifoQueue"] = "true";
            }

            if (definition.CreateDeadLetterQueue)
            {
                var deadLetterName = fifo ? definition.Name[..^5] + "-dlq.fifo" : definition.Name + "-dlq";
                // Keep dead letters as long as SQS allows, so there is time to look at them.
                var deadLetterAttributes = new Dictionary<string, string>(StringComparer.Ordinal) { ["MessageRetentionPeriod"] = "1209600" };
                if (fifo)
                {
                    deadLetterAttributes["FifoQueue"] = "true";
                }
                var deadLetter = await Sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = deadLetterName, Attributes = deadLetterAttributes },
                    token).ConfigureAwait(false);
                var arn = (await Sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
                {
                    QueueUrl = deadLetter.QueueUrl,
                    AttributeNames = ["QueueArn"]
                }, token).ConfigureAwait(false)).Attributes["QueueArn"];
                attributes["RedrivePolicy"] = RedrivePolicy(arn, definition.Settings.MaxDeliveryCount ?? 5);
            }

            await Sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = definition.Name, Attributes = attributes }, token).ConfigureAwait(false);
        }, cancellationToken);

    public override Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            var url = await QueueUrlAsync(queue, token).ConfigureAwait(false);
            var attributes = Attributes(settings);
            if (settings.MaxDeliveryCount is { } maxReceiveCount)
            {
                var current = (await Sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest { QueueUrl = url, AttributeNames = ["RedrivePolicy"] },
                    token).ConfigureAwait(false)).Attributes.GetValueOrDefault("RedrivePolicy");
                var target = AwsQueueInfo.ReadDeadLetterTargetArn(current)
                             ?? throw new InvalidOperationException($"Queue '{queue}' has no dead-letter queue, so it has no maximum receive count.");
                attributes["RedrivePolicy"] = RedrivePolicy(target, maxReceiveCount);
            }
            if (attributes.Count > 0)
            {
                await Sqs.SetQueueAttributesAsync(new SetQueueAttributesRequest { QueueUrl = url, Attributes = attributes }, token).ConfigureAwait(false);
            }
        }, cancellationToken);

    public override Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            var url = await QueueUrlAsync(queue, token).ConfigureAwait(false);
            await Sqs.DeleteQueueAsync(url, token).ConfigureAwait(false);
        }, cancellationToken);

    private async Task<string> QueueUrlAsync(string queue, CancellationToken cancellationToken)
    {
        try
        {
            return (await Sqs.GetQueueUrlAsync(queue, cancellationToken).ConfigureAwait(false)).QueueUrl;
        }
        catch (QueueDoesNotExistException exception)
        {
            throw new InvalidOperationException($"Queue '{queue}' was not found.", exception);
        }
    }

    private static Dictionary<string, string> Attributes(QueueSettings settings)
    {
        var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
        if (settings.MessageTimeToLive is { } retention)
        {
            attributes["MessageRetentionPeriod"] = ((long)Math.Clamp(retention.TotalSeconds, 60, 1_209_600)).ToString(CultureInfo.InvariantCulture);
        }
        if (settings.LockDuration is { } visibility)
        {
            attributes["VisibilityTimeout"] = ((long)Math.Clamp(visibility.TotalSeconds, 0, 43_200)).ToString(CultureInfo.InvariantCulture);
        }
        return attributes;
    }

    private static string RedrivePolicy(string deadLetterArn, int maxReceiveCount) =>
        JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["deadLetterTargetArn"] = deadLetterArn,
            ["maxReceiveCount"] = Math.Clamp(maxReceiveCount, 1, 1_000)
        });

    private static TimeSpan? Seconds(IReadOnlyDictionary<string, string> attributes, string name) =>
        long.TryParse(attributes.GetValueOrDefault(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds)
            ? TimeSpan.FromSeconds(seconds)
            : null;

    private static int? MaxReceiveCount(string? redrivePolicy)
    {
        if (string.IsNullOrWhiteSpace(redrivePolicy))
        {
            return null;
        }
        using var document = JsonDocument.Parse(redrivePolicy);
        return document.RootElement.TryGetProperty("maxReceiveCount", out var count)
            ? count.ValueKind == JsonValueKind.Number ? count.GetInt32() : int.Parse(count.GetString()!, CultureInfo.InvariantCulture)
            : null;
    }
}
