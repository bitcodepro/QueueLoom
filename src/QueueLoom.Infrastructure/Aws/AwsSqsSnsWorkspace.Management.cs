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

    // SetQueueAttributes: MessageRetentionPeriod 60 to 1,209,600 seconds, VisibilityTimeout 0 to 43,200 seconds;
    // a redrive policy's maxReceiveCount (Maximum receives) 1 to 1,000.
    internal static readonly QueueSettingLimits SqsLimits = new("Amazon SQS")
    {
        MinTimeToLive = TimeSpan.FromSeconds(60),
        MaxTimeToLive = TimeSpan.FromSeconds(1_209_600),
        TimeToLiveName = "retention period",
        MinDeliveryCount = 1,
        MaxDeliveryCount = 1_000,
        DeliveryCountName = "maximum receive count",
        MinLock = TimeSpan.Zero,
        MaxLock = TimeSpan.FromSeconds(43_200),
        LockName = "visibility timeout"
    };

    public override QueueManagementCapabilities? QueueManagement => new("queue", SqsSettings, SqsSettings, CanCreateDeadLetterQueue: true,
        UpdateNote: "Time to live is the SQS retention period (1 minute to 14 days); the lock is the visibility timeout (up to 12 hours).")
    {
        Limits = SqsLimits
    };

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

            // CreateQueue returns the URL of an existing queue whose attributes match instead of failing, so a taken name
            // is refused here, before anything is created; otherwise an existing queue would be reported as created and
            // an existing "-dlq" queue (perhaps another queue's) silently adopted.
            await EnsureQueueMissingAsync(definition.Name, token).ConfigureAwait(false);
            string? createdDeadLetter = null;
            if (definition.CreateDeadLetterQueue)
            {
                var deadLetterName = fifo ? definition.Name[..^5] + "-dlq.fifo" : definition.Name + "-dlq";
                await EnsureQueueMissingAsync(deadLetterName, token).ConfigureAwait(false);
                // Keep dead letters as long as SQS allows, so there is time to look at them.
                var deadLetterAttributes = new Dictionary<string, string>(StringComparer.Ordinal) { ["MessageRetentionPeriod"] = "1209600" };
                if (fifo)
                {
                    deadLetterAttributes["FifoQueue"] = "true";
                }
                var deadLetter = await Sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = deadLetterName, Attributes = deadLetterAttributes },
                    token).ConfigureAwait(false);
                createdDeadLetter = deadLetter.QueueUrl;
                var arn = (await Sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
                {
                    QueueUrl = deadLetter.QueueUrl,
                    AttributeNames = ["QueueArn"]
                }, token).ConfigureAwait(false)).Attributes["QueueArn"];
                attributes["RedrivePolicy"] = RedrivePolicy(arn, definition.Settings.MaxDeliveryCount ?? 5);
            }

            try
            {
                await Sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = definition.Name, Attributes = attributes }, token).ConfigureAwait(false);
            }
            catch (AmazonSQSException exception) when (createdDeadLetter is not null && (int)exception.StatusCode is >= 400 and < 500)
            {
                // SQS refused the queue (a 4xx), so nothing routes to the dead-letter queue this call created; leaving it
                // would block every retry. It stays when the queue exists after all (another creator may route to it)
                // or its state cannot be read.
                if (await QueueExistsAsync(definition.Name).ConfigureAwait(false) == false)
                {
                    try
                    {
                        await Sqs.DeleteQueueAsync(createdDeadLetter, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (AmazonSQSException)
                    {
                    }
                }
                throw;
            }
        }, cancellationToken);

    private async Task EnsureQueueMissingAsync(string name, CancellationToken cancellationToken)
    {
        try
        {
            await Sqs.GetQueueUrlAsync(name, cancellationToken).ConfigureAwait(false);
        }
        catch (QueueDoesNotExistException)
        {
            return;
        }
        throw new InvalidOperationException($"A queue named '{name}' already exists.");
    }

    /// <summary>True or false when SQS answers clearly; null when the lookup itself fails.</summary>
    private async Task<bool?> QueueExistsAsync(string name)
    {
        try
        {
            await Sqs.GetQueueUrlAsync(name, CancellationToken.None).ConfigureAwait(false);
            return true;
        }
        catch (QueueDoesNotExistException)
        {
            return false;
        }
        catch (AmazonSQSException)
        {
            return null;
        }
    }

    public override Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default) =>
        ManageAsync(async token =>
        {
            var attributes = Attributes(settings);
            var url = await QueueUrlAsync(queue, token).ConfigureAwait(false);
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

    /// <summary>
    /// The queue attributes for <paramref name="settings"/>. A value outside SQS's range is refused, never changed to the nearest
    /// allowed one: a queue that keeps messages 14 days when 30 were asked for loses them without anyone knowing.
    /// </summary>
    private static Dictionary<string, string> Attributes(QueueSettings settings)
    {
        if (SqsLimits.Check(settings) is { } error)
        {
            throw new InvalidOperationException(error);
        }
        var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
        if (settings.MessageTimeToLive is { } retention)
        {
            attributes["MessageRetentionPeriod"] = ((long)retention.TotalSeconds).ToString(CultureInfo.InvariantCulture);
        }
        if (settings.LockDuration is { } visibility)
        {
            attributes["VisibilityTimeout"] = ((long)visibility.TotalSeconds).ToString(CultureInfo.InvariantCulture);
        }
        return attributes;
    }

    private static string RedrivePolicy(string deadLetterArn, int maxReceiveCount) =>
        JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["deadLetterTargetArn"] = deadLetterArn,
            ["maxReceiveCount"] = maxReceiveCount
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
