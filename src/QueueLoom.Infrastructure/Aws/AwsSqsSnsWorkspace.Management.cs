using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Aws;

/// <summary>Creating, changing and deleting SQS queues, with a dead-letter queue connected by a redrive policy.</summary>
public sealed partial class AwsSqsSnsWorkspace
{
    private const QueueSettingFlags SqsSettings =
        QueueSettingFlags.MessageTimeToLive | QueueSettingFlags.MaxDeliveryCount | QueueSettingFlags.LockDuration;

    /// <summary>The longest SQS retention, 14 days: dead-letter queues keep their messages as long as SQS allows.</summary>
    private const int FullRetentionSeconds = 1_209_600;

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
            cancellationToken).ConfigureAwait(false)).Attributes ?? [];
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
            string? createdDeadLetterName = null;
            if (definition.CreateDeadLetterQueue)
            {
                var deadLetterName = fifo ? definition.Name[..^5] + "-dlq.fifo" : definition.Name + "-dlq";
                await EnsureQueueMissingAsync(deadLetterName, token).ConfigureAwait(false);
                // Another operator can create "<name>-dlq" between that check and CreateQueue, and SQS then answers with
                // the existing queue's URL when the attributes match; the queue would then dead-letter into someone else's
                // queue. CreateQueue documents the way to tell: an existing name whose attributes differ from the request
                // fails with QueueNameExists ("only if the request includes attributes whose values differ from those of
                // the existing queue"). So the dead-letter queue is first created with a retention nobody else picks
                // (14 days minus a random number of seconds below one day): SQS refuses it for any existing queue, and the
                // retention read back right afterwards proves the queue is the one this call created (also against
                // SQS-compatible servers that return an existing queue whatever the attributes). Tags cannot prove it:
                // CreateQueue does not document whether tags are applied to, compared with or ignored for an existing
                // queue, and tagging needs sqs:TagQueue as well.
                var marker = (FullRetentionSeconds - RandomNumberGenerator.GetInt32(1, 86_400)).ToString(CultureInfo.InvariantCulture);
                var deadLetterAttributes = new Dictionary<string, string>(StringComparer.Ordinal) { ["MessageRetentionPeriod"] = marker };
                if (fifo)
                {
                    deadLetterAttributes["FifoQueue"] = "true";
                }
                CreateQueueResponse deadLetter;
                try
                {
                    deadLetter = await Sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = deadLetterName, Attributes = deadLetterAttributes },
                        token).ConfigureAwait(false);
                }
                catch (QueueNameExistsException exception)
                {
                    throw new InvalidOperationException(
                        $"A queue named '{deadLetterName}' was created by someone else while QueueLoom was creating '{definition.Name}'. " +
                        $"Nothing was created; choose another name or connect '{definition.Name}' to that queue yourself.", exception);
                }
                createdDeadLetter = deadLetter.QueueUrl;
                createdDeadLetterName = deadLetterName;
                var deadLetterState = (await Sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
                {
                    QueueUrl = deadLetter.QueueUrl,
                    AttributeNames = ["QueueArn", "MessageRetentionPeriod"]
                }, token).ConfigureAwait(false)).Attributes;
                if (!string.Equals(deadLetterState.GetValueOrDefault("MessageRetentionPeriod"), marker, StringComparison.Ordinal))
                {
                    // Not proven to be this call's queue: it is neither used nor deleted (DeleteQueue removes a queue
                    // whatever it holds), and the main queue is not created.
                    throw new InvalidOperationException(
                        $"SQS answered with a queue named '{deadLetterName}' that QueueLoom did not create; another operator " +
                        $"probably created it meanwhile. '{definition.Name}' was not created and '{deadLetterName}' was left " +
                        "unchanged; check who uses it before deleting it in AWS.");
                }
                try
                {
                    // Keep dead letters as long as SQS allows, so there is time to look at them.
                    await Sqs.SetQueueAttributesAsync(new SetQueueAttributesRequest
                    {
                        QueueUrl = deadLetter.QueueUrl,
                        Attributes = new Dictionary<string, string>(StringComparer.Ordinal)
                        {
                            ["MessageRetentionPeriod"] = FullRetentionSeconds.ToString(CultureInfo.InvariantCulture)
                        }
                    }, token).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is AmazonServiceException or HttpRequestException or TaskCanceledException)
                {
                    // A dead letter keeps its original enqueue age, so a shortened retention can expire it early after
                    // redrive. The outcome may also be unknown (a lost response): only a read-back of 14 days lets the
                    // queue be connected; otherwise nothing more is created and the operator is told what to set.
                    string? retention = null;
                    try
                    {
                        retention = (await Sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
                        {
                            QueueUrl = deadLetter.QueueUrl,
                            AttributeNames = ["MessageRetentionPeriod"]
                        }, CancellationToken.None).ConfigureAwait(false)).Attributes.GetValueOrDefault("MessageRetentionPeriod");
                    }
                    catch (Exception readBack) when (readBack is AmazonServiceException or HttpRequestException or TaskCanceledException)
                    {
                    }
                    if (retention != FullRetentionSeconds.ToString(CultureInfo.InvariantCulture))
                    {
                        throw new InvalidOperationException(
                            $"The dead-letter queue '{deadLetterName}' was created, but its retention could not be set to 14 days " +
                            $"({exception.Message}). '{definition.Name}' was not created. Set MessageRetentionPeriod of " +
                            $"'{deadLetterName}' to 1209600 (or delete it) and create '{definition.Name}' again.", exception);
                    }
                }
                attributes["RedrivePolicy"] = RedrivePolicy(deadLetterState["QueueArn"], definition.Settings.MaxDeliveryCount ?? 5);
            }

            try
            {
                await Sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = definition.Name, Attributes = attributes }, token).ConfigureAwait(false);
            }
            catch (AmazonServiceException exception) when (createdDeadLetter is not null)
            {
                // The dead-letter queue is never deleted here. CreateQueue returns an existing queue whose attributes match,
                // so another operator may have created the same name between the check above and this call, and
                // DeleteQueue removes a queue whatever it holds: ownership cannot be proven, so it stays and is named.
                throw new InvalidOperationException(
                    $"SQS did not create the queue '{definition.Name}': {exception.Message} The dead-letter queue " +
                    $"'{createdDeadLetterName}' was left in place; delete it in AWS if nothing else uses it.", exception);
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

    /// <summary>
    /// The redrive policy's maxReceiveCount, or null when it is missing or unreadable: a policy written by another tool must not
    /// make the queue's settings impossible to open.
    /// </summary>
    private static int? MaxReceiveCount(string? redrivePolicy)
    {
        if (string.IsNullOrWhiteSpace(redrivePolicy))
        {
            return null;
        }
        try
        {
            using var document = JsonDocument.Parse(redrivePolicy);
            if (document.RootElement.ValueKind != JsonValueKind.Object ||
                !document.RootElement.TryGetProperty("maxReceiveCount", out var count))
            {
                return null;
            }
            return count.ValueKind switch
            {
                JsonValueKind.Number when count.TryGetInt32(out var number) => number,
                JsonValueKind.String when int.TryParse(count.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var text) => text,
                _ => null
            };
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
