using System.Text;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests;

/// <summary>Cycle 8: data that changes meaning on a round trip through a backup or onto another broker.</summary>
public sealed class DataRoundTripTests
{
    // ---- Finding 1: Azure backups restore the SDK's "no TTL" and "not scheduled" defaults as real values -------------

    [Fact]
    public async Task AzureBackup_OfAnUnscheduledMessageWithoutTtl_RestoresBothAsAbsentLikeALiveBrowse()
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = QueueLoomPaths.ForRoot(root);
            var profile = new ServiceBusProfile(Guid.NewGuid(), "Development", EnvironmentKind.Development, null,
                "development.servicebus.windows.net", AuthenticationSettings.Entra(), ProfileAccessMode.ReadWrite);
            var session = await new DeadLetterJsonBackupStore(paths)
                .CreateSessionAsync(profile, DateTimeOffset.Parse("2026-08-12T10:20:30Z"), CancellationToken.None);
            // What Service Bus returns for a message sent without TTL or schedule: TimeSpan.MaxValue and default.
            var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: new BinaryData(Encoding.UTF8.GetBytes("{}")),
                messageId: "m-1",
                timeToLive: TimeSpan.MaxValue,
                sequenceNumber: 5,
                enqueuedTime: DateTimeOffset.Parse("2026-08-12T10:00:00Z"));
            Assert.Equal(TimeSpan.MaxValue, message.TimeToLive);
            Assert.Equal(default, message.ScheduledEnqueueTime);

            await session.BackupAsync(message, ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);
            var repository = new JsonDeadLetterBackupRepository(paths);
            var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));

            Assert.Null(restored.Properties.TimeToLive);
            Assert.Null(restored.Properties.ScheduledEnqueueTime);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task AzureBackup_KeepsARealTtlAndSchedule()
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = QueueLoomPaths.ForRoot(root);
            var profile = new ServiceBusProfile(Guid.NewGuid(), "Development", EnvironmentKind.Development, null,
                "development.servicebus.windows.net", AuthenticationSettings.Entra(), ProfileAccessMode.ReadWrite);
            var session = await new DeadLetterJsonBackupStore(paths)
                .CreateSessionAsync(profile, DateTimeOffset.Parse("2026-08-12T10:20:30Z"), CancellationToken.None);
            var scheduled = DateTimeOffset.Parse("2026-08-13T08:00:00Z");
            var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: new BinaryData(Encoding.UTF8.GetBytes("{}")), messageId: "m-2", timeToLive: TimeSpan.FromHours(2),
                scheduledEnqueueTime: scheduled, sequenceNumber: 6, enqueuedTime: DateTimeOffset.Parse("2026-08-12T10:00:00Z"));

            await session.BackupAsync(message, ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter,
                CancellationToken.None);
            var repository = new JsonDeadLetterBackupRepository(paths);
            var restored = await repository.LoadAsync(Assert.Single(await repository.ListAsync()));

            Assert.Equal(TimeSpan.FromHours(2), restored.Properties.TimeToLive);
            Assert.Equal(scheduled, restored.Properties.ScheduledEnqueueTime);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    // ---- Finding 2: numeric properties SQS/SNS refuse as Number attributes pass validation ---------------------------

    [Theory]
    [InlineData(ApplicationPropertyType.Decimal, "1,000")]
    [InlineData(ApplicationPropertyType.Decimal, "100-")]
    [InlineData(ApplicationPropertyType.Double, "NaN")]
    [InlineData(ApplicationPropertyType.Double, "Infinity")]
    [InlineData(ApplicationPropertyType.Single, "-Infinity")]
    public void SqsSnsDraft_NumericValueTheServiceRefusesAsANumberAttribute_IsRefused(ApplicationPropertyType type, string value)
    {
        var draft = new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text), EditableMessageProperties.Empty,
            [new MessageApplicationProperty("amount", type, value)]);

        Assert.True(MessageDraftValidator.Validate(draft, MessagingProvider.AzureServiceBus).IsValid);
        var result = MessageDraftValidator.Validate(draft, MessagingProvider.AmazonSqsSns);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, error => error.Code == "message.application_property.aws_number_invalid");
    }

    [Theory]
    [InlineData(ApplicationPropertyType.Decimal, "1000.25")]
    [InlineData(ApplicationPropertyType.Double, "-2.5E+30")]
    [InlineData(ApplicationPropertyType.Int64, "-42")]
    [InlineData(ApplicationPropertyType.UInt64, "18446744073709551615")]
    public void SqsSnsDraft_PlainNumbers_StayValid(ApplicationPropertyType type, string value)
    {
        var draft = new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text), EditableMessageProperties.Empty,
            [new MessageApplicationProperty("amount", type, value)]);

        Assert.True(MessageDraftValidator.Validate(draft, MessagingProvider.AmazonSqsSns).IsValid);
    }

    // ---- Finding 3: a TTL RabbitMQ refuses (above 2^32-1 ms) passes validation -----------------------------------------

    [Fact]
    public void RabbitMqDraft_TtlAboveTheBrokerMaximum_IsRefused()
    {
        var tooLong = new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text),
            EditableMessageProperties.Empty with { TimeToLive = TimeSpan.FromDays(90) });
        var longest = new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text),
            EditableMessageProperties.Empty with { TimeToLive = TimeSpan.FromMilliseconds(uint.MaxValue) });

        Assert.True(MessageDraftValidator.Validate(tooLong, MessagingProvider.AzureServiceBus).IsValid);
        var result = MessageDraftValidator.Validate(tooLong, MessagingProvider.RabbitMq);
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, error => error.Code == "message.ttl.too_long");
        Assert.True(MessageDraftValidator.Validate(longest, MessagingProvider.RabbitMq).IsValid);
    }
}
