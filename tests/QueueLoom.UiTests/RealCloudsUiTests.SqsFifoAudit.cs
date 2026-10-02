using System.Reflection;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.App.Services;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.UiTests;

public sealed partial class RealCloudsUiTests
{
    [Fact]
    public Task SqsFifoComposerCopyThenEditedMoveRequiresANewIdentity() => UiSession.RunAsync(async () =>
    {
        var endpoint = Environment.GetEnvironmentVariable("QUEUELOOM_LOCALSTACK_URL");
        if (string.IsNullOrWhiteSpace(endpoint)) return;
        var root = Path.Combine(Path.GetTempPath(), "queueloom-fifo-audit", Guid.NewGuid().ToString("N"));
        using var sqs = new AmazonSQSClient(new BasicAWSCredentials("test", "test"),
            new AmazonSQSConfig { ServiceURL = endpoint, AuthenticationRegion = "eu-west-1" });
        var prefix = "composer-audit-" + Guid.NewGuid().ToString("N");
        var dlqUrl = (await sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = prefix + "-dlq.fifo",
            Attributes = new() { ["FifoQueue"] = "true" } })).QueueUrl;
        var dlqArn = (await sqs.GetQueueAttributesAsync(dlqUrl, ["QueueArn"])).Attributes["QueueArn"];
        var source = ServiceBusEntityReference.Queue(prefix + ".fifo");
        var queueUrl = (await sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = source.Name,
            Attributes = new() { ["FifoQueue"] = "true", ["RedrivePolicy"] = JsonSerializer.Serialize(new { deadLetterTargetArn = dlqArn, maxReceiveCount = 1 }) } })).QueueUrl;
        try
        {
            var profile = ServiceBusProfile.CreateNew("SQS FIFO emulator", EnvironmentKind.Development,
                new AuthenticationSettings(AuthenticationKind.AwsAccessKey), accessMode: ProfileAccessMode.ReadWrite)
                with { Provider = MessagingProvider.AmazonSqsSns, Aws = new AwsSettings("eu-west-1", endpoint) };
            var vault = new InMemorySecretVault();
            await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), new AwsAccessKey("test", "test").ToSecret());
            await using var workspace = new AwsSqsSnsWorkspace(vault, backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(root)));
            await workspace.ConnectAsync(profile);
            await workspace.SendMessageAsync(new QueueLoom.Core.ServiceBus.SendMessageRequest(source, new MessageDraft(new EditableMessageBody("original", MessageBodyFormat.Text),
                new EditableMessageProperties(MessageId: Guid.NewGuid().ToString("N"), SessionId: "group"))));
            // Actual redrive, rather than assigning a source ARN to a manually seeded DLQ message.
            for(var i = 0; i < 5; i++)
            {
                var delivery = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest { QueueUrl = queueUrl, VisibilityTimeout = 0, WaitTimeSeconds = 1 });
                if(delivery.Messages is null or { Count: 0 }) break;
            }
            var dead = Assert.Single(await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter)));
            var dialogs = DispatchProxy.Create<IUserDialogService, ComposerAuditDialogs>();
            var confirmations = ((ComposerAuditDialogs)dialogs).Confirmations;
            await using var vm = new MainWindowViewModel(new InMemoryProfileRepository(profile), vault, workspace, dialogs);
            await vm.InitializeAsync(); await vm.ConnectCommand.ExecuteAsync();
            vm.SelectedMessage = new MessageItemViewModel(dead, profile.Id, profile.Name);
            vm.OpenMessageAsDraftCommand.Execute(null);
            vm.GenerateMessageIdCommand.Execute(null);
            var copyId = vm.DraftMessageId;
            await vm.SendDraftCommand.ExecuteAsync();
            Assert.Empty(vm.ErrorText);
            var copy = Assert.Single((await Receive()).Messages);
            Assert.Equal("original", copy.Body);
            await sqs.DeleteMessageAsync(queueUrl, copy.ReceiptHandle);
            await sqs.SendMessageAsync(new Amazon.SQS.Model.SendMessageRequest { QueueUrl = queueUrl, MessageBody = "suppressed duplicate",
                MessageGroupId = "group", MessageDeduplicationId = copyId });
            Assert.Empty((await Receive()).Messages ?? []);
            vm.DraftBody = "edited replacement"; vm.DraftMovesOriginal = true;
            await vm.SendDraftCommand.ExecuteAsync();
            Assert.Contains("already attempted", vm.ErrorText, StringComparison.Ordinal);
            Assert.Single(confirmations);
            Assert.Single(await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter)));
            vm.GenerateMessageIdCommand.Execute(null);
            var moveId = vm.DraftMessageId;
            await vm.SendDraftCommand.ExecuteAsync();
            Assert.Empty(vm.ErrorText);
            Assert.Contains($"MessageId: {moveId}", confirmations.Last(), StringComparison.Ordinal);
            var replacement = Assert.Single((await Receive()).Messages);
            Assert.Equal("edited replacement", replacement.Body);
            Assert.Equal(moveId, replacement.Attributes["MessageDeduplicationId"]);
            await sqs.DeleteMessageAsync(queueUrl, replacement.ReceiptHandle);
            Assert.Empty(await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter)));
            Assert.Empty((await Receive()).Messages ?? []);
            Task<ReceiveMessageResponse> Receive() => sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                { QueueUrl = queueUrl, WaitTimeSeconds = 1, MessageSystemAttributeNames = ["All"] });
        }
        finally { await sqs.DeleteQueueAsync(queueUrl); await sqs.DeleteQueueAsync(dlqUrl); }
    });
}
