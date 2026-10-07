using Amazon.Runtime;
using Avalonia.Headless;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.UiTests;

/// <summary>
/// Drives the real window against LocalStack (SQS / SNS) and the Pub/Sub emulator, with the same workspace the
/// app uses. Runs only when QUEUELOOM_LOCALSTACK_URL and QUEUELOOM_PUBSUB_EMULATOR are set; writes screenshots
/// when QUEUELOOM_SCREENSHOT_DIR is set too.
/// </summary>
public sealed partial class RealCloudsUiTests
{
    private const string AwsRegion = "eu-central-1";
    private const string GoogleProject = "shipping-dev-2231";

    private static readonly string LocalStackUrl = Environment.GetEnvironmentVariable("QUEUELOOM_LOCALSTACK_URL") ?? string.Empty;
    private static readonly string PubSubHost = Environment.GetEnvironmentVariable("QUEUELOOM_PUBSUB_EMULATOR") ?? string.Empty;
    private static readonly string? ScreenshotDirectory = Environment.GetEnvironmentVariable("QUEUELOOM_SCREENSHOT_DIR");

    [EmulatorFact([Emulators.LocalStack, Emulators.PubSub])]
    public Task The_app_explores_searches_and_reads_dead_letters_in_aws_and_google_cloud() => UiSession.RunAsync(async () =>
    {
        await SeedAwsAsync();
        await SeedPubSubAsync();

        var root = Path.Combine(Path.GetTempPath(), "queueloom-ui-clouds", Guid.NewGuid().ToString("N"));
        var vault = new InMemorySecretVault();
        var aws = DemoData.AwsStaging with { Aws = new AwsSettings(AwsRegion, LocalStackUrl) };
        var google = DemoData.GoogleDevelopment with { GooglePubSub = new GooglePubSubSettings(GoogleProject, PubSubHost) };
        await vault.StoreAsync(ProfileSecretKey.ConnectionString(aws.Id), new AwsAccessKey("test", "test").ToSecret());
        await using var workspace = MessagingWorkspaces.Create(vault, QueueLoomPaths.ForRoot(root));
        await using var fixture = await WindowFixture.OpenWithAsync(workspace, vault, DemoData.Production, aws, google);
        fixture.ViewModel.ThemePreference = AppThemePreference.Dark;

        // Amazon SQS / SNS
        await ConnectAsync(fixture, aws.Id);
        Assert.Equal(MessagingProvider.AmazonSqsSns, fixture.ViewModel.ConnectedProvider);
        await fixture.NavigateAsync("Explorer");
        var orders = fixture.ViewModel.Entities.Single(entity => entity.Name == "orders");
        Assert.Equal(3, orders.DeadLetters);
        Assert.Contains(fixture.ViewModel.Entities, entity => entity.Name == "sqs:crm-sync" && entity.Active == 2);
        Assert.Contains("Dead-letter queue of orders", fixture.ViewModel.Entities.Single(entity => entity.Name == "orders-dlq").Detail);
        Save(fixture, "aws-explorer.png");

        await fixture.NavigateAsync("DeadLetters");
        fixture.ViewModel.SelectedDlqSource = fixture.ViewModel.FilteredDeadLetterSources.Single(source => source.EntityName == "orders");
        await fixture.ViewModel.BrowseDlqSourceCommand.ExecuteAsync();
        await fixture.SettleAsync();
        Assert.Equal(3, fixture.ViewModel.Messages.Count);
        fixture.ViewModel.SelectedMessage = fixture.ViewModel.Messages.First(message => message.Message.Properties.MessageId is not null);
        await fixture.SettleAsync();
        Save(fixture, "aws-dead-letters.png");

        fixture.ViewModel.DeadLetterSearchQuery = "1042";
        fixture.ViewModel.SelectedDeadLetterEnvironmentFilter = fixture.ViewModel.DeadLetterEnvironmentFilters
            .Single(filter => filter.ProfileId == aws.Id);
        await fixture.ViewModel.SearchDeadLettersCommand.ExecuteAsync();
        await fixture.SettleAsync();
        var match = Assert.Single(fixture.ViewModel.Messages);
        Assert.Contains("1042", match.Message.CreateDraft().Body.Content);

        // Google Cloud Pub/Sub
        await ConnectAsync(fixture, google.Id);
        Assert.Equal(MessagingProvider.GooglePubSub, fixture.ViewModel.ConnectedProvider);
        await fixture.NavigateAsync("Explorer");
        var billing = fixture.ViewModel.Entities.Single(entity => entity.Name == "billing");
        Assert.Equal("—", billing.ActiveDisplay);
        Assert.Contains("Dead letters go to events-dlq", billing.Detail);
        Save(fixture, "gcp-explorer.png");

        await fixture.NavigateAsync("DeadLetters");
        var source = fixture.ViewModel.FilteredDeadLetterSources.Single(item => item.EntityName == "billing");
        Assert.Equal(2, source.Count);
        fixture.ViewModel.SelectedDlqSource = source;
        await fixture.ViewModel.BrowseDlqSourceCommand.ExecuteAsync();
        fixture.ViewModel.SelectedMessage = fixture.ViewModel.Messages.FirstOrDefault();
        await fixture.SettleAsync();
        Assert.Equal(2, fixture.ViewModel.Messages.Count);
        Save(fixture, "gcp-dead-letters.png");

        try
        {
            Directory.Delete(root, recursive: true);
        }
        catch (DirectoryNotFoundException)
        {
        }
    });

    private static async Task ConnectAsync(WindowFixture fixture, Guid profileId)
    {
        fixture.ViewModel.SelectedProfile = fixture.ViewModel.Profiles.Single(profile => profile.Id == profileId);
        await fixture.ViewModel.ConnectCommand.ExecuteAsync();
        await fixture.ViewModel.ScanCurrentEnvironmentCommand.ExecuteAsync();
        await fixture.SettleAsync();
        Assert.True(fixture.ViewModel.IsConnected, fixture.ViewModel.StatusText);
    }

    private static void Save(WindowFixture fixture, string name)
    {
        fixture.Window.UpdateLayout();
        using var frame = fixture.Window.CaptureRenderedFrame();
        if (string.IsNullOrWhiteSpace(ScreenshotDirectory))
        {
            return;
        }

        Directory.CreateDirectory(ScreenshotDirectory);
        using var file = File.Create(Path.Combine(ScreenshotDirectory, name));
        frame?.Save(file, new Avalonia.Media.Imaging.PngBitmapEncoderOptions());
    }

    /// <summary>A fresh region with orders (3 dead letters), payments and an SNS topic that feeds crm-sync.</summary>
    private static async Task SeedAwsAsync()
    {
        var credentials = new BasicAWSCredentials("test", "test");
        using var sqs = new AmazonSQSClient(credentials, new AmazonSQSConfig { ServiceURL = LocalStackUrl, AuthenticationRegion = AwsRegion });
        using var sns = new AmazonSimpleNotificationServiceClient(credentials,
            new AmazonSimpleNotificationServiceConfig { ServiceURL = LocalStackUrl, AuthenticationRegion = AwsRegion });

        foreach (var url in (await sqs.ListQueuesAsync(new ListQueuesRequest())).QueueUrls ?? [])
        {
            await sqs.DeleteQueueAsync(url);
        }
        foreach (var topic in (await sns.ListTopicsAsync(new Sns.ListTopicsRequest())).Topics ?? [])
        {
            await sns.DeleteTopicAsync(topic.TopicArn);
        }

        var deadLetterArn = await CreateQueueAsync(sqs, "orders-dlq");
        await CreateQueueAsync(sqs, "orders", deadLetterArn);
        await CreateQueueAsync(sqs, "payments");
        var crmArn = await CreateQueueAsync(sqs, "crm-sync");

        var ordersUrl = (await sqs.GetQueueUrlAsync("orders")).QueueUrl;
        foreach (var (id, body) in new[]
                 {
                     ("order-1042", """{"orderId":1042,"total":129.95,"error":"card declined"}"""),
                     ("order-1043", """{"orderId":1043,"total":18.50,"error":"currency missing"}"""),
                     ("order-1044", """{"orderId":1044,"total":64.00,"error":"timeout"}""")
                 })
        {
            await sqs.SendMessageAsync(new Amazon.SQS.Model.SendMessageRequest
            {
                QueueUrl = ordersUrl,
                MessageBody = body,
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    ["CorrelationId"] = new() { DataType = "String", StringValue = id },
                    ["Subject"] = new() { DataType = "String", StringValue = "OrderPlaced" }
                }
            });
        }
        // Receiving past maxReceiveCount=1 moves every message to orders-dlq.
        for (var attempt = 0; attempt < 20; attempt++)
        {
            var received = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = ordersUrl, MaxNumberOfMessages = 10, VisibilityTimeout = 0, WaitTimeSeconds = 1
            });
            if (received.Messages is null or { Count: 0 })
            {
                break;
            }
        }

        var paymentsUrl = (await sqs.GetQueueUrlAsync("payments")).QueueUrl;
        for (var index = 1; index <= 5; index++)
        {
            await sqs.SendMessageAsync(paymentsUrl, $$"""{"paymentId":{{index}}}""");
        }

        var topicArn = (await sns.CreateTopicAsync("customer-events")).TopicArn;
        var subscriptionArn = (await sns.SubscribeAsync(new Sns.SubscribeRequest
        {
            TopicArn = topicArn, Protocol = "sqs", Endpoint = crmArn, ReturnSubscriptionArn = true
        })).SubscriptionArn;
        await sns.SetSubscriptionAttributesAsync(new Sns.SetSubscriptionAttributesRequest
        {
            SubscriptionArn = subscriptionArn, AttributeName = "RawMessageDelivery", AttributeValue = "true"
        });
        await sns.PublishAsync(topicArn, """{"event":"CustomerUpdated","id":"C-77"}""");
        await sns.PublishAsync(topicArn, """{"event":"CustomerCreated","id":"C-78"}""");
    }

    private static async Task<string> CreateQueueAsync(AmazonSQSClient sqs, string name, string? deadLetterArn = null)
    {
        var request = new CreateQueueRequest { QueueName = name, Attributes = [] };
        if (deadLetterArn is not null)
        {
            request.Attributes["RedrivePolicy"] = $$"""{"deadLetterTargetArn":"{{deadLetterArn}}","maxReceiveCount":"1"}""";
        }
        var url = (await sqs.CreateQueueAsync(request)).QueueUrl;
        return (await sqs.GetQueueAttributesAsync(url, ["QueueArn"])).Attributes["QueueArn"];
    }

    /// <summary>events with billing and shipping (dead letters to events-dlq), read through dlq-reader.</summary>
    private static async Task SeedPubSubAsync()
    {
        var publisher = await new PublisherServiceApiClientBuilder { Endpoint = PubSubHost, ChannelCredentials = ChannelCredentials.Insecure }.BuildAsync();
        var subscriber = await new SubscriberServiceApiClientBuilder { Endpoint = PubSubHost, ChannelCredentials = ChannelCredentials.Insecure }.BuildAsync();
        var project = new Google.Api.Gax.ResourceNames.ProjectName(GoogleProject);
        await foreach (var subscription in subscriber.ListSubscriptionsAsync(project))
        {
            await subscriber.DeleteSubscriptionAsync(subscription.SubscriptionName);
        }
        await foreach (var topic in publisher.ListTopicsAsync(project))
        {
            await publisher.DeleteTopicAsync(topic.TopicName);
        }

        var events = new TopicName(GoogleProject, "events");
        var deadLetters = new TopicName(GoogleProject, "events-dlq");
        await publisher.CreateTopicAsync(events);
        await publisher.CreateTopicAsync(deadLetters);
        foreach (var name in new[] { "billing", "shipping" })
        {
            await subscriber.CreateSubscriptionAsync(new Subscription
            {
                SubscriptionName = new SubscriptionName(GoogleProject, name),
                TopicAsTopicName = events,
                DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = deadLetters.ToString(), MaxDeliveryAttempts = 5 }
            });
        }
        await subscriber.CreateSubscriptionAsync(new SubscriptionName(GoogleProject, "dlq-reader"), deadLetters, null, 10);
        await publisher.PublishAsync(events, [new PubsubMessage { Data = ByteString.CopyFromUtf8("""{"parcel":"P-1"}""") }]);

        foreach (var (source, body) in new[]
                 {
                     ("billing", """{"invoice":7,"error":"tax id missing"}"""),
                     ("billing", """{"invoice":8,"error":"amount mismatch"}"""),
                     ("shipping", """{"parcel":"P-9","error":"address not found"}""")
                 })
        {
            var message = new PubsubMessage { Data = ByteString.CopyFromUtf8(body) };
            message.Attributes["CloudPubSubDeadLetterSourceSubscription"] = source;
            message.Attributes["CloudPubSubDeadLetterSourceSubscriptionProject"] = deadLetters.ProjectId;
            message.Attributes["CloudPubSubDeadLetterSourceDeliveryCount"] = "5";
            await publisher.PublishAsync(deadLetters, [message]);
        }
    }
}
