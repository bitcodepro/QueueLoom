using System.Reflection;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

// Pub/Sub refuses a message with neither data nor attributes, and attribute keys that are empty or begin with "goog".
// QueueLoom published them anyway: the refusal came back as an RPC error of unknown outcome, so a resend or replay
// recorded the item as Uncertain (never retried automatically) instead of as a proven non-delivery. Both are now refused
// before anything is sent: by the draft validator, and by the publish itself as a rejection.
public sealed class PubSubUnpublishableTests
{
    public static TheoryData<string, MessageDraft> Unpublishable => new()
    {
        { "empty", new MessageDraft(EditableMessageBody.Empty) },
        { "whitespace Base64", new MessageDraft(new EditableMessageBody(" \r\n\t", MessageBodyFormat.Base64)) },
        { "goog", Draft("googTraceId") },
        { "GOOG", Draft("GOOG-trace") },
        { "blank name", Draft("") },
    };

    [Theory]
    [MemberData(nameof(Unpublishable))]
    public void TheValidatorRefusesWhatPubSubCannotPublish(string _, MessageDraft draft)
    {
        var result = MessageDraftValidator.Validate(draft, MessagingProvider.GooglePubSub);

        Assert.False(result.IsValid);
    }

    [Theory]
    [MemberData(nameof(Unpublishable))]
    public async Task APublishOfWhatPubSubCannotAcceptIsARejectionAndSendsNothing(string _, MessageDraft draft)
    {
        var (owner, publisher) = Workspace();
        await using var __ = owner;

        await Assert.ThrowsAsync<DeliveryRejectedException>(() => Send(owner, draft));

        Assert.Equal(0, publisher.Calls);
    }

    [Theory]
    [InlineData("data only")]
    [InlineData("attribute only")]
    [InlineData("google-like but allowed")]
    [InlineData("whitespace text")]
    [InlineData("Base64 data")]
    public async Task MessagesPubSubAcceptsAreStillPublished(string shape)
    {
        var draft = shape switch
        {
            "data only" => new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text)),
            "attribute only" => Draft("tenant"),
            "whitespace text" => new MessageDraft(new EditableMessageBody(" ", MessageBodyFormat.Text)),
            "Base64 data" => new MessageDraft(new EditableMessageBody("AQID", MessageBodyFormat.Base64)),
            _ => Draft("go-trace"),
        };
        Assert.True(MessageDraftValidator.Validate(draft, MessagingProvider.GooglePubSub).IsValid);
        var (owner, publisher) = Workspace();
        await using var __ = owner;

        await Send(owner, draft);

        Assert.Equal(1, publisher.Calls);
    }

    // Malformed Base64 keeps its own error; it is not reported as an empty message.
    [Fact]
    public void MalformedBase64KeepsItsOwnErrorOnly()
    {
        var result = MessageDraftValidator.Validate(new MessageDraft(new EditableMessageBody("not base64!", MessageBodyFormat.Base64)),
            MessagingProvider.GooglePubSub);

        Assert.Contains(result.Errors, error => error.Code == "message.body.base64_invalid");
        Assert.DoesNotContain(result.Errors, error => error.Code == "message.pubsub.unpublishable");
    }

    private static MessageDraft Draft(string attribute) => new(EditableMessageBody.Empty,
        applicationProperties: [new MessageApplicationProperty(attribute, ApplicationPropertyType.String, "v")]);

    private static (GooglePubSubWorkspace Owner, CountingPublisher Publisher) Workspace()
    {
        var owner = new GooglePubSubWorkspace(new DeepAuditCloudTests.EmptyVault());
        var publisher = new CountingPublisher();
        typeof(GooglePubSubWorkspace).GetField("_publisher", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, publisher);
        typeof(GooglePubSubWorkspace).GetField("_projectId", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, "project-a");
        return (owner, publisher);
    }

    private static Task Send(GooglePubSubWorkspace owner, MessageDraft draft) =>
        (Task)typeof(GooglePubSubWorkspace).GetMethod("SendCoreAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(owner, [new ServiceBusTopology(DateTimeOffset.UtcNow), ServiceBusEntityReference.Topic("events"), draft, CancellationToken.None])!;

    private sealed class CountingPublisher : PublisherServiceApiClient
    {
        public int Calls { get; private set; }
        public override Task<PublishResponse> PublishAsync(PublishRequest request, CallSettings? callSettings = null)
        {
            Calls++;
            return Task.FromResult(new PublishResponse());
        }
    }
}
