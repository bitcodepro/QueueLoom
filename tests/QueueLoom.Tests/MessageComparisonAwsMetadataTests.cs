using System.Text.Json;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;

namespace QueueLoom.Tests;

public sealed class MessageComparisonAwsMetadataTests
{
    [Theory]
    [InlineData("Invoice sent", "Invoice paid")]
    [InlineData(null, "Invoice sent")]
    [InlineData("Invoice sent", null)]
    public void NativeSnsSubjectDifferencesAreVisible(string? leftSubject, string? rightSubject)
    {
        // A native SNS Subject and a Subject message attribute are independent. The reader unwraps the
        // notification, so its retained body and ordinary Subject can match while the native subject differs.
        var left = Browse(leftSubject, "Subject", "String", "shared attribute");
        var right = Browse(rightSubject, "Subject", "String", "shared attribute");
        Assert.Equal(left.Properties.Subject, right.Properties.Subject);
        Assert.True(left.Body.Span.SequenceEqual(right.Body.Span));
        Assert.Equal(leftSubject ?? "shared attribute", AwsMessageMapper.SnsSubject(left.CreateDraft()));
        Assert.Equal(rightSubject ?? "shared attribute", AwsMessageMapper.SnsSubject(right.CreateDraft()));

        var dialog = Compare(left, right);

        Assert.False(dialog.Result.AreEqual);
        Assert.Equal(0, dialog.Result.ChangedLines);
        var difference = Assert.Single(dialog.Properties, property => property.Differs);
        Assert.Equal("Native SNS subject", difference.Name);
        Assert.Equal(leftSubject, difference.Left);
        Assert.Equal(rightSubject, difference.Right);
        Assert.DoesNotContain("are the same", dialog.Summary, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("String", "String.Array", "[\"EU\"]")]
    [InlineData("String.customer", "String.account", "tenant-42")]
    [InlineData("Number.1", "Number.2", "42")]
    [InlineData("Binary.png", "Binary.jpeg", "AP+A")]
    public void ApplicationWireTypeDifferencesAreVisible(string leftType, string rightType, string value)
    {
        var left = Browse(null, "region", leftType, value);
        var right = Browse(null, "region", rightType, value);
        var leftProperty = Assert.Single(left.ApplicationProperties);
        var rightProperty = Assert.Single(right.ApplicationProperties);
        Assert.Equal(leftProperty.Type, rightProperty.Type);
        Assert.Equal(leftProperty.Value, rightProperty.Value);
        // These drafts send different provider types even though their editable type and value match.
        Assert.Equal(leftType, AwsMessageMapper.ToSnsAttributes(left.CreateDraft())["region"].DataType);
        Assert.Equal(rightType, AwsMessageMapper.ToSnsAttributes(right.CreateDraft())["region"].DataType);

        var dialog = Compare(left, right);

        Assert.False(dialog.Result.AreEqual);
        Assert.Equal(0, dialog.Result.ChangedLines);
        var difference = Assert.Single(dialog.Properties, property => property.Differs);
        Assert.Equal("Application wire type: region", difference.Name);
        Assert.Equal(leftProperty.WireType, difference.Left);
        Assert.Equal(rightProperty.WireType, difference.Right);
        Assert.DoesNotContain("are the same", dialog.Summary, StringComparison.Ordinal);
    }

    [Fact]
    public void IdenticalNativeSubjectsAndWireTypesRemainEqual()
    {
        var left = Browse("Invoice sent", "region", "String.Array", "[\"EU\"]");
        var right = Browse("Invoice sent", "region", "String.Array", "[\"EU\"]");

        var dialog = Compare(left, right);

        Assert.True(dialog.Result.AreEqual);
        Assert.Equal(0, dialog.Result.ChangedProperties);
        Assert.Contains(dialog.Properties, property => property.Name == "Native SNS subject" && property.Left == "Invoice sent");
        Assert.Contains(dialog.Properties, property => property.Name == "Application wire type: region" && property.Left == "String.Array");
    }

    private static CompareDialogViewModel Compare(BrowsedMessage left, BrowsedMessage right) =>
        new(new MessageItemViewModel(left), new MessageItemViewModel(right));

    private static BrowsedMessage Browse(string? subject, string attributeName, string type, string value)
    {
        var envelope = new Dictionary<string, object>
        {
            ["Type"] = "Notification",
            ["MessageId"] = "notification",
            ["TopicArn"] = "arn:aws:sns:us-east-1:123456789012:orders",
            ["Message"] = "payload",
            ["MessageAttributes"] = new Dictionary<string, object> { [attributeName] = new { Type = type, Value = value } }
        };
        if (subject is not null) envelope["Subject"] = subject;
        return AwsMessageMapper.FromSqs(new Amazon.SQS.Model.Message
        {
            MessageId = "same-id", Body = JsonSerializer.Serialize(envelope)
        }, ServiceBusEntityReference.Subscription("orders", "billing"), ServiceBusSubQueue.DeadLetter, snsEnvelope: true);
    }
}
