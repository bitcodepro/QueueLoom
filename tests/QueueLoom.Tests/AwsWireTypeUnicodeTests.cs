using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Aws;

namespace QueueLoom.Tests;

public sealed class AwsWireTypeUnicodeTests
{
    private static readonly string[] InvalidLabels = ["\0", "\u0001", "\u0008", "\u000b", "\u000c", "\u000e", "\u001f",
        "\ud800", "\udbff", "\udc00", "\udfff", "\ufffe", "\uffff", "\ud800x", "x\udc00"];
    private static readonly string[] ValidLabels = ["\t", "\n", "\r", " ", "\ud7ff", "\ue000", "\ufffd",
        "\U00010000", "\U0010ffff", "label with spaces / : .. .", "Україна-😀"];

    public static IEnumerable<object[]> Labels()
    {
        // Passing malformed UTF-16 through test discovery replaces surrogates. Construct labels inside the test.
        foreach (var prefix in new[] { "String", "Number", "Binary" })
        for (var index = 0; index < InvalidLabels.Length + ValidLabels.Length; index++) yield return [prefix, index];
    }

    [Theory]
    [MemberData(nameof(Labels))]
    public void CustomDataTypesUseMessageBodyUnicodeRulesWithoutAttributeNameRestrictions(string prefix, int index)
    {
        var allowed = index >= InvalidLabels.Length;
        var label = allowed ? ValidLabels[index - InvalidLabels.Length] : InvalidLabels[index];
        var type = prefix switch { "Binary" => ApplicationPropertyType.Binary, "Number" => ApplicationPropertyType.Int64,
            _ => ApplicationPropertyType.String };
        var value = prefix switch { "Binary" => "AP+A", "Number" => "42", _ => "invoice" };
        var wireType = prefix + "." + label;
        var draft = new MessageDraft(EditableMessageBody.FromBytes("body"u8.ToArray()), applicationProperties:
            [new MessageApplicationProperty("p", type, value) { WireType = wireType }]);
        var validation = MessageDraftValidator.Validate(draft, MessagingProvider.AmazonSqsSns);
        Assert.Equal(allowed, validation.IsValid);
        if (!allowed)
        {
            Assert.Contains(validation.Errors, error => error.Code == "message.application_property.wire_type_invalid");
            return;
        }
        Assert.Equal(wireType, AwsMessageMapper.ToSqsAttributes(draft)["p"].DataType);
        Assert.Equal(wireType, AwsMessageMapper.ToSnsAttributes(draft)["p"].DataType);
    }
}
