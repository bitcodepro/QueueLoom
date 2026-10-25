using RabbitMQ.Client;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

/// <summary>
/// An AMQP 0-9-1 decimal is a scale octet and a sign plus 31-bit magnitude. RabbitMQ.Client 7 throws "Decimal overflow
/// in AMQP encoding" for a value that needs more than the low 32 bits of its unscaled form (0.12345678901 fits .NET but not
/// AMQP), and silently writes a wrong value when the low word has its top bit set (2147483648 goes out as 0,
/// 3000000000 as 852516352). Such a Decimal is refused before sending instead of failing or corrupting mid-resend.
/// </summary>
public sealed class RabbitDecimalHeaderTests
{
    private static MessageDraft Draft(string value) =>
        new(EditableMessageBody.Empty, EditableMessageProperties.Empty, [new MessageApplicationProperty("amount", ApplicationPropertyType.Decimal, value)]);

    [Theory]
    [InlineData("3000000000")]
    [InlineData("2147483648")]
    [InlineData("0.12345678901")]
    [InlineData("-2147483648")]
    public void TheValidatorRefusesADecimalRabbitMqCannotCarryExactly(string value)
    {
        var result = MessageDraftValidator.Validate(Draft(value), MessagingProvider.RabbitMq);

        var error = Assert.Single(result.Errors, error => error.Code == "message.application_property.amqp_decimal_overflow");
        Assert.Contains("'amount'", error.Message, StringComparison.Ordinal);
        Assert.True(MessageDraftValidator.Validate(Draft(value), MessagingProvider.AzureServiceBus).IsValid);
        Assert.Throws<InvalidOperationException>(() => RabbitMqMessageMapper.ToAmqp(Draft(value)));
    }

    [Theory]
    [InlineData("123.45", "123.45")]
    [InlineData("2147483647", "2147483647")]
    [InlineData("-2147483647", "-2147483647")]
    [InlineData("0.2147483647", "0.2147483647")]
    [InlineData("1.500000000000", "1.5")]
    public void ADecimalThatFitsIsSentAndReadBackUnchanged(string value, string expected)
    {
        Assert.True(MessageDraftValidator.Validate(Draft(value), MessagingProvider.RabbitMq).IsValid);

        var properties = RabbitMqMessageMapper.ToAmqp(Draft(value));
        // The client's own frame writer accepts it.
        var writeable = (IAmqpWriteable)properties;
        writeable.WriteTo(new byte[writeable.GetRequiredBufferSize()]);
        var read = RabbitMqMessageMapper.FromAmqp(Array.Empty<byte>(), properties, "orders", ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.Active);

        var amount = Assert.Single(read.ApplicationProperties, property => property.Name == "amount");
        Assert.Equal(ApplicationPropertyType.Decimal, amount.Type);
        Assert.Equal(expected, amount.Value);
    }
}
