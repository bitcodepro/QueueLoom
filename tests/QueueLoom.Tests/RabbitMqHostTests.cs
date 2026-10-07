using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;

namespace QueueLoom.Tests;

// The RabbitMQ host accepts names and IPv4 and IPv6 addresses (written plain or in brackets). The management URL was
// built by pasting the host between "http://" and ":port", which is not a URL for an IPv6 address ("http://::1:15672/"
// throws), so an environment the editor accepted could not connect. The AMQP side received the host as typed, so a
// bracketed address was looked up as a name and spaces around a name were kept.
public sealed class RabbitMqHostTests
{
    [Theory]
    [InlineData("rabbit.internal", "rabbit.internal", "http://rabbit.internal:15672/")]
    [InlineData("  rabbit.internal  ", "rabbit.internal", "http://rabbit.internal:15672/")]
    [InlineData("10.0.0.5", "10.0.0.5", "http://10.0.0.5:15672/")]
    [InlineData("::1", "::1", "http://[::1]:15672/")]
    [InlineData("[::1]", "::1", "http://[::1]:15672/")]
    [InlineData("fe80::1", "fe80::1", "http://[fe80::1]:15672/")]
    [InlineData(" [2001:db8::5] ", "2001:db8::5", "http://[2001:db8::5]:15672/")]
    public void TheHostIsNormalizedForAmqpAndBracketedInTheManagementUrl(string host, string amqpHost, string managementUrl)
    {
        var settings = new RabbitMqSettings(host, "guest");

        Assert.Equal(amqpHost, settings.HostName);
        Assert.Equal(new Uri(managementUrl), settings.ManagementUri);
    }

    [Fact]
    public void TlsUsesHttpsAndTheConfiguredManagementPort()
    {
        var settings = new RabbitMqSettings("::1", "guest", ManagementPort: 15671, UseTls: true);

        Assert.Equal(new Uri("https://[::1]:15671/"), settings.ManagementUri);
    }

    // Whatever host the environment editor accepts must give a working management URL.
    [Theory]
    [InlineData("rabbit.internal")]
    [InlineData("localhost")]
    [InlineData("10.0.0.5")]
    [InlineData("::1")]
    [InlineData("[::1]")]
    [InlineData("fe80::1")]
    [InlineData("2001:db8::5")]
    public void EveryAcceptedHostGivesAManagementUrl(string host)
    {
        var profile = ServiceBusProfile.CreateNew("rabbit", EnvironmentKind.Development, new(AuthenticationKind.RabbitMqPassword)) with
        {
            Provider = MessagingProvider.RabbitMq,
            RabbitMq = new RabbitMqSettings(host, "guest")
        };
        Assert.DoesNotContain(ProfileValidator.Validate(profile).Errors, error => error.Code.StartsWith("profile.rabbitmq.host", StringComparison.Ordinal));

        var url = profile.RabbitMq!.ManagementUri;

        Assert.Equal(15672, url.Port);
        Assert.Equal(profile.RabbitMq.HostName, url.IdnHost.Trim('[', ']'));
    }
}
