using System.Net;
using System.Text.Json;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

public sealed class ProxySettingsTests
{
    private static ServiceBusProfile Azure(ProxySettings? proxy) =>
        ServiceBusProfile.CreateNew("Behind a proxy", EnvironmentKind.Development, new(AuthenticationKind.ConnectionString)) with { Proxy = proxy };

    [Fact]
    public void AnEnvironmentWithoutAProxySavesAsBefore()
    {
        var profile = Azure(null);

        Assert.DoesNotContain("Proxy", JsonSerializer.Serialize(profile), StringComparison.Ordinal);
        Assert.Contains("\"Proxy\"", JsonSerializer.Serialize(profile with { Proxy = new ProxySettings(ProxyMode.None) }), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(ProxyMode.System, null, null)]
    [InlineData(ProxyMode.None, null, null)]
    [InlineData(ProxyMode.Custom, "http://proxy.invalid:3128", null)]
    [InlineData(ProxyMode.Custom, "https://proxy.invalid", null)]
    [InlineData(ProxyMode.Custom, "socks5://proxy.invalid:1080", null)]
    [InlineData(ProxyMode.Custom, null, "profile.proxy.address")]
    [InlineData(ProxyMode.Custom, "proxy.invalid:3128", "profile.proxy.address")]
    [InlineData(ProxyMode.Custom, "ftp://proxy.invalid", "profile.proxy.address")]
    [InlineData(ProxyMode.Custom, "http://user:secret@proxy.invalid", "profile.proxy.address")]
    [InlineData(ProxyMode.Custom, "http://proxy.invalid/path", "profile.proxy.address")]
    [InlineData(ProxyMode.None, "http://proxy.invalid", "profile.proxy.address")]
    public void TheProxyAddressIsValidated(ProxyMode mode, string? address, string? expected)
    {
        var codes = ProfileValidator.Validate(Azure(new ProxySettings(mode, address)) with { UseAmqpWebSockets = true }).Errors.Select(error => error.Code).ToArray();

        if (expected is null) Assert.Empty(codes); else Assert.Contains(expected, codes);
    }

    [Fact]
    public void AzureSendsThroughACustomProxyOnlyOverWebSockets()
    {
        var custom = Azure(new ProxySettings(ProxyMode.Custom, "http://proxy.invalid:3128"));

        Assert.Contains("profile.proxy.websockets", ProfileValidator.Validate(custom).Errors.Select(error => error.Code));
        Assert.Empty(ProfileValidator.Validate(custom with { UseAmqpWebSockets = true }).Errors);
    }

    [Fact]
    public void AzureClientsGoThroughTheProxy()
    {
        var proxy = new WebProxy("http://proxy.invalid:3128");

        Assert.Same(proxy, QueueLoom.Infrastructure.Azure.AzureServiceBusWorkspace.CreateMessagingClientOptions(webSockets: true, proxy).WebProxy);
        Assert.IsType<global::Azure.Core.Pipeline.HttpClientTransport>(
            QueueLoom.Infrastructure.Azure.AzureServiceBusWorkspace.CreateAdministrationClientOptions(proxy).Transport);
        Assert.Null(QueueLoom.Infrastructure.Azure.AzureServiceBusWorkspace.CreateMessagingClientOptions(webSockets: true).WebProxy);
    }

    [Fact]
    public void RabbitMqManagementAndTheSchemaRegistryGoThroughTheProxy()
    {
        var proxy = new WebProxy("http://proxy.invalid:3128");

        using var management = QueueLoom.Infrastructure.RabbitMq.RabbitMqWorkspace.CreateManagementHandler(null, proxy);
        using var registry = QueueLoom.Infrastructure.Kafka.KafkaWorkspace.RegistryHandler(proxy);

        Assert.Same(proxy, management.Proxy);
        Assert.Same(proxy, registry.Proxy);
    }

    [Fact]
    public void TheEditorSavesACustomProxyAndKeepsItsPasswordInTheVault()
    {
        var editor = new QueueLoom.App.ViewModels.ProfileEditorViewModel(null) { Name = "Behind a proxy" };
        editor.SelectedAuthentication = editor.AuthenticationOptions.Single(option => option.Kind == AuthenticationKind.ConnectionString);
        editor.ConnectionString = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=synthetic";
        editor.UseAmqpWebSockets = true;

        Assert.True(editor.TryBuild(out var direct), editor.Error);
        Assert.Null(direct!.Profile.Proxy);

        editor.SelectedProxyMode = editor.ProxyModeOptions.Single(option => option.Mode == ProxyMode.Custom);
        editor.ProxyAddress = " http://proxy.invalid:3128 ";
        editor.ProxyUserName = " me ";
        editor.ProxyPassword = "synthetic";

        Assert.True(editor.TryBuild(out var result), editor.Error);
        Assert.Equal(new ProxySettings(ProxyMode.Custom, "http://proxy.invalid:3128", "me"), result!.Profile.Proxy);
        Assert.Equal("synthetic", result.ProxyPassword);
        Assert.False(result.RemovesProxyPassword);
    }

    [Fact]
    public void ClearingTheProxyUserRemovesItsStoredPassword()
    {
        var existing = Azure(new ProxySettings(ProxyMode.Custom, "http://proxy.invalid:3128", "me")) with { UseAmqpWebSockets = true };
        var editor = new QueueLoom.App.ViewModels.ProfileEditorViewModel(existing);

        Assert.Equal(ProxyMode.Custom, editor.SelectedProxyMode.Mode);
        Assert.Equal("me", editor.ProxyUserName);
        Assert.True(editor.TryBuild(out var kept), editor.Error);
        Assert.Null(kept!.ProxyPassword);
        Assert.False(kept.RemovesProxyPassword);

        editor.SelectedProxyMode = editor.ProxyModeOptions.Single(option => option.Mode == ProxyMode.System);
        Assert.True(editor.TryBuild(out var cleared), editor.Error);
        Assert.Null(cleared!.Profile.Proxy);
        Assert.True(cleared.RemovesProxyPassword);
    }

    [Fact]
    public void AUserNameNeedsACustomProxy()
    {
        var codes = ProfileValidator.Validate(Azure(new ProxySettings(ProxyMode.System, UserName: "me"))).Errors.Select(error => error.Code);

        Assert.Contains("profile.proxy.user", codes);
    }

    [Fact]
    public void TheProxyPasswordHasItsOwnVaultEntry()
    {
        var id = Guid.NewGuid();

        Assert.Equal(new ProfileSecretKey(id, ProfileSecretKind.ProxyPassword), ProfileSecretKey.ProxyPassword(id));
    }

    [Fact]
    public void TheSystemProxyIsLeftToDotNet()
    {
        Assert.Null(ProxyFactory.Create(null, null));
        Assert.Null(ProxyFactory.Create(new ProxySettings(ProxyMode.System), null));
    }

    [Fact]
    public void NoProxyConnectsDirectly()
    {
        var proxy = ProxyFactory.Create(new ProxySettings(ProxyMode.None), null)!;

        Assert.True(proxy.IsBypassed(new Uri("https://example.invalid")));
    }

    [Fact]
    public void ACustomProxyCarriesItsSignIn()
    {
        var proxy = ProxyFactory.Create(new ProxySettings(ProxyMode.Custom, " http://proxy.invalid:3128 ", " me "), "synthetic")!;
        var target = new Uri("https://example.invalid");

        Assert.Equal(new Uri("http://proxy.invalid:3128"), proxy.GetProxy(target));
        // The cloud metadata endpoint (managed identity) and this computer are always reached directly.
        Assert.True(proxy.IsBypassed(new Uri("http://169.254.169.254/metadata/identity")));
        Assert.True(proxy.IsBypassed(new Uri("http://localhost:5300")));
        var credential = proxy.Credentials!.GetCredential(new Uri("http://proxy.invalid:3128"), "Basic")!;
        Assert.Equal(("me", "synthetic"), (credential.UserName, credential.Password));

        var anonymous = ProxyFactory.Create(new ProxySettings(ProxyMode.Custom, "http://proxy.invalid:3128"), null)!;
        Assert.Null(anonymous.Credentials);
    }

    [Fact]
    public void HandlersUseTheEnvironmentsProxy()
    {
        using var system = new SocketsHttpHandler();
        ProxyFactory.Apply(system, null);
        Assert.True(system.UseProxy);
        Assert.Null(system.Proxy);

        var custom = new WebProxy("http://proxy.invalid:3128");
        using var handler = new SocketsHttpHandler();
        ProxyFactory.Apply(handler, custom);
        Assert.Same(custom, handler.Proxy);
        Assert.True(handler.UseProxy);
    }
}
