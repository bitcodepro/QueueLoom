using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Confluent.Kafka;
using QueueLoom.App.Services;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class ClientTlsTests
{
    [Fact]
    public void APkcs12ClientCertificateIsLoadedWithItsPassword()
    {
        using var directory = new TemporaryDirectory();
        using var certificate = Issue("client");
        var path = Path.Combine(directory.Path, "client.pfx");
        File.WriteAllBytes(path, certificate.Export(X509ContentType.Pkcs12, "synthetic"));

        using var tls = ClientTls.Load(new TlsClientSettings(ClientCertificatePath: path), "synthetic");

        Assert.True(tls.ClientCertificate!.HasPrivateKey);
        Assert.Equal(certificate.Thumbprint, tls.ClientCertificate.Thumbprint);
        Assert.Null(tls.Authorities);
    }

    [Fact]
    public void AWrongPasswordIsReportedWithTheFileName()
    {
        using var directory = new TemporaryDirectory();
        using var certificate = Issue("client");
        var path = Path.Combine(directory.Path, "client.p12");
        File.WriteAllBytes(path, certificate.Export(X509ContentType.Pkcs12, "synthetic"));

        var error = Assert.Throws<InvalidOperationException>(() => ClientTls.Load(new TlsClientSettings(ClientCertificatePath: path), "wrong"));

        Assert.Contains(path, error.Message, StringComparison.Ordinal);
        Assert.Contains("password", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void APemCertificateIsLoadedWithItsKeyInTheSameOrASeparateFile(bool separateKey)
    {
        using var directory = new TemporaryDirectory();
        using var certificate = Issue("client");
        using var key = certificate.GetRSAPrivateKey()!;
        var certificatePath = Path.Combine(directory.Path, "client.pem");
        var keyPath = Path.Combine(directory.Path, "client.key");
        var keyPem = key.ExportPkcs8PrivateKeyPem();
        File.WriteAllText(certificatePath, certificate.ExportCertificatePem() + (separateKey ? string.Empty : "\n" + keyPem));
        if (separateKey)
        {
            File.WriteAllText(keyPath, keyPem);
        }

        using var tls = ClientTls.Load(new TlsClientSettings(ClientCertificatePath: certificatePath, ClientKeyPath: separateKey ? keyPath : null), null);

        Assert.True(tls.ClientCertificate!.HasPrivateKey);
        Assert.Equal(certificate.Thumbprint, tls.ClientCertificate.Thumbprint);
    }

    [Fact]
    public void ACertificateWithoutItsKeyIsRefused()
    {
        using var directory = new TemporaryDirectory();
        using var certificate = Issue("client");
        var path = Path.Combine(directory.Path, "client.pem");
        File.WriteAllText(path, certificate.ExportCertificatePem());

        var error = Assert.Throws<InvalidOperationException>(() => ClientTls.Load(new TlsClientSettings(ClientCertificatePath: path), null));

        Assert.Contains(path, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AMissingFileIsNamed()
    {
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"), "ca.pem");

        var error = Assert.Throws<InvalidOperationException>(() => ClientTls.Load(new TlsClientSettings(CaCertificatePath: path), null));

        Assert.Contains("certificate authority", error.Message, StringComparison.Ordinal);
        Assert.Contains(path, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AServerCertificateIsAcceptedWhenItChainsToThePrivateAuthorityOnly()
    {
        using var directory = new TemporaryDirectory();
        using var authority = Issue("Synthetic CA", authority: true);
        using var other = Issue("Other CA", authority: true);
        using var server = Issue("broker.invalid", issuer: authority);
        using var stranger = Issue("broker.invalid", issuer: other);
        var path = Path.Combine(directory.Path, "ca.pem");
        File.WriteAllText(path, authority.ExportCertificatePem());

        using var tls = ClientTls.Load(new TlsClientSettings(CaCertificatePath: path), null);

        Assert.Single(tls.Authorities!);
        Assert.True(tls.ValidateServerCertificate(server, SslPolicyErrors.RemoteCertificateChainErrors));
        Assert.False(tls.ValidateServerCertificate(stranger, SslPolicyErrors.RemoteCertificateChainErrors));
        // The host name is still checked: a private authority does not excuse another host's certificate.
        Assert.False(tls.ValidateServerCertificate(server, SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch));
        Assert.True(tls.ValidateServerCertificate(stranger, SslPolicyErrors.None));
    }

    [Fact]
    public void WithoutAPrivateAuthorityOnlyTheSystemVerdictCounts()
    {
        using var directory = new TemporaryDirectory();
        using var certificate = Issue("client");
        var path = Path.Combine(directory.Path, "client.pfx");
        File.WriteAllBytes(path, certificate.Export(X509ContentType.Pkcs12));
        using var tls = ClientTls.Load(new TlsClientSettings(ClientCertificatePath: path), null);

        Assert.False(tls.ValidateServerCertificate(certificate, SslPolicyErrors.RemoteCertificateChainErrors));
        Assert.True(tls.ValidateServerCertificate(certificate, SslPolicyErrors.None));
    }

    [Fact]
    public void KafkaReadsAPkcs12KeystoreOrPemFilesAndThePrivateAuthority()
    {
        var pkcs12 = new ClientConfig();
        KafkaWorkspace.ApplyTls(pkcs12, new TlsClientSettings("/certs/ca.pem", "/certs/client.p12"), "synthetic");
        Assert.Equal("/certs/ca.pem", pkcs12.SslCaLocation);
        Assert.Equal("/certs/client.p12", pkcs12.SslKeystoreLocation);
        Assert.Equal("synthetic", pkcs12.SslKeystorePassword);
        Assert.Null(pkcs12.SslCertificateLocation);

        var pem = new ClientConfig();
        KafkaWorkspace.ApplyTls(pem, new TlsClientSettings(ClientCertificatePath: "/certs/client.pem", ClientKeyPath: "/certs/client.key"), null);
        Assert.Null(pem.SslCaLocation);
        Assert.Equal("/certs/client.pem", pem.SslCertificateLocation);
        Assert.Equal("/certs/client.key", pem.SslKeyLocation);
        Assert.Null(pem.SslKeyPassword);
        Assert.Null(pem.SslKeystoreLocation);

        var combined = new ClientConfig();
        KafkaWorkspace.ApplyTls(combined, new TlsClientSettings(ClientCertificatePath: "/certs/client.pem"), "synthetic");
        Assert.Equal("/certs/client.pem", combined.SslKeyLocation);
        Assert.Equal("synthetic", combined.SslKeyPassword);
    }

    [Fact]
    public void RabbitMqPresentsTheClientCertificateAndChecksThePrivateAuthority()
    {
        using var directory = new TemporaryDirectory();
        using var authority = Issue("Synthetic CA", authority: true);
        using var client = Issue("client");
        var caPath = Path.Combine(directory.Path, "ca.pem");
        var clientPath = Path.Combine(directory.Path, "client.pfx");
        File.WriteAllText(caPath, authority.ExportCertificatePem());
        File.WriteAllBytes(clientPath, client.Export(X509ContentType.Pkcs12));
        var settings = new RabbitMqSettings("rabbit.invalid", "user", UseTls: true, Tls: new TlsClientSettings(caPath, clientPath));
        using var tls = ClientTls.Load(settings.Tls!, null);

        var option = RabbitMqWorkspace.CreateSslOption(settings, tls);

        Assert.True(option.Enabled);
        Assert.Equal("rabbit.invalid", option.ServerName);
        Assert.Equal(client.Thumbprint, Assert.Single(option.Certs!.Cast<X509Certificate>()).GetCertHashString());
        Assert.NotNull(option.CertificateValidationCallback);

        var plain = RabbitMqWorkspace.CreateSslOption(settings with { Tls = null }, null);
        Assert.Null(plain.Certs);
        Assert.Null(plain.CertificateValidationCallback);
    }

    [Theory]
    [InlineData(false, "/certs/ca.pem", null, null, "profile.kafka.tls.off")]
    [InlineData(true, "/certs/ca.pem", "/certs/client.pfx", "/certs/client.key", "profile.kafka.tls.key_without_pem")]
    [InlineData(true, null, null, "/certs/client.key", "profile.kafka.tls.key_without_pem")]
    [InlineData(true, " /certs/ca.pem", null, null, "profile.kafka.tls.path_invalid")]
    [InlineData(true, "/certs/ca.pem", "/certs/client.pem", "/certs/client.key", null)]
    public void CertificateFilesAreValidated(bool useTls, string? ca, string? certificate, string? key, string? expected)
    {
        var profile = ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development, new(AuthenticationKind.KafkaNone)) with
        {
            Provider = MessagingProvider.Kafka,
            Kafka = new KafkaSettings("broker.invalid:9093", useTls, Tls: new TlsClientSettings(ca, certificate, key))
        };

        var codes = ProfileValidator.Validate(profile).Errors.Select(error => error.Code).ToArray();

        if (expected is null)
        {
            Assert.Empty(codes);
        }
        else
        {
            Assert.Contains(expected, codes);
        }
    }

    [Fact]
    public void ProfilesWithoutCertificatesSerializeAsBefore()
    {
        // The configuration identity of scheduled resends is hashed from this JSON.
        var kafka = JsonSerializer.Serialize(new KafkaSettings("broker:9092"));
        var rabbit = JsonSerializer.Serialize(new RabbitMqSettings("rabbit", "user"));

        Assert.DoesNotContain("Tls\":", kafka.Replace("UseTls\":", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.DoesNotContain("Tls\":", rabbit.Replace("UseTls\":", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
    }

    internal static X509Certificate2 Issue(string name, bool authority = false, X509Certificate2? issuer = null)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={name}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(authority, false, 0, authority));
        if (authority)
        {
            request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        }
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        // An issued certificate must not outlive its issuer, whose expiry was rounded to the second.
        var notAfter = issuer is null ? DateTimeOffset.UtcNow.AddDays(30) : issuer.NotAfter.AddDays(-1);
        if (issuer is null)
        {
            return request.CreateSelfSigned(notBefore, notAfter);
        }
        using var signed = request.Create(issuer, notBefore, notAfter, RandomNumberGenerator.GetBytes(8));
        return signed.CopyWithPrivateKey(key);
    }
}

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task AClientCertificatePasswordIsStoredWithTheEnvironmentAndRemovedWithItsCertificate()
    {
        using var directory = new TemporaryDirectory();
        using var repository = new JsonProfileRepository(QueueLoomPaths.ForRoot(directory.Path));
        var original = ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development, new(AuthenticationKind.KafkaNone)) with
        {
            Provider = MessagingProvider.Kafka,
            Kafka = new KafkaSettings("broker.invalid:9093", UseTls: true)
        };
        await repository.UpsertAndSelectAsync(original);
        var vault = new FakeSecretVault();
        var key = ProfileSecretKey.ClientCertificatePassword(original.Id);
        var withCertificate = original with
        {
            Kafka = original.Kafka! with { Tls = new TlsClientSettings(ClientCertificatePath: "/certs/client.p12") }
        };
        var dialogs = new FakeDialogService
        {
            EditResult = new ProfileEditorResult(withCertificate, null, false) { ClientCertificatePassword = "synthetic" }
        };
        await using var vm = CreateViewModel(repository, new FakeWorkspace(), dialogs, secretVault: vault);
        await vm.InitializeAsync();

        await vm.EditEnvironmentCommand.ExecuteAsync();

        Assert.False(vm.HasError, vm.ErrorText);
        Assert.Equal("synthetic", await vault.RetrieveAsync(key));
        Assert.Equal("/certs/client.p12", (await repository.GetAsync(original.Id))!.Kafka!.Tls!.ClientCertificatePath);

        dialogs.EditResult = new ProfileEditorResult(original, null, false) { RemovesClientCertificatePassword = true };
        await vm.EditEnvironmentCommand.ExecuteAsync();

        Assert.False(vm.HasError, vm.ErrorText);
        Assert.Null(await vault.RetrieveAsync(key));
        Assert.Null((await repository.GetAsync(original.Id))!.Kafka!.Tls);
    }
}

public sealed class ClientTlsEditorTests
{
    private static ServiceBusProfile Saved(TlsClientSettings? tls) =>
        ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development, new(AuthenticationKind.KafkaNone)) with
        {
            Provider = MessagingProvider.Kafka,
            Kafka = new KafkaSettings("broker.invalid:9093", UseTls: true, Tls: tls)
        };

    [Fact]
    public void ANewClientCertificateIsSavedWithItsPassword()
    {
        var editor = new QueueLoom.App.ViewModels.ProfileEditorViewModel(Saved(null))
        {
            TlsCaCertificatePath = " /certs/ca.pem ",
            TlsClientCertificatePath = "/certs/client.p12",
            TlsClientCertificatePassword = "synthetic"
        };

        Assert.True(editor.TryBuild(out var result), editor.Error);

        Assert.Equal(new TlsClientSettings("/certs/ca.pem", "/certs/client.p12"), result!.Profile.Kafka!.Tls);
        Assert.Equal("synthetic", result.ClientCertificatePassword);
        Assert.False(result.RemovesClientCertificatePassword);
    }

    [Fact]
    public void AnUnchangedCertificateKeepsItsStoredPassword()
    {
        var tls = new TlsClientSettings(ClientCertificatePath: "/certs/client.p12");
        var editor = new QueueLoom.App.ViewModels.ProfileEditorViewModel(Saved(tls));

        Assert.True(editor.TryBuild(out var result), editor.Error);

        Assert.Equal(tls, result!.Profile.Kafka!.Tls);
        Assert.Null(result.ClientCertificatePassword);
        Assert.False(result.RemovesClientCertificatePassword);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ClearingTheCertificateOrTurningTlsOffRemovesItsPassword(bool turnTlsOff)
    {
        var editor = new QueueLoom.App.ViewModels.ProfileEditorViewModel(Saved(new TlsClientSettings("/certs/ca.pem", "/certs/client.p12")));
        if (turnTlsOff)
        {
            editor.KafkaUseTls = false;
        }
        else
        {
            editor.TlsClientCertificatePath = string.Empty;
        }

        Assert.True(editor.TryBuild(out var result), editor.Error);

        Assert.Equal(turnTlsOff ? null : new TlsClientSettings("/certs/ca.pem"), result!.Profile.Kafka!.Tls);
        Assert.True(result.RemovesClientCertificatePassword);
    }
}
