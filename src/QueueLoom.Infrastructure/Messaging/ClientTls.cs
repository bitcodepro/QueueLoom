using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Infrastructure.Messaging;

/// <summary>
/// A private certificate authority and a client certificate (mutual TLS) for brokers reached over TLS. The files are read
/// when connecting, so a missing file, a wrong password or a certificate without its key is reported then, by name.
/// </summary>
internal sealed class ClientTls : IDisposable
{
    private ClientTls(X509Certificate2? clientCertificate, X509Certificate2Collection? authorities)
    {
        ClientCertificate = clientCertificate;
        Authorities = authorities;
    }

    /// <summary>The client certificate with its private key, or null when the broker does not ask for one.</summary>
    public X509Certificate2? ClientCertificate { get; }

    /// <summary>The private certificate authorities the broker's certificate must chain to, or null for the system's.</summary>
    public X509Certificate2Collection? Authorities { get; }

    public static async Task<ClientTls?> LoadAsync(ServiceBusProfile profile, TlsClientSettings? settings, ISecretVault vault,
        CancellationToken cancellationToken)
    {
        if (settings is null || settings.IsEmpty)
        {
            return null;
        }

        var password = settings.ClientCertificatePath is null
            ? null
            : await vault.RetrieveForProfileAsync(profile, ProfileSecretKind.ClientCertificatePassword, cancellationToken).ConfigureAwait(false);
        return Load(settings, password);
    }

    internal static ClientTls Load(TlsClientSettings settings, string? password)
    {
        X509Certificate2Collection? authorities = null;
        X509Certificate2? client = null;
        try
        {
            if (settings.CaCertificatePath is { } caPath)
            {
                authorities = [];
                authorities.ImportFromPemFile(Existing(caPath, "certificate authority"));
                if (authorities.Count == 0)
                {
                    throw new InvalidOperationException($"The certificate authority file {caPath} holds no PEM certificate.");
                }
            }
            if (settings.ClientCertificatePath is { } certificatePath)
            {
                client = LoadClientCertificate(settings, Existing(certificatePath, "client certificate"), password);
            }
            return new ClientTls(client, authorities);
        }
        catch
        {
            client?.Dispose();
            DisposeAll(authorities);
            throw;
        }
    }

    /// <summary>
    /// Accepts the broker's certificate as the system would, or, with a private authority, when it chains to that
    /// authority. The host name must match either way.
    /// </summary>
    public bool ValidateServerCertificate(X509Certificate? certificate, SslPolicyErrors errors)
    {
        if (errors == SslPolicyErrors.None)
        {
            return true;
        }
        if (Authorities is null || certificate is null || (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
        {
            return false;
        }

        using var leaf = X509CertificateLoader.LoadCertificate(certificate.GetRawCertData());
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.AddRange(Authorities);
        // A private authority rarely publishes revocation lists the client can reach.
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(leaf);
    }

    public void Dispose()
    {
        ClientCertificate?.Dispose();
        DisposeAll(Authorities);
    }

    private static X509Certificate2 LoadClientCertificate(TlsClientSettings settings, string path, string? password)
    {
        X509Certificate2 certificate;
        try
        {
            if (settings.IsPkcs12)
            {
                certificate = X509CertificateLoader.LoadPkcs12FromFile(path, password);
            }
            else
            {
                var keyPath = settings.ClientKeyPath is null ? null : Existing(settings.ClientKeyPath, "client key");
                using var pem = string.IsNullOrEmpty(password)
                    ? X509Certificate2.CreateFromPemFile(path, keyPath)
                    : X509Certificate2.CreateFromEncryptedPemFile(path, password, keyPath);
                // Windows TLS (SChannel) cannot use the ephemeral key a PEM file is loaded with; a PKCS #12 round trip
                // gives it a key it can use, and does no harm elsewhere.
                certificate = X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
            }
        }
        catch (CryptographicException exception)
        {
            throw new InvalidOperationException(
                $"The client certificate {path} could not be read: {exception.Message} Check the file and its password.", exception);
        }

        if (!certificate.HasPrivateKey)
        {
            certificate.Dispose();
            throw new InvalidOperationException(
                $"The client certificate {path} has no private key. Use a .pfx or .p12 file with the key, or name the PEM key file.");
        }
        return certificate;
    }

    private static string Existing(string path, string what) => File.Exists(path)
        ? path
        : throw new InvalidOperationException($"The {what} file {path} was not found. Edit the environment and choose it again.");

    private static void DisposeAll(X509Certificate2Collection? certificates)
    {
        foreach (var certificate in certificates ?? [])
        {
            certificate.Dispose();
        }
    }
}
