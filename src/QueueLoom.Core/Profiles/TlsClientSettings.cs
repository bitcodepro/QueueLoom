namespace QueueLoom.Core.Profiles;

/// <summary>
/// Files for TLS beyond the system defaults: a private certificate authority and a client certificate (mutual TLS).
/// Only paths are kept in the profile; a client certificate's password is kept in the secret vault.
/// </summary>
/// <param name="CaCertificatePath">
/// A PEM file with the certificate authority (or authorities) that signed the broker's certificate, for clusters whose
/// certificate the operating system does not trust. Without it the system trust store is used.
/// </param>
/// <param name="ClientCertificatePath">
/// The client certificate the broker asks for: a PKCS #12 file (.pfx or .p12) with its private key, or a PEM file with
/// the certificate (and the key, unless <paramref name="ClientKeyPath"/> names another file).
/// </param>
/// <param name="ClientKeyPath">The private key of a PEM client certificate, when it is in a separate PEM file.</param>
public sealed record TlsClientSettings(
    string? CaCertificatePath = null,
    string? ClientCertificatePath = null,
    string? ClientKeyPath = null)
{
    public bool IsEmpty => CaCertificatePath is null && ClientCertificatePath is null && ClientKeyPath is null;

    /// <summary>Whether the client certificate is a PKCS #12 bundle rather than PEM.</summary>
    public bool IsPkcs12 => ClientCertificatePath is { } path &&
        (path.EndsWith(".pfx", StringComparison.OrdinalIgnoreCase) || path.EndsWith(".p12", StringComparison.OrdinalIgnoreCase));
}
