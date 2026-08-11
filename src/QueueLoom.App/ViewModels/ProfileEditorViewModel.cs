using QueueLoom.App.Services;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.App.ViewModels;

public sealed class ProfileEditorViewModel : ObservableObject
{
    private readonly ServiceBusProfile? _existing;
    private string _name;
    private EnvironmentKind _environment;
    private string _customEnvironmentName;
    private AuthenticationKind _authenticationKind;
    private string _fullyQualifiedNamespace;
    private string _connectionString = string.Empty;
    private EntraIdCredentialKind _credentialKind;
    private string _tenantId;
    private string _clientId;
    private ProfileAccessMode _accessMode;
    private string _error = string.Empty;

    public ProfileEditorViewModel(ServiceBusProfile? existing)
    {
        _existing = existing;
        _name = existing?.Name ?? string.Empty;
        _environment = existing?.Environment ?? EnvironmentKind.Development;
        _customEnvironmentName = existing?.CustomEnvironmentName ?? string.Empty;
        _authenticationKind = existing?.Authentication.Kind ?? AuthenticationKind.EntraId;
        _fullyQualifiedNamespace = existing?.FullyQualifiedNamespace ?? string.Empty;
        _credentialKind = existing?.Authentication.EntraId?.CredentialKind
            ?? EntraIdCredentialKind.DefaultAzureCredential;
        _tenantId = existing?.Authentication.EntraId?.TenantId ?? string.Empty;
        _clientId = existing?.Authentication.EntraId?.ClientId ?? string.Empty;
        _accessMode = existing?.AccessMode
            ?? (_environment == EnvironmentKind.Production ? ProfileAccessMode.ReadOnly : ProfileAccessMode.ReadWrite);
    }

    public string DialogTitle => _existing is null ? "Add environment" : "Edit environment";

    public IReadOnlyList<EnvironmentKind> EnvironmentKinds { get; } = Enum.GetValues<EnvironmentKind>();

    public IReadOnlyList<AuthenticationKind> AuthenticationKinds { get; } = Enum.GetValues<AuthenticationKind>();

    public IReadOnlyList<EntraIdCredentialKind> CredentialKinds { get; } = Enum.GetValues<EntraIdCredentialKind>();

    public IReadOnlyList<ProfileAccessMode> AccessModes { get; } = Enum.GetValues<ProfileAccessMode>();

    public string Name
    {
        get => _name;
        set => SetProperty(ref _name, value);
    }

    public EnvironmentKind Environment
    {
        get => _environment;
        set
        {
            if (!SetProperty(ref _environment, value))
            {
                return;
            }

            if (value == EnvironmentKind.Production)
            {
                AccessMode = ProfileAccessMode.ReadOnly;
            }
            OnPropertyChanged(nameof(IsCustomEnvironment));
            OnPropertyChanged(nameof(IsProduction));
        }
    }

    public string CustomEnvironmentName
    {
        get => _customEnvironmentName;
        set => SetProperty(ref _customEnvironmentName, value);
    }

    public AuthenticationKind AuthenticationKind
    {
        get => _authenticationKind;
        set
        {
            if (SetProperty(ref _authenticationKind, value))
            {
                OnPropertyChanged(nameof(IsConnectionString));
                OnPropertyChanged(nameof(IsEntraId));
            }
        }
    }

    public string FullyQualifiedNamespace
    {
        get => _fullyQualifiedNamespace;
        set => SetProperty(ref _fullyQualifiedNamespace, value);
    }

    public string ConnectionString
    {
        get => _connectionString;
        set => SetProperty(ref _connectionString, value);
    }

    public EntraIdCredentialKind CredentialKind
    {
        get => _credentialKind;
        set => SetProperty(ref _credentialKind, value);
    }

    public string TenantId
    {
        get => _tenantId;
        set => SetProperty(ref _tenantId, value);
    }

    public string ClientId
    {
        get => _clientId;
        set => SetProperty(ref _clientId, value);
    }

    public ProfileAccessMode AccessMode
    {
        get => _accessMode;
        set => SetProperty(ref _accessMode, value);
    }

    public string Error
    {
        get => _error;
        private set
        {
            if (SetProperty(ref _error, value))
            {
                OnPropertyChanged(nameof(HasError));
            }
        }
    }

    public bool HasError => !string.IsNullOrWhiteSpace(Error);

    public bool IsCustomEnvironment => Environment == EnvironmentKind.Custom;

    public bool IsProduction => Environment == EnvironmentKind.Production;

    public bool IsConnectionString => AuthenticationKind == AuthenticationKind.ConnectionString;

    public bool IsEntraId => AuthenticationKind == AuthenticationKind.EntraId;

    public bool HasExistingConnectionString =>
        _existing?.Authentication.Kind == AuthenticationKind.ConnectionString;

    public string ConnectionStringHint => HasExistingConnectionString
        ? "Leave empty to keep the currently encrypted secret."
        : "Stored only in the operating-system-backed QueueLoom vault.";

    public bool TryBuild(out ProfileEditorResult? result)
    {
        result = null;
        Error = string.Empty;

        string? normalizedNamespace = string.IsNullOrWhiteSpace(FullyQualifiedNamespace)
            ? null
            : ProfileValidator.NormalizeFullyQualifiedNamespace(FullyQualifiedNamespace);
        var replacesSecret = false;
        string? newSecret = null;

        AuthenticationSettings authentication;
        if (AuthenticationKind == AuthenticationKind.ConnectionString)
        {
            authentication = AuthenticationSettings.ConnectionString();
            if (!string.IsNullOrWhiteSpace(ConnectionString))
            {
                if (!ServiceBusConnectionStringInspector.TryGetNamespace(
                        ConnectionString,
                        out var parsedNamespace,
                        out var parseError))
                {
                    Error = parseError ?? "The connection string is invalid.";
                    return false;
                }

                normalizedNamespace = parsedNamespace;
                newSecret = ConnectionString;
                replacesSecret = true;
            }
            else if (!HasExistingConnectionString)
            {
                Error = "Enter a namespace-level Azure Service Bus connection string.";
                return false;
            }
        }
        else
        {
            authentication = AuthenticationSettings.Entra(
                CredentialKind,
                NullIfWhiteSpace(TenantId),
                NullIfWhiteSpace(ClientId));
        }

        var profile = new ServiceBusProfile(
            _existing?.Id ?? Guid.NewGuid(),
            Name.Trim(),
            Environment,
            NullIfWhiteSpace(CustomEnvironmentName),
            normalizedNamespace,
            authentication,
            Environment == EnvironmentKind.Production ? ProfileAccessMode.ReadOnly : AccessMode);
        var validation = ProfileValidator.Validate(profile);
        if (!validation.IsValid)
        {
            Error = string.Join(" ", validation.Errors.Select(item => item.Message));
            return false;
        }

        result = new ProfileEditorResult(profile, newSecret, replacesSecret);
        return true;
    }

    private static string? NullIfWhiteSpace(string value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
