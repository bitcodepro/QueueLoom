using System.Text.Json;
using QueueLoom.App.Services;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.App.ViewModels;

/// <summary>A cloud the operator can pick when adding an environment.</summary>
public sealed record ProviderOption(MessagingProvider Provider, string Title, string Description);

/// <summary>A sign-in method with the label shown in the editor.</summary>
public sealed record AuthenticationOption(AuthenticationKind Kind, string Label);

public sealed partial class ProfileEditorViewModel : ObservableObject
{
    private readonly ServiceBusProfile? _existing;
    private ProviderOption _selectedProvider;
    private IReadOnlyList<AuthenticationOption> _authenticationOptions = [];
    private AuthenticationOption? _selectedAuthentication;
    private string _name;
    private EnvironmentKind _environment;
    private string _customEnvironmentName;
    private string _fullyQualifiedNamespace;
    private string _connectionString = string.Empty;
    private EntraIdCredentialKind _credentialKind;
    private string _tenantId;
    private string _clientId;
    private string _awsRegion;
    private string _awsServiceUrl;
    private string _awsProfileName;
    private string _awsAccessKeyId = string.Empty;
    private string _awsSecretAccessKey = string.Empty;
    private string _awsSessionToken = string.Empty;
    private string _googleProjectId;
    private string _googleEmulatorHost;
    private string _googleServiceAccountKey = string.Empty;
    private ProfileAccessMode _accessMode;
    private bool _allowQueueManagement;
    private string _error = string.Empty;
    public int EmulatorManagementPort { get; set; } = 5300;

    public ProfileEditorViewModel(ServiceBusProfile? existing)
    {
        _existing = existing;
        EmulatorManagementPort = existing?.EmulatorManagementPort ?? 5300;
        _name = existing?.Name ?? string.Empty;
        _environment = existing?.Environment ?? EnvironmentKind.Development;
        _customEnvironmentName = existing?.CustomEnvironmentName ?? string.Empty;
        _fullyQualifiedNamespace = existing?.FullyQualifiedNamespace ?? string.Empty;
        _credentialKind = existing?.Authentication.EntraId?.CredentialKind
            ?? EntraIdCredentialKind.DefaultAzureCredential;
        _tenantId = existing?.Authentication.EntraId?.TenantId ?? string.Empty;
        _clientId = existing?.Authentication.EntraId?.ClientId ?? string.Empty;
        _awsRegion = existing?.Aws?.Region ?? string.Empty;
        _awsServiceUrl = existing?.Aws?.ServiceUrl ?? string.Empty;
        _awsProfileName = existing?.Aws?.ProfileName ?? string.Empty;
        _googleProjectId = existing?.GooglePubSub?.ProjectId ?? string.Empty;
        _googleEmulatorHost = existing?.GooglePubSub?.EmulatorHost ?? string.Empty;
        _allowQueueManagement = existing?.AllowQueueManagement ?? false;
        _accessMode = existing?.AccessMode
            ?? (_environment == EnvironmentKind.Production ? ProfileAccessMode.ReadOnly : ProfileAccessMode.ReadWrite);

        InitializeBrokers(existing);
        _selectedProvider = ProviderOptions.First(option => option.Provider == (existing?.Provider ?? MessagingProvider.AzureServiceBus));
        ApplyProvider(existing?.Authentication.Kind);
    }

    public string DialogTitle => _existing is null ? "Add environment" : "Edit environment";

    public IReadOnlyList<ProviderOption> ProviderOptions { get; } =
    [
        new(MessagingProvider.AzureServiceBus, "Azure Service Bus", "Queues, topics and subscriptions of a namespace"),
        new(MessagingProvider.AmazonSqsSns, "Amazon SQS / SNS", "SQS queues and SNS topics of an account region"),
        new(MessagingProvider.GooglePubSub, "Google Cloud Pub/Sub", "Topics and subscriptions of a project"),
        new(MessagingProvider.RabbitMq, "RabbitMQ", "Queues and exchanges of a virtual host"),
        new(MessagingProvider.Kafka, "Apache Kafka", "Topics of a cluster, with dead-letter topics")
    ];

    /// <summary>The cloud cannot change for a saved environment: its secret and history belong to that cloud.</summary>
    public bool CanChangeProvider => _existing is null;

    public ProviderOption SelectedProvider
    {
        get => _selectedProvider;
        set
        {
            if (value is null || !CanChangeProvider || !SetProperty(ref _selectedProvider, value))
            {
                return;
            }

            ApplyProvider(null);
        }
    }

    public MessagingProvider Provider => SelectedProvider.Provider;

    public bool IsAzure => Provider == MessagingProvider.AzureServiceBus;

    public bool IsAws => Provider == MessagingProvider.AmazonSqsSns;

    public bool IsGoogle => Provider == MessagingProvider.GooglePubSub;

    public IReadOnlyList<EnvironmentKind> EnvironmentKinds { get; } = Enum.GetValues<EnvironmentKind>();

    public IReadOnlyList<AuthenticationOption> AuthenticationOptions
    {
        get => _authenticationOptions;
        private set => SetProperty(ref _authenticationOptions, value);
    }

    public AuthenticationOption? SelectedAuthentication
    {
        get => _selectedAuthentication;
        set
        {
            if (value is not null && SetProperty(ref _selectedAuthentication, value))
            {
                NotifyAuthenticationChanged();
            }
        }
    }

    public AuthenticationKind AuthenticationKind
    {
        get => SelectedAuthentication?.Kind ?? AuthenticationKind.EntraId;
        set => SelectedAuthentication = AuthenticationOptions.FirstOrDefault(option => option.Kind == value) ?? SelectedAuthentication;
    }

    public IReadOnlyList<EntraIdCredentialKind> CredentialKinds { get; } = Enum.GetValues<EntraIdCredentialKind>();

    public IReadOnlyList<ProfileAccessMode> AccessModes { get; } = Enum.GetValues<ProfileAccessMode>();

    /// <summary>Creating, changing and deleting queues; off unless the operator turns it on for this environment.</summary>
    public bool AllowQueueManagement
    {
        get => _allowQueueManagement;
        set => SetProperty(ref _allowQueueManagement, value);
    }

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

    public string AwsRegion
    {
        get => _awsRegion;
        set => SetProperty(ref _awsRegion, value);
    }

    /// <summary>Common regions offered in the region box; any other region code can be typed.</summary>
    public IReadOnlyList<string> AwsRegions { get; } =
    [
        "us-east-1", "us-east-2", "us-west-1", "us-west-2", "ca-central-1", "sa-east-1",
        "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-central-2", "eu-north-1", "eu-south-1",
        "ap-south-1", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2", "me-central-1"
    ];

    public string AwsServiceUrl
    {
        get => _awsServiceUrl;
        set => SetProperty(ref _awsServiceUrl, value);
    }

    public string AwsProfileName
    {
        get => _awsProfileName;
        set => SetProperty(ref _awsProfileName, value);
    }

    public string AwsAccessKeyId
    {
        get => _awsAccessKeyId;
        set => SetProperty(ref _awsAccessKeyId, value);
    }

    public string AwsSecretAccessKey
    {
        get => _awsSecretAccessKey;
        set => SetProperty(ref _awsSecretAccessKey, value);
    }

    public string AwsSessionToken
    {
        get => _awsSessionToken;
        set => SetProperty(ref _awsSessionToken, value);
    }

    public string GoogleProjectId
    {
        get => _googleProjectId;
        set => SetProperty(ref _googleProjectId, value);
    }

    public string GoogleEmulatorHost
    {
        get => _googleEmulatorHost;
        set => SetProperty(ref _googleEmulatorHost, value);
    }

    public string GoogleServiceAccountKey
    {
        get => _googleServiceAccountKey;
        set
        {
            if (SetProperty(ref _googleServiceAccountKey, value))
            {
                OnPropertyChanged(nameof(GoogleKeySummary));
                // A key names its project: fill it in when the operator has not typed one.
                if (string.IsNullOrWhiteSpace(GoogleProjectId) && TryReadServiceAccount(value, out _, out var projectId))
                {
                    GoogleProjectId = projectId ?? string.Empty;
                }
            }
        }
    }

    /// <summary>A service account key is a few kilobytes; anything much larger is not one.</summary>
    internal const int MaximumKeyFileBytes = 64 * 1024;

    /// <summary>
    /// Loads a downloaded service account key. The stream is read without asking its length (a picker may hand out
    /// one that cannot tell), at most <see cref="MaximumKeyFileBytes"/>; any failure is shown here instead of escaping
    /// the click handler, where it would end the application.
    /// </summary>
    public async Task LoadGoogleServiceAccountKeyAsync(Func<Task<Stream>> open)
    {
        ArgumentNullException.ThrowIfNull(open);
        try
        {
            await using var stream = await open().ConfigureAwait(true);
            var buffer = new byte[MaximumKeyFileBytes + 1];
            var read = 0;
            int count;
            while (read < buffer.Length && (count = await stream.ReadAsync(buffer.AsMemory(read)).ConfigureAwait(true)) > 0)
            {
                read += count;
            }
            if (read > MaximumKeyFileBytes)
            {
                Error = "That file is larger than a service account key (64 KB). Choose the JSON key downloaded for the service account.";
                return;
            }
            Error = string.Empty;
            GoogleServiceAccountKey = new System.Text.UTF8Encoding(false).GetString(buffer, 0, read).TrimStart('\uFEFF');
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            ReportKeyFileProblem(exception);
        }
    }

    /// <summary>Shows why a key file could not be chosen or read.</summary>
    public void ReportKeyFileProblem(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        Error = $"The key file could not be read: {QueueLoom.Core.Diagnostics.SensitiveDataRedactor.SummarizeException(exception)}";
    }

    /// <summary>"Key for queueloom@orders-prod.iam.gserviceaccount.com", or how to get one.</summary>
    public string GoogleKeySummary =>
        TryReadServiceAccount(GoogleServiceAccountKey, out var email, out _)
            ? $"Key for {email}"
            : HasExistingSecret
                ? "Leave empty to keep the currently encrypted key."
                : "Paste the JSON key of a service account, or load the downloaded key file.";

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

    public bool IsAwsAccessKey => AuthenticationKind == AuthenticationKind.AwsAccessKey;

    public bool IsAwsDefaultCredentials => AuthenticationKind == AuthenticationKind.AwsDefaultCredentials;

    public bool IsGoogleServiceAccountKey => AuthenticationKind == AuthenticationKind.GoogleServiceAccountKey;

    /// <summary>The saved environment already has an encrypted secret for the chosen sign-in method.</summary>
    public bool HasExistingSecret =>
        _existing?.Authentication.Kind == AuthenticationKind && AuthenticationKind.UsesStoredSecret();

    public bool HasExistingConnectionString =>
        _existing?.Authentication.Kind == AuthenticationKind.ConnectionString;

    public string ConnectionStringHint => HasExistingConnectionString
        ? "Leave empty to keep the currently encrypted secret."
        : "Stored only in the operating-system-backed QueueLoom vault.";

    public string AwsAccessKeyHint => HasExistingSecret
        ? "Leave both empty to keep the currently encrypted key."
        : "Stored only in the operating-system-backed QueueLoom vault. For LocalStack any values work, for example test / test.";

    public string SecretNote => Provider switch
    {
        MessagingProvider.AmazonSqsSns => "Access keys never enter profile metadata or logs.",
        MessagingProvider.GooglePubSub => "Service account keys never enter profile metadata or logs.",
        MessagingProvider.RabbitMq or MessagingProvider.Kafka => "Passwords never enter profile metadata or logs.",
        _ => "Connection strings never enter profile metadata or logs."
    };

    public bool TryBuild(out ProfileEditorResult? result)
    {
        result = null;
        Error = string.Empty;
        if (EmulatorManagementPort is < 1 or > 65535)
        {
            Error = "Emulator management port must be between 1 and 65535.";
            return false;
        }

        var built = Provider switch
        {
            MessagingProvider.AmazonSqsSns => BuildAws(),
            MessagingProvider.GooglePubSub => BuildGoogle(),
            MessagingProvider.RabbitMq => BuildRabbitMq(),
            MessagingProvider.Kafka => BuildKafka(),
            _ => BuildAzure()
        };
        if (built is null)
        {
            return false;
        }

        var (profile, newSecret) = built.Value;
        var validation = ProfileValidator.Validate(profile);
        if (!validation.IsValid)
        {
            Error = string.Join(" ", validation.Errors.Select(item => item.Message));
            return false;
        }

        var (registryPassword, removeRegistryPassword) = SchemaRegistrySecret(profile);
        result = new ProfileEditorResult(profile, newSecret, newSecret is not null)
        {
            SchemaRegistryPassword = registryPassword,
            RemovesSchemaRegistryPassword = removeRegistryPassword
        };
        return true;
    }

    private (ServiceBusProfile Profile, string? Secret)? BuildAzure()
    {
        string? normalizedNamespace = string.IsNullOrWhiteSpace(FullyQualifiedNamespace)
            ? null
            : ProfileValidator.NormalizeFullyQualifiedNamespace(FullyQualifiedNamespace);
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
                    return null;
                }

                normalizedNamespace = parsedNamespace;
                newSecret = ConnectionString;
            }
            else if (!HasExistingConnectionString)
            {
                Error = "Enter a namespace-level Azure Service Bus connection string.";
                return null;
            }
        }
        else
        {
            authentication = AuthenticationSettings.Entra(
                CredentialKind,
                NullIfWhiteSpace(TenantId),
                NullIfWhiteSpace(ClientId));
        }

        return (NewProfile(authentication, normalizedNamespace), newSecret);
    }

    private (ServiceBusProfile Profile, string? Secret)? BuildAws()
    {
        string? newSecret = null;
        if (AuthenticationKind == AuthenticationKind.AwsAccessKey)
        {
            var hasId = !string.IsNullOrWhiteSpace(AwsAccessKeyId);
            var hasSecret = !string.IsNullOrWhiteSpace(AwsSecretAccessKey);
            if (hasId && hasSecret)
            {
                newSecret = new AwsAccessKey(
                    AwsAccessKeyId.Trim(),
                    AwsSecretAccessKey.Trim(),
                    NullIfWhiteSpace(AwsSessionToken)).ToSecret();
            }
            else if (hasId || hasSecret || !HasExistingSecret)
            {
                Error = "Enter both the access key ID and the secret access key.";
                return null;
            }
        }

        var settings = new AwsSettings(
            AwsRegion.Trim().ToLowerInvariant(),
            NullIfWhiteSpace(AwsServiceUrl)?.TrimEnd('/'),
            AuthenticationKind == AuthenticationKind.AwsDefaultCredentials ? NullIfWhiteSpace(AwsProfileName) : null);
        var profile = NewProfile(new AuthenticationSettings(AuthenticationKind), null) with { Aws = settings };
        return (profile, newSecret);
    }

    private (ServiceBusProfile Profile, string? Secret)? BuildGoogle()
    {
        string? newSecret = null;
        if (AuthenticationKind == AuthenticationKind.GoogleServiceAccountKey)
        {
            if (!string.IsNullOrWhiteSpace(GoogleServiceAccountKey))
            {
                if (!TryReadServiceAccount(GoogleServiceAccountKey, out _, out _))
                {
                    Error = "This is not a service account key. In Google Cloud, open IAM → Service accounts → Keys and create a JSON key.";
                    return null;
                }
                newSecret = GoogleServiceAccountKey.Trim();
            }
            else if (!HasExistingSecret && string.IsNullOrWhiteSpace(GoogleEmulatorHost))
            {
                Error = "Paste or load a service account key.";
                return null;
            }
        }

        var settings = new GooglePubSubSettings(GoogleProjectId.Trim(), NullIfWhiteSpace(GoogleEmulatorHost));
        var profile = NewProfile(new AuthenticationSettings(AuthenticationKind), null) with { GooglePubSub = settings };
        return (profile, newSecret);
    }

    private ServiceBusProfile NewProfile(AuthenticationSettings authentication, string? fullyQualifiedNamespace) =>
        new ServiceBusProfile(
            _existing?.Id ?? Guid.NewGuid(),
            Name.Trim(),
            Environment,
            NullIfWhiteSpace(CustomEnvironmentName),
            fullyQualifiedNamespace,
            authentication,
            Environment == EnvironmentKind.Production ? ProfileAccessMode.ReadOnly : AccessMode)
        {
            EmulatorManagementPort = EmulatorManagementPort,
            Provider = Provider,
            AllowQueueManagement = AllowQueueManagement
        };

    private void ApplyProvider(AuthenticationKind? preferred)
    {
        AuthenticationOptions = AuthenticationKinds.For(Provider)
            .Select(kind => new AuthenticationOption(kind, Label(kind)))
            .ToArray();
        _selectedAuthentication = AuthenticationOptions.FirstOrDefault(option => option.Kind == preferred)
            ?? AuthenticationOptions[0];
        OnPropertyChanged(nameof(SelectedAuthentication));
        OnPropertyChanged(nameof(Provider));
        OnPropertyChanged(nameof(IsAzure));
        OnPropertyChanged(nameof(IsAws));
        OnPropertyChanged(nameof(IsGoogle));
        OnPropertyChanged(nameof(IsRabbitMq));
        OnPropertyChanged(nameof(IsKafka));
        OnPropertyChanged(nameof(SecretNote));
        NotifyAuthenticationChanged();
    }

    private void NotifyAuthenticationChanged()
    {
        OnPropertyChanged(nameof(AuthenticationKind));
        OnPropertyChanged(nameof(IsConnectionString));
        OnPropertyChanged(nameof(IsEntraId));
        OnPropertyChanged(nameof(IsAwsAccessKey));
        OnPropertyChanged(nameof(IsAwsDefaultCredentials));
        OnPropertyChanged(nameof(IsGoogleServiceAccountKey));
        OnPropertyChanged(nameof(HasExistingSecret));
        OnPropertyChanged(nameof(AwsAccessKeyHint));
        OnPropertyChanged(nameof(GoogleKeySummary));
        OnPropertyChanged(nameof(IsKafkaSasl));
        OnPropertyChanged(nameof(BrokerPasswordHint));
    }

    private static string Label(AuthenticationKind kind) => kind switch
    {
        AuthenticationKind.EntraId => "Microsoft Entra ID (passwordless)",
        AuthenticationKind.ConnectionString => "Connection string (SAS)",
        AuthenticationKind.AwsAccessKey => "Access key",
        AuthenticationKind.AwsDefaultCredentials => "AWS profile or default credentials",
        AuthenticationKind.GoogleServiceAccountKey => "Service account key (JSON)",
        AuthenticationKind.GoogleApplicationDefault => "Application Default Credentials",
        AuthenticationKind.RabbitMqPassword => "User name and password",
        AuthenticationKind.KafkaNone => "No sign-in",
        AuthenticationKind.KafkaSaslPassword => "SASL user name and password",
        _ => kind.ToString()
    };

    public static bool TryReadServiceAccount(string? json, out string? clientEmail, out string? projectId)
    {
        clientEmail = null;
        projectId = null;
        if (string.IsNullOrWhiteSpace(json))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object ||
                !root.TryGetProperty("type", out var type) || type.GetString() != "service_account" ||
                !root.TryGetProperty("private_key", out var key) || string.IsNullOrWhiteSpace(key.GetString()) ||
                !root.TryGetProperty("client_email", out var email) || string.IsNullOrWhiteSpace(email.GetString()))
            {
                return false;
            }

            clientEmail = email.GetString();
            projectId = root.TryGetProperty("project_id", out var project) ? project.GetString() : null;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    private static string? NullIfWhiteSpace(string value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
