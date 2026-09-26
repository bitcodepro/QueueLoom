using QueueLoom.App.Models;
using QueueLoom.Core.Profiles;

namespace QueueLoom.App.ViewModels;

public sealed class ProfileItemViewModel(ServiceBusProfile profile) : ObservableObject
{
    private bool _isConnected;

    public ServiceBusProfile Profile { get; private set; } = profile;

    public Guid Id => Profile.Id;

    public string Name => Profile.Name;

    public string EnvironmentLabel => Profile.Environment switch
    {
        EnvironmentKind.Development => "DEV",
        EnvironmentKind.Test => "TEST",
        EnvironmentKind.Production => "PROD",
        _ => Profile.EnvironmentDisplayName.ToUpperInvariant()
    };

    public Tone EnvironmentTone => Tones.ForEnvironment(Profile.Environment);

    public string Namespace => string.IsNullOrWhiteSpace(Profile.FullyQualifiedNamespace)
        ? "Namespace from secure connection string"
        : Profile.FullyQualifiedNamespace;

    public string AuthenticationLabel => Profile.Authentication.Kind switch
    {
        AuthenticationKind.ConnectionString => "SAS connection string",
        AuthenticationKind.EntraId => $"Entra ID · {Profile.Authentication.EntraId?.CredentialKind}",
        _ => "Unknown"
    };

    public bool IsProduction => Profile.Environment == EnvironmentKind.Production;

    public bool IsReadOnly => Profile.AccessMode == ProfileAccessMode.ReadOnly;

    public bool IsConnected
    {
        get => _isConnected;
        private set
        {
            if (SetProperty(ref _isConnected, value))
            {
                OnPropertyChanged(nameof(ConnectionLabel));
                OnPropertyChanged(nameof(ConnectionTone));
            }
        }
    }

    public string ConnectionLabel => IsConnected ? "CONNECTED" : "OFFLINE";

    public Tone ConnectionTone => IsConnected ? Tone.Success : Tone.Neutral;

    internal void UpdateConnectionState(bool isConnected) => IsConnected = isConnected;

    public void Update(ServiceBusProfile updated)
    {
        Profile = updated;
        OnPropertyChanged(nameof(Name));
        OnPropertyChanged(nameof(EnvironmentLabel));
        OnPropertyChanged(nameof(EnvironmentTone));
        OnPropertyChanged(nameof(Namespace));
        OnPropertyChanged(nameof(AuthenticationLabel));
        OnPropertyChanged(nameof(IsProduction));
        OnPropertyChanged(nameof(IsReadOnly));
    }
}
