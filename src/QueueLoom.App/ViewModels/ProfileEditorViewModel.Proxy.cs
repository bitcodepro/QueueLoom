using QueueLoom.Core.Profiles;

namespace QueueLoom.App.ViewModels;

public sealed record ProxyModeOption(ProxyMode Mode, string Label);

public sealed partial class ProfileEditorViewModel
{
    private ProxyModeOption _selectedProxyMode = null!;
    private string _proxyAddress = string.Empty;
    private string _proxyUserName = string.Empty;
    private string _proxyPassword = string.Empty;
    private bool _hasExistingProxyUser;

    public IReadOnlyList<ProxyModeOption> ProxyModeOptions { get; } =
    [
        new(ProxyMode.System, "This computer's proxy settings"),
        new(ProxyMode.None, "No proxy (connect directly)"),
        new(ProxyMode.Custom, "Custom proxy")
    ];

    public ProxyModeOption SelectedProxyMode
    {
        get => _selectedProxyMode;
        set
        {
            if (SetProperty(ref _selectedProxyMode, value ?? ProxyModeOptions[0]))
            {
                OnPropertyChanged(nameof(IsCustomProxy));
            }
        }
    }

    public bool IsCustomProxy => SelectedProxyMode.Mode == ProxyMode.Custom;

    public string ProxyAddress
    {
        get => _proxyAddress;
        set => SetProperty(ref _proxyAddress, value);
    }

    public string ProxyUserName
    {
        get => _proxyUserName;
        set => SetProperty(ref _proxyUserName, value);
    }

    public string ProxyPassword
    {
        get => _proxyPassword;
        set => SetProperty(ref _proxyPassword, value);
    }

    public string ProxyHint => _hasExistingProxyUser
        ? "Leave the password empty to keep the saved one. Kafka brokers and RabbitMQ's AMQP port always connect directly."
        : "HTTP and WebSocket connections use this proxy. Kafka brokers and RabbitMQ's AMQP port always connect directly.";

    private void InitializeProxy(ProxySettings? proxy)
    {
        _selectedProxyMode = ProxyModeOptions.Single(option => option.Mode == (proxy?.Mode ?? ProxyMode.System));
        _proxyAddress = proxy?.Address ?? string.Empty;
        _proxyUserName = proxy?.UserName ?? string.Empty;
        _hasExistingProxyUser = proxy is { Mode: ProxyMode.Custom, UserName: not null };
    }

    /// <summary>The computer's proxy is saved as no setting, so environments that keep it save as before.</summary>
    private ProxySettings? BuildProxy() => SelectedProxyMode.Mode switch
    {
        ProxyMode.System => null,
        ProxyMode.None => new ProxySettings(ProxyMode.None),
        _ => new ProxySettings(ProxyMode.Custom, NullIfWhiteSpace(ProxyAddress), NullIfWhiteSpace(ProxyUserName))
    };

    /// <summary>What happens to the proxy password in the vault: a new value, removal, or nothing.</summary>
    private (string? Password, bool Remove) ProxySecret(ServiceBusProfile profile) =>
        profile.Proxy?.UserName is null
            ? (null, _hasExistingProxyUser)
            : (string.IsNullOrEmpty(ProxyPassword) ? null : ProxyPassword, false);
}
