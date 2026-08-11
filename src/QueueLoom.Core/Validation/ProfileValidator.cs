using QueueLoom.Core.Profiles;

namespace QueueLoom.Core.Validation;

public static class ProfileValidator
{
    public const int MaxNameLength = 100;
    public const int MaxEnvironmentNameLength = 50;

    public static ValidationResult Validate(ServiceBusProfile? profile)
    {
        if (profile is null)
        {
            return new ValidationResult(
                [new ValidationError("profile.required", "A profile is required.")]);
        }

        var errors = new List<ValidationError>();

        if (profile.Id == Guid.Empty)
        {
            errors.Add(new ValidationError(
                "profile.id.required",
                "The profile identifier must not be empty.",
                nameof(profile.Id)));
        }

        ValidateName(profile.Name, errors);
        ValidateEnvironment(profile, errors);
        ValidateAccessMode(profile, errors);
        ValidateAuthentication(profile, errors);
        ValidateNamespace(profile, errors);

        return errors.Count == 0 ? ValidationResult.Valid : new ValidationResult(errors);
    }

    public static string NormalizeFullyQualifiedNamespace(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        var normalized = value.Trim();
        if (normalized.StartsWith("sb://", StringComparison.OrdinalIgnoreCase))
        {
            normalized = normalized[5..];
        }

        return normalized.TrimEnd('/').ToLowerInvariant();
    }

    private static void ValidateName(string? name, ICollection<ValidationError> errors)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            errors.Add(new ValidationError(
                "profile.name.required",
                "A profile name is required.",
                nameof(ServiceBusProfile.Name)));
        }
        else if (name.Trim().Length > MaxNameLength)
        {
            errors.Add(new ValidationError(
                "profile.name.too_long",
                $"The profile name cannot exceed {MaxNameLength} characters.",
                nameof(ServiceBusProfile.Name)));
        }
    }

    private static void ValidateEnvironment(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        if (!Enum.IsDefined(profile.Environment))
        {
            errors.Add(new ValidationError(
                "profile.environment.invalid",
                "The environment is not supported.",
                nameof(profile.Environment)));
            return;
        }

        if (profile.Environment != EnvironmentKind.Custom)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(profile.CustomEnvironmentName))
        {
            errors.Add(new ValidationError(
                "profile.environment_name.required",
                "A custom environment name is required.",
                nameof(profile.CustomEnvironmentName)));
        }
        else if (profile.CustomEnvironmentName.Trim().Length > MaxEnvironmentNameLength)
        {
            errors.Add(new ValidationError(
                "profile.environment_name.too_long",
                $"The custom environment name cannot exceed {MaxEnvironmentNameLength} characters.",
                nameof(profile.CustomEnvironmentName)));
        }
    }

    private static void ValidateAccessMode(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        if (!Enum.IsDefined(profile.AccessMode))
        {
            errors.Add(new ValidationError(
                "profile.access_mode.invalid",
                "The profile access mode is not supported.",
                nameof(profile.AccessMode)));
            return;
        }

        if (profile.Environment == EnvironmentKind.Production &&
            profile.AccessMode != ProfileAccessMode.ReadOnly)
        {
            errors.Add(new ValidationError(
                "profile.production.read_only",
                "Production profiles must be persisted as read-only.",
                nameof(profile.AccessMode)));
        }
    }

    private static void ValidateAuthentication(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        if (profile.Authentication is null)
        {
            errors.Add(new ValidationError(
                "profile.authentication.required",
                "An authentication method is required.",
                nameof(profile.Authentication)));
            return;
        }

        if (!Enum.IsDefined(profile.Authentication.Kind))
        {
            errors.Add(new ValidationError(
                "profile.authentication.invalid",
                "The authentication method is not supported.",
                nameof(profile.Authentication)));
            return;
        }

        if (profile.Authentication.Kind == AuthenticationKind.ConnectionString &&
            profile.Authentication.EntraId is not null)
        {
            errors.Add(new ValidationError(
                "profile.authentication.entra_unexpected",
                "Entra ID settings cannot be used with connection string authentication.",
                nameof(profile.Authentication)));
        }

        if (profile.Authentication.Kind == AuthenticationKind.EntraId &&
            profile.Authentication.EntraId is null)
        {
            errors.Add(new ValidationError(
                "profile.authentication.entra_required",
                "Entra ID settings are required for Entra ID authentication.",
                nameof(profile.Authentication)));
        }
    }

    private static void ValidateNamespace(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        var namespaceValue = profile.FullyQualifiedNamespace;
        if (string.IsNullOrWhiteSpace(namespaceValue))
        {
            if (profile.Authentication?.Kind == AuthenticationKind.EntraId)
            {
                errors.Add(new ValidationError(
                    "profile.namespace.required",
                    "A fully qualified namespace is required for Entra ID authentication.",
                    nameof(profile.FullyQualifiedNamespace)));
            }

            return;
        }

        var value = namespaceValue.Trim();
        if (value.Contains("://", StringComparison.Ordinal) ||
            value.Contains('/') ||
            value.Contains('\\') ||
            value.Contains('?') ||
            value.Contains('#') ||
            value.Contains(':') ||
            Uri.CheckHostName(value) != UriHostNameType.Dns)
        {
            errors.Add(new ValidationError(
                "profile.namespace.invalid",
                "Use a bare fully qualified namespace, for example contoso.servicebus.windows.net.",
                nameof(profile.FullyQualifiedNamespace)));
        }
    }
}
