namespace QueueLoom.Core.Validation;

public sealed class ValidationResult
{
    private static readonly ValidationResult ValidResult = new([]);

    public ValidationResult(IEnumerable<ValidationError> errors)
    {
        ArgumentNullException.ThrowIfNull(errors);
        Errors = Array.AsReadOnly(errors.ToArray());
    }

    public IReadOnlyList<ValidationError> Errors { get; }

    public bool IsValid => Errors.Count == 0;

    public static ValidationResult Valid => ValidResult;

    public bool HasError(string code) =>
        Errors.Any(error => string.Equals(error.Code, code, StringComparison.Ordinal));
}
