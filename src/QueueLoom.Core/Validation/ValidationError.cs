namespace QueueLoom.Core.Validation;

public sealed record ValidationError(
    string Code,
    string Message,
    string? MemberName = null);
