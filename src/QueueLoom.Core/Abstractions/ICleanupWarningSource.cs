namespace QueueLoom.Core.Abstractions;

/// <summary>
/// A workspace that tells when returning read messages to their queue did not fully work after an operation (for
/// example SQS left some invisible until their visibility timeout ends). The operation's own result stands; the text
/// says what was not released and what that means.
/// </summary>
public interface ICleanupWarningSource
{
    event EventHandler<string>? CleanupWarning;
}
