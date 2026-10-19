namespace QueueLoom.Core.Abstractions;

/// <summary>Something the operator should know about an operation that still succeeded, shown in the activity list.</summary>
public sealed record WorkspaceNotice(string Title, string Details);

/// <summary>
/// A workspace that reports notices during operations, for example that the Kafka Schema Registry could not be read
/// and messages are shown without their schemas. The operation's own result stands.
/// </summary>
public interface IWorkspaceNoticeSource
{
    event EventHandler<WorkspaceNotice>? Notice;
}
