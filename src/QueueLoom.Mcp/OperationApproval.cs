using ModelContextProtocol.Server;

namespace QueueLoom.Mcp;

/// <summary>A change an MCP client asked for, described for the person who has to approve it.</summary>
/// <param name="Action">Short action name, e.g. "Delete dead-letter messages".</param>
/// <param name="EnvironmentName">The saved environment the change applies to.</param>
/// <param name="IsProduction">Production changes require typing the environment name.</param>
/// <param name="Details">Everything the approver needs: namespace, entities, counts and consequences.</param>
public sealed record ApprovalRequest(
    string Action,
    string EnvironmentName,
    bool IsProduction,
    string Details);

public sealed record ApprovalDecision(bool Approved, string Reason)
{
    public static ApprovalDecision Approve(string reason) => new(true, reason);

    public static ApprovalDecision Deny(string reason) => new(false, reason);
}

/// <summary>
/// Asks a human to approve a change. Implementations must never let the MCP client (and so the model)
/// answer on the user's behalf.
/// </summary>
public interface IOperationApprover
{
    Task<ApprovalDecision> RequestAsync(ApprovalRequest request, McpServer server, CancellationToken cancellationToken);
}
