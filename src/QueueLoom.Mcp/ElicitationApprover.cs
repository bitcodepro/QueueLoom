using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace QueueLoom.Mcp;

/// <summary>
/// Asks through the MCP client's own user interface (MCP elicitation). The protocol requires the client
/// to show the request to the user rather than to the model. Used when no QueueLoom window can be shown.
/// </summary>
public sealed class ElicitationApprover : IOperationApprover
{
    public async Task<ApprovalDecision> RequestAsync(
        ApprovalRequest request,
        McpServer server,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(server);
        if (server.ClientCapabilities?.Elicitation is null)
        {
            return ApprovalDecision.Deny(
                "This change needs a person to approve it, but no QueueLoom window can be shown here and the MCP client " +
                "does not support approval prompts (elicitation). Run the client on a desktop session or use the QueueLoom app.");
        }

        var message = $"{request.Action} in '{request.EnvironmentName}'.\n\n{request.Details}\n\n" +
                      (request.IsProduction
                          ? $"This is a production environment. Type '{request.EnvironmentName}' to approve."
                          : "Tick 'approve' to allow this change.");
        var result = await server.ElicitAsync<ApprovalForm>(message, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        if (!result.IsAccepted || result.Content is not { Approve: true } form)
        {
            return ApprovalDecision.Deny("The user declined the change.");
        }
        if (request.IsProduction &&
            !string.Equals(form.ConfirmEnvironmentName?.Trim(), request.EnvironmentName, StringComparison.Ordinal))
        {
            return ApprovalDecision.Deny("The environment name was not typed correctly, so the production change was not approved.");
        }

        return ApprovalDecision.Approve("Approved by the user in the MCP client.");
    }

    public sealed class ApprovalForm
    {
        [System.ComponentModel.Description("Approve this change")]
        public bool Approve { get; set; }

        [System.ComponentModel.Description("For production: type the environment name")]
        public string? ConfirmEnvironmentName { get; set; }
    }
}
