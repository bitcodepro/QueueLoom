using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Mcp;
using static QueueLoom.Tests.ViewModelStateTests;

namespace QueueLoom.Tests;

public sealed class DeepAuditIdentityTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReplayBindsProviderConfigurationAndRevision(bool edited)
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            var profile = CreateProfile("Test", EnvironmentKind.Test);
            var workspace = new FakeWorkspace();
            var store = new BatchReplayStore(root);
            var plan = await store.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("q"), [(MessageDraft.Empty, "test")], false, 50, default,
                profile.EndpointDisplay, ScheduledResend.IdentityFor(profile));
            await workspace.ConnectAsync(edited ? profile with { ConfigurationRevision = Guid.NewGuid() } : profile);
            if (edited)
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunAsync(plan, workspace, () => true, null, default));
                Assert.Empty(workspace.SentMessages);
            }
            else
            {
                Assert.Equal(1, (await store.RunAsync(plan, workspace, () => true, null, default)).Sent);
                Assert.Equal(1, (await new BatchReplayStore(root).RunAsync(plan, workspace, () => true, null, default)).Sent);
                Assert.Single(workspace.SentMessages);
            }
        }
        finally { if (Directory.Exists(root)) Directory.Delete(root, true); }
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task McpReconnectsAfterEditingTheSameProfile(bool write)
    {
        var first = CreateProfile("Development", EnvironmentKind.Development);
        var repository = new FakeProfileRepository([first], first.Id);
        var workspace = new FakeWorkspace();
        using var session = new McpWorkspaceSession(repository, workspace, NullLogger<McpWorkspaceSession>.Instance);
        await session.ReadAsync(first, (_, _) => Task.FromResult(0), default);
        var edited = first with { FullyQualifiedNamespace = "other.servicebus.windows.net", ConfigurationRevision = Guid.NewGuid() };
        await repository.UpsertAsync(edited);
        if (write) await session.WriteAsync(edited, (_, _) => Task.FromResult(0), default);
        else await session.ReadAsync(edited, (_, _) => Task.FromResult(0), default);
        Assert.Equal(2, workspace.ConnectCalls);
        Assert.Equal(ProfileAccessMode.ReadOnly, workspace.ConnectedAccessMode);
    }

    [Fact]
    public async Task McpRejectsAProfileEditedWhileApprovalIsPending()
    {
        var approved = CreateProfile("Development", EnvironmentKind.Development);
        var repository = new FakeProfileRepository([approved], approved.Id);
        var workspace = new FakeWorkspace();
        using var session = new McpWorkspaceSession(repository, workspace, NullLogger<McpWorkspaceSession>.Instance);
        await session.ReadAsync(approved, (_, _) => Task.FromResult(0), default);
        await repository.UpsertAsync(approved with { ConfigurationRevision = Guid.NewGuid() });
        var writes = 0;
        await Assert.ThrowsAsync<McpException>(() => session.WriteAsync(approved, (_, _) => Task.FromResult(++writes), default));
        Assert.Equal(0, writes);
        Assert.DoesNotContain(ProfileAccessMode.ReadWrite, workspace.AccessModeChanges);
    }

    [Fact]
    public async Task ReplayBlocksCaseSensitiveEndpointChangesBeforeSending()
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        try
        {
            var profile = CreateProfile("Rabbit", EnvironmentKind.Development) with
            {
                Provider = MessagingProvider.RabbitMq,
                RabbitMq = new RabbitMqSettings("localhost", "guest", "orders")
            };
            var workspace = new FakeWorkspace();
            await workspace.ConnectAsync(profile);
            var store = new BatchReplayStore(root);
            var draft = new MessageDraft(EditableMessageBody.Empty, EditableMessageProperties.Empty, []);
            var plan = await store.CreateAsync(profile.Id, ServiceBusEntityReference.Queue("q"), [(draft, "test")], false, 50, default,
                profile.EndpointDisplay!.Replace("orders", "Orders", StringComparison.Ordinal));
            await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunAsync(plan, workspace, () => true, null, default));
            Assert.Empty(workspace.SentMessages);
        }
        finally { if (Directory.Exists(root)) Directory.Delete(root, true); }
    }
}
