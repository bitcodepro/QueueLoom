using System.Text.Json;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

/// <summary>Starts the real <c>QueueLoom --mcp</c> process and talks to it over stdio, like an LLM client does.</summary>
public sealed class McpProcessTests
{
    [Fact]
    public async Task ReadOnlyServer_StartsOverStdioAndListsSavedEnvironments()
    {
        using var data = new TemporaryDirectory();
        using (var profiles = new JsonProfileRepository(QueueLoomPaths.ForRoot(data.Path)))
        {
            await profiles.UpsertAsync(new ServiceBusProfile(
                Guid.NewGuid(), "Staging", EnvironmentKind.Test, null, "staging.servicebus.windows.net",
                AuthenticationSettings.Entra(), ProfileAccessMode.ReadOnly));
        }

        await using var client = await StartAsync(data.Path, "--mcp", "--read-only");

        Assert.Equal("QueueLoom", client.ServerInfo.Name);
        var tools = await client.ListToolsAsync();
        Assert.All(tools, tool => Assert.True(tool.ProtocolTool.Annotations?.ReadOnlyHint));
        var result = await client.CallToolAsync("list_environments", new Dictionary<string, object?>());
        var environments = JsonDocument.Parse(string.Concat(result.Content.OfType<TextContentBlock>().Select(block => block.Text)))
            .RootElement;
        Assert.Equal("Staging", environments[0].GetProperty("name").GetString());
        Assert.Equal("Test", environments[0].GetProperty("kind").GetString());
    }

    [Fact]
    public async Task ServerWithoutADesktop_OffersChangeToolsButCannotApproveThemSilently()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsMacOS())
        {
            return; // These always have a desktop session, where approvals use the QueueLoom window.
        }

        using var data = new TemporaryDirectory();
        using (var profiles = new JsonProfileRepository(QueueLoomPaths.ForRoot(data.Path)))
        {
            await profiles.UpsertAsync(new ServiceBusProfile(
                Guid.NewGuid(), "Dev", EnvironmentKind.Development, null, "dev.servicebus.windows.net",
                AuthenticationSettings.Entra(), ProfileAccessMode.ReadWrite));
        }

        await using var client = await StartAsync(data.Path, "--mcp");

        var tools = await client.ListToolsAsync();
        Assert.Contains(tools, tool => tool.Name == "delete_dead_letter_messages");
    }

    private static async Task<McpClient> StartAsync(string dataDirectory, params string[] arguments)
    {
        var app = Path.Combine(AppContext.BaseDirectory, "QueueLoom.dll");
        Assert.True(File.Exists(app), $"The app was not copied to {app}.");
        var transport = new StdioClientTransport(new StdioClientTransportOptions
        {
            Name = "QueueLoom",
            Command = "dotnet",
            Arguments = [app, .. arguments],
            EnvironmentVariables = new Dictionary<string, string?>
            {
                ["QUEUELOOM_DATA_DIRECTORY"] = dataDirectory,
                ["DISPLAY"] = null,
                ["WAYLAND_DISPLAY"] = null
            }
        });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        return await McpClient.CreateAsync(transport, cancellationToken: timeout.Token);
    }
}
