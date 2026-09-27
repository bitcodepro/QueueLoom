# QueueLoom

A Windows desktop app for **Azure Service Bus**. Browse queues, topics and subscriptions, find messages in dead-letter queues, and back up, delete, resend or restore them safely. AI assistants (Claude, Cursor, VS Code) can use it too, through MCP.

![QueueLoom: dead-letter search with two messages ticked for deletion](docs/images/messages-dark.png)

<table>
  <tr>
    <td><img src="docs/images/explorer-light.png" alt="Explorer"></td>
    <td><img src="docs/images/backups-dark.png" alt="Backups"></td>
    <td><img src="docs/images/composer-light.png" alt="Composer"></td>
  </tr>
  <tr>
    <td align="center">Explorer</td>
    <td align="center">Backups</td>
    <td align="center">Composer</td>
  </tr>
</table>

## Install

Download `QueueLoom-<version>-win-x64.zip` from [Releases](../../releases), unzip it and run `QueueLoom.exe`. No .NET installation is needed.

## Getting started

1. **Environments** → **Add environment**. Use a connection string or sign in with Microsoft Entra ID.
2. Choose the environment in the top bar and press **Connect**.
3. **Explorer** shows every queue, topic and subscription with live counters. Select one and press **View DLQ** or **View active**.

## What you can do

- **Find dead letters.** On **Messages / DLQ**, search by Message ID, Correlation ID, subject, body text or a property.
- **Delete only the messages you need.** Tick the found messages and press **Delete N messages…**; the others stay in the queue.
- **Empty a dead-letter queue**, with a limit on how many messages to remove.
- **Resend a message.** **Open as draft** copies it to **Composer**, where you can edit it and send it.
- **Restore from backups** or replay copies to any queue or topic.
- **Watch dead-letter counts** on **Monitors** while the app is open.

QueueLoom never deletes anything without a local backup, and it asks for confirmation first. Production environments are read-only until you press **Unlock 10 min** and type the environment name.

## Keyboard shortcuts

| Keys | Action |
|---|---|
| Ctrl+1 … Ctrl+8 | Switch pages |
| Ctrl+F | Search on the current page |
| Ctrl+R | Refresh counters |
| Ctrl+N | New message |
| Ctrl+Shift+F | Format JSON |
| Esc | Cancel the running operation |

Right-click a row to copy a name or ID. Click a column header in Explorer to sort by it.

## Connect an AI assistant (MCP)

QueueLoom can act as an [MCP](https://modelcontextprotocol.io) server, so an assistant can look into your Service Bus for you. For example, you can ask: *"Which queues in Staging have dead letters? Find the ones about order 1042 and delete them."*

**1. Copy the configuration.** In QueueLoom, open **Overview** → **AI assistants (MCP)** → **Copy configuration**. It already contains the right path to the app:

```json
{
  "mcpServers": {
    "queueloom": {
      "command": "C:\\Tools\\QueueLoom\\QueueLoom.exe",
      "args": ["--mcp"]
    }
  }
}
```

**2. Paste it into your assistant:**
- **Claude Desktop:** Settings → Developer → Edit Config, then restart Claude.
- **Cursor:** `~/.cursor/mcp.json`
- **VS Code:** `.vscode/mcp.json`, using `"servers"` instead of `"mcpServers"`.
- **Claude Code:** `claude mcp add queueloom -- "C:\Tools\QueueLoom\QueueLoom.exe" --mcp`

**What the assistant may do:**
- **Read freely:** list environments and queues, scan and search dead letters, view messages. Reading never changes anything.
- **Change only with your approval:** delete messages, empty a dead-letter queue or send a message. QueueLoom shows you this window, and nothing happens until you press **Approve**:

<img src="docs/images/mcp-approval.png" alt="QueueLoom asking to approve a deletion requested by an AI assistant" width="520">

Add `"--read-only"` to `args` if the assistant should never be able to change anything.

> Messages the assistant reads are sent to your AI provider. Use `--read-only`, or skip MCP, for data that must stay on your machine.

## Where data is stored

Settings, encrypted credentials, backups and logs are kept in `%LOCALAPPDATA%\QueueLoom`. Backups contain message bodies in plain text.

---

Build from source: `dotnet test QueueLoom.slnx` (.NET 10 SDK). · [MIT License](LICENSE) · [Third-party notices](THIRD-PARTY-NOTICES.md)
