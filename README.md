# QueueLoom

A Windows desktop app for message queues in **Azure Service Bus**, **Amazon SQS / SNS** and **Google Cloud Pub/Sub**. Browse queues, topics and subscriptions, find messages in dead-letter queues, and back up, delete, resend or restore them safely. AI assistants (Claude, Cursor, VS Code) can use it too, through MCP.

![QueueLoom: dead-letter search with two messages ticked for deletion](docs/images/messages-dark.png)

<table>
  <tr>
    <td><img src="docs/images/environments-dark.png" alt="Environments in three clouds"></td>
    <td><img src="docs/images/add-environment-light.png" alt="Adding an AWS environment"></td>
    <td><img src="docs/images/aws-explorer-dark.png" alt="Explorer with SQS queues and SNS topics"></td>
  </tr>
  <tr>
    <td align="center">Environments in three clouds</td>
    <td align="center">Add environment</td>
    <td align="center">SQS queues and SNS topics</td>
  </tr>
</table>

## Install

Download `QueueLoom-<version>-win-x64.zip` from [Releases](../../releases), unzip it and run `QueueLoom.exe`. No .NET installation is needed.

## Getting started

1. **Environments** → **Add environment**. Pick the service and sign in:
   - **Azure Service Bus**: Microsoft Entra ID or a connection string.
   - **Amazon SQS / SNS**: a region and an access key, or your AWS profile.
   - **Google Cloud Pub/Sub**: a project ID and a service account key, or `gcloud` default credentials.
2. Choose the environment in the top bar and press **Connect**.
3. **Explorer** shows every queue, topic and subscription with live counters. Select one and press **View DLQ** or **View active**.

The badge next to each environment (**AZURE**, **AWS**, **GCP**) shows its cloud, and the coloured dot shows its stage: dev, test or prod. The connected environment's cloud and namespace, region or project are always visible in the top bar.

## What you can do

- **Find dead letters.** On **Messages / DLQ**, search by Message ID, Correlation ID, subject, body text or a property.
- **Delete only the messages you need.** Tick the found messages and press **Delete N messages…**; the others stay in the queue.
- **Empty a dead-letter queue**, with a limit on how many messages to remove.
- **Resend a message.** **Open as draft** copies it to **Composer**, where you can edit it and send it.
- **Restore from backups** or replay copies to any queue or topic.
- **Watch dead-letter counts** on **Monitors** while the app is open.

QueueLoom never deletes anything without a local backup, and it asks for confirmation first. Production environments are read-only until you press **Unlock 10 min** and type the environment name.

## How AWS and Google Cloud look in QueueLoom

| | Amazon SQS / SNS | Google Cloud Pub/Sub |
|---|---|---|
| Queues | SQS queues | none |
| Topics and subscriptions | SNS topics and their subscriptions | topics and subscriptions |
| Dead-letter queue | the queue in the redrive policy | a subscription on the dead-letter topic |
| Counters | approximate, from SQS | not reported by Pub/Sub; dead letters are counted when you scan |

SQS and Pub/Sub cannot peek. To show messages, QueueLoom receives them and hands them back unchanged a moment later. This counts as one more receive (SQS) or delivery attempt (Pub/Sub), and in SQS FIFO queues it shows up to 10 messages per message group. For local testing, set the endpoint to [LocalStack](https://localstack.cloud) (`http://localhost:4566`) or the Pub/Sub emulator (`localhost:8085`).

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

QueueLoom can act as an [MCP](https://modelcontextprotocol.io) server, so an assistant can look into your queues for you. For example, you can ask: *"Which queues in Staging have dead letters? Find the ones about order 1042 and delete them."*

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
- **Read freely:** list environments and queues, scan and search dead letters, view messages. Reading never removes anything.
- **Change only with your approval:** delete messages, empty a dead-letter queue or send a message. QueueLoom shows you this window, and nothing happens until you press **Approve**:

<img src="docs/images/mcp-approval.png" alt="QueueLoom asking to approve a deletion requested by an AI assistant" width="520">

Add `"--read-only"` to `args` if the assistant should never be able to change anything.

> Messages the assistant reads are sent to your AI provider. Use `--read-only`, or skip MCP, for data that must stay on your machine.

## Where data is stored

Settings, encrypted credentials, backups and logs are kept in `%LOCALAPPDATA%\QueueLoom`. Backups contain message bodies in plain text.

---

Build from source: `dotnet test QueueLoom.slnx` (.NET 10 SDK). Tests against LocalStack and the Pub/Sub emulator run when `QUEUELOOM_LOCALSTACK_URL` and `QUEUELOOM_PUBSUB_EMULATOR` are set. · [MIT License](LICENSE) · [Third-party notices](THIRD-PARTY-NOTICES.md)
