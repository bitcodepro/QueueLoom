# Local Service Bus lab

Requires running Docker Desktop in Linux-container mode and .NET SDK 10. The lab uses official Microsoft Service Bus emulator and SQL Server 2022 images. Running Start-Lab.ps1 accepts their EULAs through ACCEPT_EULA=Y. SQL credentials are generated locally in ignored `.env`; no cloud credentials are used. Ports 5672 and 5300 bind only to 127.0.0.1. Other Compose projects are unaffected.

From the QueueLoom repository directory:

```powershell
./dev/emulator/Start-Lab.ps1
Invoke-RestMethod http://localhost:5300/health
dotnet build dev/QueueLoom.Lab/QueueLoom.Lab.csproj -c Release
dotnet dev/QueueLoom.Lab/bin/Release/net10.0/QueueLoom.Lab.dll seed
dotnet dev/QueueLoom.Lab/bin/Release/net10.0/QueueLoom.Lab.dll verify
dotnet dev/QueueLoom.Lab/bin/Release/net10.0/QueueLoom.Lab.dll roundtrip
dotnet dev/QueueLoom.Lab/bin/Release/net10.0/QueueLoom.Lab.dll install-profile
```

Allow about 1–2 minutes for first initialization after image download. `install-profile` adds/selects the synthetic **Local emulator** Test profile in the current OS user's QueueLoom settings; it preserves other profiles. Start QueueLoom under the same OS user. Click Connect, Explorer, select ql-orders or ql-events/billing, then View active / View DLQ. Scan current on Messages also discovers local DLQs.

Data connection (public emulator placeholder, not a real secret):

```text
Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
```

QueueLoom automatically sends administration requests to port 5300 for this connection. The profile editor can override that port. The original AMQP endpoint is retained for messages. Normal Azure connections are unchanged.

## Synthetic scenarios

- `seed`: sends 12 messages to ql-orders and 12 to ql-events. The queue consumer explicitly dead-letters four; billing dead-letters three; analytics completes five. Active messages remain for Peek. Repeating seed adds another batch.
- `verify`: connects through QueueLoom's actual workspace implementation, discovers topology, peeks active/DLQ messages and checks reason mapping, scans and searches DLQ bodies.
- `roundtrip`: verifies first, then backs up and deletes **at most two ql-orders DLQ messages**, restores copies to ql-replay, verifies full bodies and checks that reopening/resuming the completed batch sends no duplicates. Run seed first if fewer than two order DLQs remain.
- `install-profile`: installs only the local test connection; never accepts cloud connection strings.

Lab backups and replay checkpoints are under `dev/QueueLoom.Lab/bin/Release/net10.0/lab-data`. Active messages have a one-hour TTL. Restarting the emulator does not preserve messages; run seed again. The lab intentionally remains running for interactive inspection.

To stop only this project:

```powershell
docker compose -f dev/emulator/compose.yaml stop
```

## Observed emulator differences

The tested image reports zero management counters despite existing messages. QueueLoom therefore uses bounded non-locking Peek samples for local emulator counts (1,000 messages per source, 25 per request); this transfers bodies. The UI labels the sample and unavailable counters. These are not production metrics.

Transfer DLQ Peek returned an emulator GeneralError for a subscription, so the local UI disables transfer DLQ and excludes it from local scanning/search. Normal Azure transfer-DLQ support remains enabled. Session browsing and partitioned/large-message scenarios are outside this lab's coverage.

Official setup: https://learn.microsoft.com/en-us/azure/service-bus-messaging/test-locally-with-service-bus-emulator

Official emulator differences: https://learn.microsoft.com/en-us/azure/service-bus-messaging/overview-emulator
