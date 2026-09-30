#!/bin/sh
# Adds QueueLoom to the application menu of the current user (GNOME, KDE, Xfce and others).
# Run it again after moving the QueueLoom folder; run it with --remove to take the entry away.
set -e
dir=$(cd "$(dirname "$0")" && pwd)
apps="${XDG_DATA_HOME:-$HOME/.local/share}/applications"
entry="$apps/queueloom.desktop"

if [ "$1" = "--remove" ]; then
  rm -f "$entry"
  echo "Removed $entry"
  exit 0
fi

mkdir -p "$apps"
cat > "$entry" <<DESKTOP
[Desktop Entry]
Type=Application
Name=QueueLoom
GenericName=Message queue console
Comment=Browse queues and safely handle dead letters in Azure Service Bus, Amazon SQS / SNS and Google Cloud Pub/Sub
Exec="$dir/QueueLoom"
Icon=$dir/queueloom.png
Terminal=false
Categories=Development;Utility;
StartupWMClass=QueueLoom
DESKTOP
chmod +x "$entry"
command -v update-desktop-database >/dev/null 2>&1 && update-desktop-database "$apps" >/dev/null 2>&1 || true
echo "QueueLoom was added to your application menu ($entry)"
