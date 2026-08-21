#!/usr/bin/env bash
# Installs the poller as a launchd agent that survives reboots and crashes.
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLIST="$HOME/Library/LaunchAgents/com.nepse.pipeline.plist"

mkdir -p "$HOME/Library/LaunchAgents" "$PROJECT_DIR/logs"
sed "s|__PROJECT_DIR__|$PROJECT_DIR|g" "$PROJECT_DIR/deploy/com.nepse.pipeline.plist" > "$PLIST"

launchctl bootout "gui/$(id -u)/com.nepse.pipeline" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
launchctl enable "gui/$(id -u)/com.nepse.pipeline"

echo "installed: $PLIST"
echo "status : launchctl print gui/$(id -u)/com.nepse.pipeline | head -20"
echo "logs   : tail -f $PROJECT_DIR/logs/pipeline.out.log"
echo "stop   : launchctl bootout gui/$(id -u)/com.nepse.pipeline"
