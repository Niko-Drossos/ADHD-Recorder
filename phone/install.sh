#!/data/data/com.termux/files/usr/bin/bash
# One-time setup of the recall recorder in Termux. Safe to re-run.
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
config_dir="$HOME/.config/recall"
config="$config_dir/config"

echo "Installing packages…"
pkg install -y termux-api curl jq >/dev/null

# Keep existing answers as defaults when re-running.
RECALL_SERVER="http://100.82.32.109:8888"; RECALL_TOKEN=""; RECALL_DEVICE="phone"
RECALL_CHUNK_SECONDS=300; RECALL_SPEAK=0
[ -f "$config" ] && . "$config"

read -rp "Server URL [$RECALL_SERVER]: " ans; RECALL_SERVER="${ans:-$RECALL_SERVER}"
RECALL_SERVER="${RECALL_SERVER%/}"
if [ -n "$RECALL_TOKEN" ]; then
  read -rsp "INGEST_TOKEN [keep current]: " ans; echo
else
  read -rsp "INGEST_TOKEN (from the server's .env): " ans; echo
fi
RECALL_TOKEN="${ans:-$RECALL_TOKEN}"
read -rp "Device name [$RECALL_DEVICE]: " ans; RECALL_DEVICE="${ans:-$RECALL_DEVICE}"
read -rp "Read summaries aloud? (y/n) [$([ "$RECALL_SPEAK" = 1 ] && echo y || echo n)]: " ans
case "$ans" in y|Y) RECALL_SPEAK=1 ;; n|N) RECALL_SPEAK=0 ;; esac

mkdir -p "$config_dir"
( umask 077
  {
    printf 'RECALL_SERVER=%q\n' "$RECALL_SERVER"
    printf 'RECALL_TOKEN=%q\n' "$RECALL_TOKEN"
    printf 'RECALL_DEVICE=%q\n' "$RECALL_DEVICE"
    printf 'RECALL_CHUNK_SECONDS=%q\n' "$RECALL_CHUNK_SECONDS"
    printf 'RECALL_SPEAK=%q\n' "$RECALL_SPEAK"
  } > "$config" )

install -m 755 "$here/recall" "$PREFIX/bin/recall"

# Termux:Widget: scripts in ~/.shortcuts/tasks run in the background
# without opening a terminal window.
widgets="$HOME/.shortcuts/tasks"
mkdir -p "$widgets"
mkwidget() { printf '#!%s/bin/bash\n%s/bin/recall %s\n' "$PREFIX" "$PREFIX" "$2" > "$widgets/$1"; chmod 755 "$widgets/$1"; }
mkwidget "Recall 10 min" "summary 10"
mkwidget "Recall 30 min" "summary 30"
mkwidget "Pause 1h"      "pause 60"
mkwidget "Resume"        "resume"
mkwidget "Start recorder" "start"
mkwidget "Stop recorder"  "stop"

# Termux:Boot: start recording when the phone boots.
mkdir -p "$HOME/.termux/boot"
printf '#!%s/bin/bash\ntermux-wake-lock\n%s/bin/recall start\n' "$PREFIX" "$PREFIX" \
  > "$HOME/.termux/boot/recall"
chmod 755 "$HOME/.termux/boot/recall"

echo
echo "Checking the server…"
if curl -fsS --max-time 10 "$RECALL_SERVER/health" >/dev/null; then
  if curl -fsS --max-time 10 -H "Authorization: Bearer $RECALL_TOKEN" "$RECALL_SERVER/pause" >/dev/null; then
    echo "  Server reachable, token accepted."
  else
    echo "  Server reachable but the TOKEN WAS REJECTED. Re-run install.sh."
  fi
else
  echo "  Can't reach $RECALL_SERVER. Is Tailscale on?"
fi

cat <<'EOF'

Installed. Next:
  1. Run:  recall test     (Android asks for mic permission the first time)
  2. If the transcript matches what you said:  recall start
  3. Open the Termux:Boot app once so it can start recall on boot.
  4. Add the Termux:Widget widget to your home screen for the buttons.
  5. Settings > Apps > Termux and Termux:API > Battery > Unrestricted.
EOF
