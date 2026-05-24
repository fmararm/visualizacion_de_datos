#!/bin/bash
REPORT_DIR="$(cd "$(dirname "$0")" && pwd)"
TARGET="$REPORT_DIR/memtfg.tex"
LAST=$(stat -c %Y "$TARGET")

echo "[watch] Watching $TARGET. Ctrl+C to stop."

while true; do
    sleep 2
    NOW=$(stat -c %Y "$TARGET")
    if [ "$NOW" != "$LAST" ]; then
        LAST=$NOW
        echo "[watch] Cambio detectado a $(date '+%H:%M:%S'). Recompilando..."
        cd "$REPORT_DIR" && bash compile.sh
        echo "[watch] Listo."
    fi
done
