#!/bin/sh
# chalcopyrite-serve.sh -- pitchfork wrapper: inject client keys from the keyring.
# Chalcopyrite REQUIRES CHALCOPYRITE_CLIENT_KEYS; without it parseClientKeys()
# returns an empty digest list and EVERY Bearer token is rejected with
# auth_missing (the 2026-10-15 "flock key" bug). Secrets live in the
# secretsmith keyring (SSOT); ~/.secrets is retired (deleted 2026-10-26).
set -e
# Single D-Bus key lookup, no secret echoed to stdout.
if [ -z "${CHALCOPYRITE_CLIENT_KEY:-}" ]; then
  CHALCOPYRITE_CLIENT_KEY="$(secretsmith get --label CHALCOPYRITE_CLIENT_KEY --show 2>/dev/null | sed -n 's/^secret[[:space:]]*:[[:space:]]*//p' | head -n1)"
  CHALCOPYRITE_CLIENT_KEY="${CHALCOPYRITE_CLIENT_KEY:-$(secretsmith get --label CHALCOPYRITE_CLIENT_KEY --show --json 2>/dev/null | jq -r '.secret // empty' 2>/dev/null)}"
fi
: "${CHALCOPYRITE_CLIENT_KEY:?CHALCOPYRITE_CLIENT_KEY missing from secretsmith keyring}"
export CHALCOPYRITE_CLIENT_KEYS="$CHALCOPYRITE_CLIENT_KEY"
cd /home/toxic/pens/geode/products/chalcopyrite
exec bun run src/server.ts