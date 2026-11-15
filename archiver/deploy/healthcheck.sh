#!/usr/bin/env bash
# Alerts when `archiver status` reports a problem. Installed as
# /usr/local/bin/archiver-healthcheck and run every 5 minutes by archiver-health.timer.
# Replace the body of `notify` with whatever reaches you: mail, ntfy, a webhook...
#   ARCHIVER_CONFIG  config file (default /etc/archiver/config.toml)
set -uo pipefail

config="${ARCHIVER_CONFIG:-/etc/archiver/config.toml}"

notify() {
    logger -t archiver-health "$1"
    # curl -fsS -m 10 -d "$1" https://ntfy.sh/<your-topic> >/dev/null
}

if ! output=$(/usr/local/bin/archiver status --config "$config" 2>&1); then
    problems=$(grep UNHEALTHY <<<"$output" | tr '\n' ' ')
    notify "archiver unhealthy: ${problems:-$output}"
    exit 1
fi
