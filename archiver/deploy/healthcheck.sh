#!/usr/bin/env bash
# Alerts when `archiver status` reports a problem. Run every few minutes from cron, e.g.
#   */5 * * * * archiver /usr/local/bin/healthcheck.sh
# Replace `notify` with whatever reaches you (mail, ntfy, a webhook...).
set -uo pipefail

notify() {
    logger -t archiver-health "$1"
}

if ! output=$(/usr/local/bin/archiver status --config /etc/archiver/config.toml 2>&1); then
    notify "archiver unhealthy: $(grep UNHEALTHY <<<"$output" | tr '\n' ' ')"
    exit 1
fi
