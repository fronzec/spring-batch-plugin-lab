#!/usr/bin/env bash

set -Eeuo pipefail

container_id="$(podman ps -q \
  --filter label=com.docker.compose.project=frbatch-local \
  --filter label=com.docker.compose.service=db \
  | head -n 1)"

if [[ -z "$container_id" ]]; then
  exit 1
fi

[[ "$(podman inspect --format='{{.State.Health.Status}}' "$container_id")" == "healthy" ]]
