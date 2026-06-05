#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# Download a Gardener shoot kubeconfig (or its control-plane / seed-side
# kubeconfig) using gardenctl and write it under operator/scenarios/.
#
# Usage:
#   get-kubeconfig.sh [shoot|cp|both]
#
#   shoot  download the shoot (user-facing) kubeconfig         [default]
#   cp     download the shoot control-plane (seed-side) kubeconfig
#   both   download both
#
# Inputs (env vars, override on the CLI):
#   GARDEN, PROJECT, SHOOT       required, no defaults
#   OUT_DIR                      directory for the output files
#                                (default: <repo>/operator/scenarios)
#   KUBECONFIG_SHOOT_FILE        default: ${OUT_DIR}/kubeconfig.yaml
#   KUBECONFIG_CP_FILE           default: ${OUT_DIR}/kubeconfig-cp.yaml
#   GARDENCTL_KUBECONFIG_FLAGS   default: --flatten --raw
#                                (--flatten inlines cert data; --raw skips the
#                                 exec-credential-plugin callback into gardenctl)
#   GCTL_SESSION_ID              default: $TERM_SESSION_ID if it matches
#                                gardenctl's allowed pattern ([A-Za-z0-9_-]{1,128}),
#                                otherwise a stable fallback string
#
# Requires `gardenctl` (https://github.com/gardener/gardenctl-v2) on PATH and
# a working garden login (e.g. via `gardenctl rc` / `gardenlogin`).

set -o errexit
set -o nounset
set -o pipefail

mode="${1:-shoot}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scenarios_dir="$(cd "${script_dir}/.." && pwd)"

: "${OUT_DIR:=${scenarios_dir}}"
: "${KUBECONFIG_SHOOT_FILE:=${OUT_DIR}/kubeconfig.yaml}"
: "${KUBECONFIG_CP_FILE:=${OUT_DIR}/kubeconfig-cp.yaml}"
: "${GARDENCTL_KUBECONFIG_FLAGS:=--flatten --raw}"

# gardenctl requires GCTL_SESSION_ID to be alnum/underscore/dash, 1-128 chars.
# macOS Terminal.app sets TERM_SESSION_ID to a value containing ':' and '/',
# which gardenctl rejects, so only adopt it if it matches the allowed pattern.
if [[ -z "${GCTL_SESSION_ID:-}" ]]; then
  if [[ "${TERM_SESSION_ID:-}" =~ ^[A-Za-z0-9_-]{1,128}$ ]]; then
    GCTL_SESSION_ID="${TERM_SESSION_ID}"
  else
    GCTL_SESSION_ID="scaling-advisor-script"
  fi
fi
export GCTL_SESSION_ID

GARDEN="${GARDEN:-}"
PROJECT="${PROJECT:-}"
SHOOT="${SHOOT:-}"

if [[ -z "${GARDEN}" || -z "${PROJECT}" || -z "${SHOOT}" ]]; then
  echo "ERROR: GARDEN, PROJECT and SHOOT must be set." >&2
  echo "Usage: GARDEN=<g> PROJECT=<p> SHOOT=<s> $0 [shoot|cp|both]" >&2
  exit 1
fi

if ! command -v gardenctl >/dev/null 2>&1; then
  echo "ERROR: gardenctl not found on PATH. See https://github.com/gardener/gardenctl-v2" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"

# fetch <out_file> [--control-plane]
fetch() {
  local out_file="$1"
  shift
  local kind="shoot"
  if [[ "${1:-}" == "--control-plane" ]]; then
    kind="shoot control-plane"
  fi

  echo "Downloading ${kind} kubeconfig: garden=${GARDEN} project=${PROJECT} shoot=${SHOOT} -> ${out_file}"
  # shellcheck disable=SC2086 # GARDENCTL_KUBECONFIG_FLAGS is an intentional flag list
  gardenctl kubeconfig "$@" \
    --garden "${GARDEN}" --project "${PROJECT}" --shoot "${SHOOT}" \
    ${GARDENCTL_KUBECONFIG_FLAGS} > "${out_file}"
  chmod 600 "${out_file}"
}

case "${mode}" in
  shoot)
    fetch "${KUBECONFIG_SHOOT_FILE}"
    ;;
  cp|control-plane)
    fetch "${KUBECONFIG_CP_FILE}" --control-plane
    ;;
  both)
    fetch "${KUBECONFIG_SHOOT_FILE}"
    fetch "${KUBECONFIG_CP_FILE}" --control-plane
    ;;
  *)
    echo "ERROR: unknown mode '${mode}'. Expected: shoot | cp | both" >&2
    exit 1
    ;;
esac
