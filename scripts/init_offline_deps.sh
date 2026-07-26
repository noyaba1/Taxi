#!/bin/bash
#
# init_offline_deps.sh  --  install the pipeline's Python deps WITHOUT PyPI
# =========================================================================
# Dataproc initialization action, run as root on every node (master + workers).
#
# WHY THIS EXISTS
# ---------------
# Google's stock `python/pip-install.sh` init action pip-installs from PyPI.
# On this project's VPC that fails:
#
#   Failed to establish a new connection: [Errno 101] Network is unreachable
#   ERROR: No matching distribution found for h3==3.7.7
#
# The cluster VMs have no route to the public internet -- no external IP and no
# Cloud NAT, which is normal for an org that restricts egress. Raising the
# init-action timeout does NOT help: pip is not slow, it is unreachable.
#
# What the VMs *can* reach is Google Cloud Storage (that is where they write
# their own logs). So we stage the wheels into the project's own bucket from a
# machine that does have internet, and install from there with `--no-index`.
# No PyPI, no compiler, no network dependency at all.
#
# Bonus: the run becomes reproducible. The exact wheel bytes used by the cluster
# are archived in the bucket next to the results.
#
# Metadata contract (set by scripts/dataproc_submit.sh):
#   WHEELHOUSE_URI   gs://<bucket>/<prefix>/wheels
#
set -euo pipefail

readonly LOCAL_DIR=/opt/porto-wheels
readonly META=/usr/share/google/get_metadata_value

log () { echo "[init_offline_deps] $*"; }

WHEELHOUSE_URI="$($META attributes/WHEELHOUSE_URI)"
if [[ -z "$WHEELHOUSE_URI" ]]; then
  echo "[init_offline_deps] FATAL: WHEELHOUSE_URI metadata is not set" >&2
  exit 1
fi

log "staging wheels from $WHEELHOUSE_URI"
mkdir -p "$LOCAL_DIR"
gsutil -q -m cp "$WHEELHOUSE_URI/*" "$LOCAL_DIR/"
log "staged: $(ls -1 "$LOCAL_DIR" | tr '\n' ' ')"

# Dataproc 2.x ships Python under Conda; `pip` on PATH is that interpreter's.
PIP="$(command -v pip3 || command -v pip)"
log "using $PIP ($("$PIP" --version))"

# --no-index      : never contact PyPI (it is unreachable; fail loudly, not slowly)
# --find-links    : resolve everything from the staged directory
# --no-deps       : the wheels' deps (numpy) are ALREADY on the image. Letting pip
#                   pull numpy 2.x would replace the 1.x that the image's pandas
#                   and pyarrow are compiled against and break every pandas_udf.
install_required () {
  log "installing (required): $*"
  "$PIP" install --no-index --find-links="$LOCAL_DIR" --no-deps "$@"
}

# python-geohash is sdist-only and needs a compile. Nothing in the CLOUD run
# imports it -- it backs the local grid-comparison sweep, whose imports are lazy
# -- so a failure here must not destroy a cluster. Best-effort, never fatal.
install_optional () {
  log "installing (optional): $*"
  if "$PIP" install --no-index --find-links="$LOCAL_DIR" --no-deps \
       --no-build-isolation "$@"; then
    log "optional install OK: $*"
  else
    log "WARNING: optional install failed: $* -- continuing (no cloud stage needs it)"
  fi
}

install_required h3 datasketches
install_optional python-geohash

# Prove the interpreter Spark will actually use can import them. Without this a
# broken install surfaces four stages into the run instead of here, after the
# cluster has already been paid for.
log "verifying imports"
PY_BIN="$(dirname "$PIP")/python3"
[[ -x "$PY_BIN" ]] || PY_BIN="$(command -v python3)"
"$PY_BIN" - <<'PY'
import sys
import h3, datasketches
print(f"[init_offline_deps] python   {sys.version.split()[0]}")
print(f"[init_offline_deps] h3       {getattr(h3, '__version__', 'ok')}")
print(f"[init_offline_deps] sketches {getattr(datasketches, '__version__', 'ok')}")
PY

log "done"
