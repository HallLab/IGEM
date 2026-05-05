# IGEM container — embedded/snapshot variant.
#
# Image bundles IGEM Server (FastAPI + GE) + IGEM Client (analysis API)
# + scientific Python stack. At runtime the user bind-mounts a Parquet
# snapshot at /snapshot and a workspace at /work; the entrypoint sets
# IGEM_URL=embedded:///snapshot, so any `from igem import IGEM` call
# transparently mounts the FastAPI app in-process and reads the snapshot
# read-only. Zero network, zero PostgreSQL.
#
# Specifically targeted at HPC / SLURM-style batch jobs. For users that
# want to talk to a remote IGEM-Server over HTTP, a separate
# Dockerfile.client (without the bundled server) makes more sense.
#
# Build (from repo root):
#     docker build -f docker/Dockerfile -t igem:0.1.0 .
#
# Run a one-off analysis:
#     docker run --rm \
#         -v /local/snapshots/2026-04-25:/snapshot:ro \
#         -v "$PWD":/work \
#         igem:0.1.0 \
#         python analysis.py
#
# See docker/README.md for SLURM / Singularity examples.

FROM python:3.12-slim AS runtime

# Build-time version info — propagated into image labels for traceability.
# CI sets these from git tags; local builds default to "dev".
ARG IGEM_CONTAINER_VERSION=dev
ARG IGEM_CLIENT_VERSION=dev
ARG IGEM_SERVER_VERSION=dev

LABEL org.opencontainers.image.title="IGEM"
LABEL org.opencontainers.image.description="IGEM HPC/snapshot container — bundled client + server"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.version="${IGEM_CONTAINER_VERSION}"
LABEL igem.client_version="${IGEM_CLIENT_VERSION}"
LABEL igem.server_version="${IGEM_SERVER_VERSION}"

# System libs for: pyarrow (libstdc++), numpy/scipy/sgkit (BLAS),
# bed-reader (build), psycopg2-binary (libpq even though we never
# connect to PG in embedded mode — duckdb-engine still imports it
# transitively in some paths).
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user — HPC clusters typically refuse root inside containers
# (Singularity maps the calling UID); creating a fixed UID/GID matches
# common HPC conventions and avoids file-ownership surprises in /work.
ARG USER_UID=1000
ARG USER_GID=1000
RUN groupadd -g ${USER_GID} igem && \
    useradd -u ${USER_UID} -g ${USER_GID} -m -s /bin/bash igem

# Install the IGEM packages. We copy pyproject + sources for both,
# then install via pip — this is simpler than carrying poetry into
# the runtime image.
WORKDIR /opt/igem

# Note: we don't COPY poetry.lock — it's gitignored AND `pip install`
# doesn't use it. If you decide to commit lock files for stricter
# reproducibility, add `backend/poetry.lock` and `client/poetry.lock`
# to the COPY lines.
COPY backend/pyproject.toml ./backend/
COPY backend/src ./backend/src

COPY client/pyproject.toml ./client/
COPY client/src ./client/src
COPY client/README.md ./client/

# Backend first; client embedded extra resolves to a path-installed
# backend that's already in the image (poetry path dep is not honoured
# by pip, so we install both explicitly here).
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir ./backend && \
    pip install --no-cache-dir ./client

# Entrypoint validates the snapshot bind-mount and sets IGEM_URL.
COPY docker/entrypoint.sh /usr/local/bin/igem-entrypoint
RUN chmod +x /usr/local/bin/igem-entrypoint && \
    chown -R igem:igem /opt/igem

# Default mount points + env
ENV IGEM_URL=embedded:///snapshot \
    SNAPSHOT_DIR=/snapshot \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Drop privileges
USER igem

# /work is the user's workspace; the user is expected to bind-mount it.
WORKDIR /work

ENTRYPOINT ["igem-entrypoint"]
# Default to an interactive Python REPL when no command is given.
CMD ["python"]
