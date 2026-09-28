# syntax=docker/dockerfile:1
# Build from this repository with:
# docker buildx build --build-context shared=../littledevil-shared --tag littledevil-recorder:local --load .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LITTLEDEVIL_DATA_ROOT=/data

WORKDIR /opt/littledevil

# Use the checked-out shared package instead of the Git URL declared by the
# recorder's package metadata. The named build context points at the sibling
# littledevil-shared checkout.
COPY --from=shared /pyproject.toml /tmp/build/shared/pyproject.toml
COPY --from=shared /src/ /tmp/build/shared/src/
COPY pyproject.toml /tmp/build/recorder/pyproject.toml
COPY src/ /tmp/build/recorder/src/

RUN python -m pip install --no-cache-dir /tmp/build/shared \
    && python -m pip install --no-cache-dir \
        "httpx>=0.27" \
        "websockets>=13.0" \
        "pyarrow>=17.0" \
        "pydantic>=2.8" \
        "psycopg[binary]>=3.2" \
    && python -m pip install --no-cache-dir --no-deps /tmp/build/recorder \
    && groupadd --system --gid 10001 recorder \
    && useradd --system --uid 10001 --gid 10001 --no-create-home recorder \
    && mkdir -p /data \
    && chown 10001:10001 /data \
    && rm -rf /tmp/build

USER 10001:10001
VOLUME ["/data"]

CMD ["littledevil-recorder"]
