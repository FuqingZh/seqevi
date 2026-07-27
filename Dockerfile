ARG PYTHON_IMAGE=python:3.13-slim

FROM ${PYTHON_IMAGE} AS builder

ARG PDM_VERSION=2.26.9
ENV PDM_CHECK_UPDATE=false \
    PDM_VENV_IN_PROJECT=true

WORKDIR /opt/seqevi

RUN python -m pip install --no-cache-dir "pdm==${PDM_VERSION}"

COPY pyproject.toml pdm.lock README.md LICENSE ./
COPY src ./src

RUN pdm install --frozen-lockfile --prod -G server --no-editable \
    && .venv/bin/python -c "import fastapi, psycopg, seqevi"

FROM ${PYTHON_IMAGE} AS service

ENV PATH=/opt/seqevi/.venv/bin:${PATH} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SEQEVI_HEALTH_URL=http://127.0.0.1:8000/health

RUN groupadd --system --gid 10001 seqevi \
    && useradd --system --uid 10001 --gid seqevi \
        --home-dir /var/lib/seqevi --create-home seqevi \
    && chmod 0755 /var/lib/seqevi \
    && install -d -o seqevi -g seqevi /var/lib/seqevi/artifacts

COPY --from=builder /opt/seqevi/.venv /opt/seqevi/.venv

USER seqevi
WORKDIR /var/lib/seqevi

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen(os.environ['SEQEVI_HEALTH_URL'], timeout=3).read()"]

ENTRYPOINT ["seqevi"]
CMD ["serve"]
