ARG PYTHON_IMAGE=python:3.13.11-slim-bookworm@sha256:ac76900038d8606cc99b413d4ede77bc7152f1e42b94cf5d50d4b80a999652fe
ARG VERSION=0.4.0

FROM ${PYTHON_IMAGE} AS builder

ARG VERSION

COPY requirements-seqevi.lock requirements-dbcan.lock /tmp/requirements/
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: \
        -r /tmp/requirements/requirements-seqevi.lock \
    && python -m venv /opt/dbcan-venv \
    && /opt/dbcan-venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: \
        -r /tmp/requirements/requirements-dbcan.lock

COPY --from=seqevi-wheel / /tmp/seqevi-wheel/
RUN wheel_path="$(find /tmp/seqevi-wheel -maxdepth 1 -name "seqevi-${VERSION}-*.whl" -print -quit)" \
    && test -n "${wheel_path}" \
    && /opt/venv/bin/pip install --no-cache-dir --no-deps "${wheel_path}" \
    && /opt/venv/bin/pip check \
    && /opt/dbcan-venv/bin/pip check

COPY inputs.json fetch_inputs.py /tmp/inputs/
RUN python /tmp/inputs/fetch_inputs.py /tmp/inputs/inputs.json /tmp/downloads \
    && mkdir -p /opt/diamond /opt/oras /usr/share/seqevi/licenses /usr/share/seqevi/compliance/sources \
    && tar -xzf /tmp/downloads/diamond-binary -C /opt/diamond \
    && install -m 0755 /opt/diamond/diamond /usr/local/bin/diamond \
    && tar -xzf /tmp/downloads/oras-binary -C /opt/oras oras LICENSE \
    && install -m 0755 /opt/oras/oras /usr/local/bin/oras \
    && install -m 0644 /opt/oras/LICENSE /usr/share/seqevi/licenses/ORAS-Apache-2.0.txt \
    && install -m 0644 /tmp/downloads/dbcan-license /usr/share/seqevi/licenses/dbCAN-GPL-3.0.txt \
    && install -m 0644 /tmp/downloads/diamond-license /usr/share/seqevi/licenses/DIAMOND-GPL-3.0.txt \
    && install -m 0644 /tmp/downloads/dbcan-source /usr/share/seqevi/compliance/sources/dbcan-source \
    && install -m 0644 /tmp/downloads/diamond-source /usr/share/seqevi/compliance/sources/diamond-source

COPY collect_python_compliance.py /tmp/collect_python_compliance.py
RUN /opt/venv/bin/python /tmp/collect_python_compliance.py /tmp/seqevi-python-compliance \
    && /opt/dbcan-venv/bin/python /tmp/collect_python_compliance.py /tmp/dbcan-python-compliance

FROM ${PYTHON_IMAGE} AS runtime

ARG REVISION
ARG VERSION
LABEL org.opencontainers.image.source="https://github.com/FuqingZh/seqevi" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="GPL-3.0-only" \
      org.opencontainers.image.base.name="docker.io/library/python:3.13.11-slim-bookworm" \
      org.opencontainers.image.base.digest="sha256:ac76900038d8606cc99b413d4ede77bc7152f1e42b94cf5d50d4b80a999652fe"

ENV PATH=/opt/venv/bin:/opt/dbcan-venv/bin:/usr/local/bin:${PATH} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /opt/dbcan-venv /opt/dbcan-venv
COPY --from=builder /usr/local/bin/diamond /usr/local/bin/diamond
COPY --from=builder /usr/local/bin/oras /usr/local/bin/oras
COPY --from=builder /usr/share/seqevi /usr/share/seqevi
COPY --from=builder /tmp/seqevi-python-compliance /usr/share/seqevi/licenses/python/seqevi-environment
COPY --from=builder /tmp/dbcan-python-compliance /usr/share/seqevi/licenses/python/dbcan-environment
COPY NOTICE SOURCE-CORRESPONDENCE.md inputs.json requirements-seqevi.lock requirements-dbcan.lock /usr/share/seqevi/compliance/
COPY --from=seqevi-wheel /LICENSE /usr/share/seqevi/licenses/SeqEvi-MIT.txt
RUN install -m 0644 /usr/local/lib/python3.13/LICENSE.txt \
        /usr/share/seqevi/licenses/CPython-3.13.11.txt

RUN install -d -m 1777 /work /tmp \
    && /opt/venv/bin/seqevi --version \
    && /opt/dbcan-venv/bin/run_dbcan version | grep -F 5.2.9 \
    && oras version | grep -F 'Version:        1.3.4' \
    && diamond version \
    && ! find / -xdev -type f \( -name CAZy.dmnd -o -name dbCAN.hmm -o -name dbCAN-sub.hmm -o -name fam-substrate-mapping.tsv \) -print -quit | grep .

USER 65532:65532
WORKDIR /work
ENTRYPOINT ["/opt/venv/bin/seqevi"]
