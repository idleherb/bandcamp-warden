FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        tini \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY patches /app/patches

# Build provenance — populated by the GHA workflow, surfaced via /healthz.
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=local
ARG BUILD_DATE=unknown
ENV WARDEN_GIT_COMMIT=$GIT_COMMIT \
    WARDEN_GIT_BRANCH=$GIT_BRANCH \
    WARDEN_BUILD_DATE=$BUILD_DATE

EXPOSE 8080

ENTRYPOINT ["tini", "--"]
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "info"]
