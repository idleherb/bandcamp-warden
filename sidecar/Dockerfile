FROM python:3.13-slim

WORKDIR /app

# Tini for signal-handling, plus runtime deps for Playwright's bundled
# browsers. Image grows from ~250MB to ~1.5GB but Plan A/B/C all
# proved Bandcamp throttles non-browser HTTP clients to a crawl
# regardless of TLS+headers; a real browser is the only path that
# bypasses their behavioural fingerprinting.
RUN apt-get update && apt-get install -y --no-install-recommends \
        tini \
        ca-certificates \
        # Chromium runtime deps
        libnss3 libnspr4 libdbus-1-3 libatk1.0-0 libatk-bridge2.0-0 \
        libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 \
        libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 \
        libasound2 \
        # Firefox runtime deps
        libdbus-glib-1-2 libxt6 libgtk-3-0 libgdk-pixbuf-2.0-0 \
        # Common
        fonts-liberation fonts-dejavu-core fonts-noto-cjk \
        xdg-utils \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers. Both chromium and firefox so we can
# rotate per run.
RUN python -m playwright install chromium firefox

COPY app.py .
COPY downloader.py .
COPY browser_downloader.py .
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
