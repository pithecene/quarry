# syntax=docker/dockerfile:1

# =============================================================================
# Stage 1: build — compile Go binary with embedded executor bundle
# =============================================================================
FROM node:24.13.0-bookworm-slim AS build

# Pin Go version to match go.mod
ARG GO_VERSION=1.25.6
# TARGETARCH is injected by BuildKit for multi-platform builds (e.g. amd64, arm64).
ARG TARGETARCH
# Commit SHA for version embedding (.git is excluded from Docker context).
ARG COMMIT=unknown

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git \
  && rm -rf /var/lib/apt/lists/*

# Install Go from official tarball
RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz" \
  | tar -C /usr/local -xz
ENV PATH="/usr/local/go/bin:${PATH}"

# Enable corepack for pnpm
RUN corepack enable

WORKDIR /src

# Layer 1: dependency manifests (cacheable)
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY sdk/package.json sdk/
COPY executor-node/package.json executor-node/

# Build stage only compiles TS/Go — skip Puppeteer's browser download.
ENV PUPPETEER_SKIP_DOWNLOAD=true

RUN pnpm install --frozen-lockfile

# Layer 2: Go module download (cacheable separately from source)
COPY quarry/go.mod quarry/go.sum quarry/
RUN --mount=type=cache,target=/go/pkg/mod \
    cd quarry && go mod download

# Layer 3: source code
COPY sdk/ sdk/
COPY executor-node/ executor-node/
COPY quarry/ quarry/

# Build TS packages → bundle executor → compile Go binary
RUN pnpm -C sdk run build \
  && pnpm -C executor-node run build \
  && pnpm -C executor-node run bundle

WORKDIR /src/quarry
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -ldflags "-X main.commit=${COMMIT}" -o /usr/local/bin/quarry ./cmd/quarry

# =============================================================================
# Stage 2: deps — install runtime Node.js dependencies
# =============================================================================
FROM node:24.13.0-bookworm-slim AS deps

WORKDIR /opt/quarry

# Install puppeteer ecosystem without downloading bundled Chromium.
# The full image uses system Chromium; the slim image has no browser.
ENV PUPPETEER_SKIP_DOWNLOAD=true

# Pin exact versions matching pnpm-lock.yaml for reproducible builds.
# npm resolves this tree independently of pnpm-lock.yaml, so the security
# override for basic-ftp (transitive via @puppeteer/browsers → proxy-agent)
# must be restated here as an npm root override.
RUN npm init -y >/dev/null \
  && npm pkg set 'overrides.basic-ftp=6.2.3' \
  && npm install --omit=dev \
  puppeteer@25.14.0 \
  puppeteer-extra@3.3.6 \
  puppeteer-extra-plugin-stealth@2.11.2 \
  puppeteer-extra-plugin-adblocker@2.13.6

# =============================================================================
# Stage 3: slim — runtime image without Chromium
# =============================================================================
FROM node:24.13.0-bookworm-slim AS slim

# Inherited by the full stage. The source label links the GHCR package to
# this repository.
LABEL org.opencontainers.image.title="quarry" \
      org.opencontainers.image.description="Web extraction runtime for browser-driven crawling and durable ingestion" \
      org.opencontainers.image.source="https://github.com/pithecene/quarry" \
      org.opencontainers.image.url="https://github.com/pithecene/quarry" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="Pithecene" \
      org.opencontainers.image.authors="contact@pithecene.sh"

RUN groupadd -r quarry && useradd -r -g quarry -m quarry

COPY --from=build /usr/local/bin/quarry /usr/local/bin/quarry
COPY --from=deps /opt/quarry/node_modules /opt/quarry/node_modules

# Let Node resolve puppeteer and friends from the shared location.
# User scripts that ship their own node_modules resolve those first.
ENV NODE_PATH=/opt/quarry/node_modules

# Chromium cannot use its sandbox inside containers without extra capabilities.
ENV QUARRY_NO_SANDBOX=1

USER quarry
WORKDIR /work

ENTRYPOINT ["quarry"]

# =============================================================================
# Stage 4: full — runtime image with Chrome for Testing + fonts (amd64 only)
#
# Chrome for Testing does not publish linux-arm64 builds.
# arm64 users needing a browser should install Chromium into the slim image.
# =============================================================================
FROM slim AS full

USER root

# Chrome for Testing only publishes linux-amd64 binaries.
# Fail fast so builders don't get a silent wrong-arch image.
ARG TARGETARCH
RUN if [ "$TARGETARCH" != "amd64" ]; then \
      echo "ERROR: full image requires amd64 (got ${TARGETARCH:-unknown}). Use the slim image on other architectures." >&2; \
      exit 1; \
    fi

# Pin Chrome for Testing version matching puppeteer@25.14.0 (see deps stage).
# Look up the mapping at: https://pptr.dev/supported-browsers
ARG CHROME_VERSION=155.0.8059.79

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates curl unzip \
    fonts-liberation \
    fonts-noto-color-emoji \
    fonts-noto-cjk \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libcairo2 libcups2 \
    libdbus-1-3 libdrm2 libexpat1 libfontconfig1 libgbm1 \
    libglib2.0-0 libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 \
    libpangocairo-1.0-0 libx11-6 libxcb1 libxcomposite1 \
    libxdamage1 libxext6 libxfixes3 libxkbcommon0 libxrandr2 \
    libxrender1 \
  && curl -fsSL \
    "https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chrome-linux64.zip" \
    -o /tmp/chrome.zip \
  && unzip -q /tmp/chrome.zip -d /opt \
  && rm /tmp/chrome.zip \
  && ln -s /opt/chrome-linux64/chrome /usr/bin/chromium \
  && apt-get purge -y curl unzip \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/*

# Point Puppeteer at the Chrome for Testing binary via symlink.
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

USER quarry
