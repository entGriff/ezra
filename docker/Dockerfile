FROM ubuntu:22.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
ARG EZRA_VERSION=latest
ARG REPO=entGriff/ezra

RUN case "$TARGETARCH" in \
      arm64) ARTIFACT=ezra-linux_arm64 ;; \
      *)     ARTIFACT=ezra-linux_x86_64 ;; \
    esac \
    && if [ "$EZRA_VERSION" = "latest" ]; then \
         curl -fLo /usr/local/bin/ezra \
           "https://github.com/${REPO}/releases/latest/download/${ARTIFACT}"; \
       else \
         curl -fLo /usr/local/bin/ezra \
           "https://github.com/${REPO}/releases/download/${EZRA_VERSION}/${ARTIFACT}"; \
       fi \
    && chmod +x /usr/local/bin/ezra

EXPOSE 42002

ENV EZRA_DATA_DIR=/data
ENV EZRA_PORT=42002
ENV LANG=C.UTF-8

ENTRYPOINT ["/usr/local/bin/ezra"]
